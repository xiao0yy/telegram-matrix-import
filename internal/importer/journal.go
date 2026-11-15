package importer

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	_ "github.com/mattn/go-sqlite3"
)

type Journal struct {
	db   *sql.DB
	lock *os.File
}

func Open(path string) (*Journal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("another importer owns this state")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		lock.Close()
		return nil, err
	}
	f.Close()
	if err = os.Chmod(path, 0600); err != nil {
		lock.Close()
		return nil, err
	}
	u := &url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite3", u.String()+"?_journal_mode=WAL&_synchronous=FULL&_foreign_keys=on&_busy_timeout=5000")
	if err != nil {
		lock.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	j := &Journal{db: db, lock: lock}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, source BLOB NOT NULL, media BLOB NOT NULL DEFAULT '{}', payload BLOB, event_id TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS cleanup(id INTEGER PRIMARY KEY, media BLOB NOT NULL, error TEXT NOT NULL DEFAULT '');`)
	if err != nil {
		j.Close()
		return nil, err
	}
	return j, nil
}
func OpenReadOnly(path string) (*Journal, error) {
	u := &url.URL{Scheme: "file", Path: path}
	db, e := sql.Open("sqlite3", u.String()+"?mode=ro&_busy_timeout=5000")
	if e != nil {
		return nil, e
	}
	return &Journal{db: db}, nil
}
func (j *Journal) Close() error {
	e := j.db.Close()
	if j.lock != nil {
		j.lock.Close()
	}
	return e
}
func (j *Journal) Bind(identity string) error {
	var old string
	err := j.db.QueryRow("SELECT value FROM settings WHERE key='identity'").Scan(&old)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = j.db.Exec("INSERT INTO settings VALUES('identity',?)", identity)
		return err
	}
	if err != nil {
		return err
	}
	if old != identity {
		return errors.New("state belongs to another source, room or storage configuration")
	}
	return nil
}
func (j *Journal) Get(key string) (string, error) {
	var s string
	e := j.db.QueryRow("SELECT value FROM settings WHERE key=?", key).Scan(&s)
	if errors.Is(e, sql.ErrNoRows) {
		return "", nil
	}
	return s, e
}
func (j *Journal) Set(key, value string) error {
	_, e := j.db.Exec("INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value)
	return e
}
func (j *Journal) AddPage(messages []Message, after int64) error {
	tx, e := j.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, m := range messages {
		b, e := json.Marshal(m)
		if e != nil {
			return e
		}
		if _, e = tx.Exec("INSERT INTO messages(id,source) VALUES(?,?) ON CONFLICT(id) DO NOTHING", m.ID, b); e != nil {
			return e
		}
	}
	if _, e = tx.Exec("INSERT INTO settings VALUES('cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", fmt.Sprint(after)); e != nil {
		return e
	}
	return tx.Commit()
}
func (j *Journal) Next() (*Record, error) {
	r := &Record{}
	var source, media, payload []byte
	e := j.db.QueryRow("SELECT source,media,payload,event_id FROM messages WHERE event_id='' ORDER BY id LIMIT 1").Scan(&source, &media, &payload, &r.EventID)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal(source, &r.Message); e != nil {
		return nil, e
	}
	r.Payload = payload
	e = json.Unmarshal(media, &r.Media)
	return r, e
}
func (j *Journal) SaveMedia(id int64, m Media) error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	_, e = j.db.Exec("UPDATE messages SET media=? WHERE id=?", b, id)
	return e
}
func (j *Journal) SavePayload(id int64, p json.RawMessage) error {
	if !json.Valid(p) {
		return errors.New("invalid event payload")
	}
	_, e := j.db.Exec("UPDATE messages SET payload=? WHERE id=? AND payload IS NULL", []byte(p), id)
	return e
}
func (j *Journal) Sent(id int64, event string) error {
	if event == "" {
		return errors.New("empty Matrix event ID")
	}
	_, e := j.db.Exec("UPDATE messages SET event_id=? WHERE id=?", event, id)
	return e
}
func (j *Journal) Event(id int64) (string, error) {
	var s string
	e := j.db.QueryRow("SELECT event_id FROM messages WHERE id=?", id).Scan(&s)
	if errors.Is(e, sql.ErrNoRows) {
		return "", nil
	}
	return s, e
}
func (j *Journal) QueueCleanup(id int64, m Media) error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	_, e = j.db.Exec("INSERT INTO cleanup(id,media) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET media=excluded.media", id, b)
	return e
}
func (j *Journal) CleanupRecords(after int64) (map[int64]Media, error) {
	rows, e := j.db.Query("SELECT id,media FROM cleanup WHERE id>? ORDER BY id LIMIT 32", after)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[int64]Media{}
	for rows.Next() {
		var id int64
		var b []byte
		if e = rows.Scan(&id, &b); e != nil {
			return nil, e
		}
		var m Media
		if e = json.Unmarshal(b, &m); e != nil {
			return nil, e
		}
		out[id] = m
	}
	return out, rows.Err()
}
func (j *Journal) CleanupResult(id int64, err error) error {
	if err == nil {
		_, e := j.db.Exec("DELETE FROM cleanup WHERE id=?", id)
		return e
	}
	_, e := j.db.Exec("UPDATE cleanup SET error=? WHERE id=?", err.Error(), id)
	return e
}
func (j *Journal) Status() (map[string]any, error) {
	var total, sent, failed, bytes, cleanup int64
	e := j.db.QueryRow(`SELECT COUNT(*),COALESCE(SUM(event_id!=''),0),COALESCE(SUM(json_extract(media,'$.state')='failed'),0),COALESCE(SUM(json_extract(media,'$.offset')),0) FROM messages`).Scan(&total, &sent, &failed, &bytes)
	if e != nil {
		return nil, e
	}
	e = j.db.QueryRow("SELECT COUNT(*) FROM cleanup").Scan(&cleanup)
	return map[string]any{"messages_discovered": total, "messages_sent": sent, "attachments_failed": failed, "uploaded_bytes": bytes, "cleanup_pending": cleanup}, e
}
func (j *Journal) Each(fn func(Record) error) error {
	rows, e := j.db.Query("SELECT source,media,payload,event_id FROM messages ORDER BY id")
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var r Record
		var a, b, payload []byte
		if e = rows.Scan(&a, &b, &payload, &r.EventID); e != nil {
			return e
		}
		r.Payload = payload
		if e = json.Unmarshal(a, &r.Message); e != nil {
			return e
		}
		if e = json.Unmarshal(b, &r.Media); e != nil {
			return e
		}
		if e = fn(r); e != nil {
			return e
		}
	}
	return rows.Err()
}
