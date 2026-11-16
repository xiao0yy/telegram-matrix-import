// Package bridge writes only the portal and imported message mappings expected
// by mautrix-telegram v0.2610.0. It leaves login and device identity unchanged.
package bridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
)

type Config struct {
	Database        string `json:"database"`
	Container       string `json:"container"`
	Image           string `json:"image"`
	BridgeID        string `json:"bridge_id"`
	LoginID         string `json:"login_id"`
	Owner           string `json:"owner"`
	CryptoAccount   string `json:"crypto_account"`
	CryptoPickleKey string `json:"crypto_pickle_key"`
}

func (c Config) Stopped(ctx context.Context) error {
	if c.Container == "" || c.Image == "" {
		return errors.New("bridge container and exact image ID are required")
	}
	out, e := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{json .State.Running}} {{json .State.Restarting}} {{.Image}} {{.HostConfig.RestartPolicy.Name}}", c.Container).Output()
	if e != nil {
		return errors.New("cannot inspect bridge container")
	}
	parts := strings.Fields(string(out))
	if len(parts) != 4 || parts[0] != "false" || parts[1] != "false" {
		return errors.New("bridge must remain stopped during import and handoff")
	}
	if parts[2] != c.Image {
		return errors.New("bridge image changed; compatibility must be checked again")
	}
	if parts[3] != "no" {
		return errors.New("bridge restart policy must be no during import")
	}
	return nil
}
func (c Config) Apply(ctx context.Context, j *importer.Journal, kind string, peerID int64, title, room, server, ghostPrefix, backupDir string, copyKeys ...func(context.Context) error) error {
	if err := c.Stopped(ctx); err != nil {
		return err
	}
	phase, e := j.Get("phase")
	if e != nil {
		return e
	}
	if phase != "imported" {
		return errors.New("import must finish before handoff")
	}
	stats, e := j.Status()
	if e != nil {
		return e
	}
	if stats["messages_sent"] != stats["messages_discovered"] || stats["cleanup_pending"] != int64(0) {
		return errors.New("unfinished messages or media cleanup prevent handoff")
	}
	if kind != "chat" && kind != "channel" && kind != "user" {
		return errors.New("unsupported portal type")
	}
	if _, e = os.Stat(c.Database); e != nil {
		return e
	}
	u := &url.URL{Scheme: "file", Path: c.Database}
	db, e := sql.Open("sqlite3", u.String()+"?mode=rw&_foreign_keys=on&_busy_timeout=5000")
	if e != nil {
		return e
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if e = os.MkdirAll(backupDir, 0700); e != nil {
		return e
	}
	// SQLite's snapshot includes a possible WAL. A raw file copy would not.
	backup := filepath.Join(backupDir, "bridge-before-handoff-"+time.Now().UTC().Format("20060102T150405.000000000")+".db")
	if _, e = db.ExecContext(ctx, "VACUUM INTO ?", backup); e != nil {
		return e
	}
	if e = os.Chmod(backup, 0600); e != nil {
		return e
	}
	// Only inbound room keys are copied. The bridge's device, account, outbound
	// sessions and Telegram update cursors remain its own.
	for _, copy := range copyKeys {
		if e = copy(ctx); e != nil {
			return e
		}
	}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var owner string
	if e = tx.QueryRowContext(ctx, "SELECT user_mxid FROM user_login WHERE bridge_id=? AND id=?", c.BridgeID, c.LoginID).Scan(&owner); e != nil {
		return e
	}
	if owner != c.Owner {
		return errors.New("bridge login owner mismatch")
	}
	portal := kind + ":" + strconv.FormatInt(peerID, 10)
	receiver := ""
	if kind == "chat" || kind == "user" {
		receiver = c.LoginID
	}
	var existing sql.NullString
	e = tx.QueryRowContext(ctx, "SELECT mxid FROM portal WHERE bridge_id=? AND id=? AND receiver=?", c.BridgeID, portal, receiver).Scan(&existing)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if existing.Valid && existing.String != "" && existing.String != room {
		return errors.New("portal already belongs to another Matrix room")
	}
	if e == nil && (!existing.Valid || existing.String == "") {
		// The bridge caches Telegram dialogs before creating Matrix rooms.
		// Reuse an empty cache entry, but never adopt an orphaned history.
		var count int
		if e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM message WHERE bridge_id=? AND room_id=? AND room_receiver=?", c.BridgeID, portal, receiver).Scan(&count); e != nil {
			return e
		}
		if count != 0 {
			return errors.New("unmapped portal still has message history; review it before handoff")
		}
	}
	roomType, otherUser := "", ""
	if kind == "user" {
		roomType, otherUser = "dm", strconv.FormatInt(peerID, 10)
	}
	metadata, _ := json.Marshal(map[string]any{"is_supergroup": kind == "channel", "allowed_reactions": []string{}})
	if _, e = tx.ExecContext(ctx, `INSERT INTO portal(bridge_id,id,receiver,mxid,name,topic,avatar_id,avatar_hash,avatar_mxc,name_set,avatar_set,topic_set,in_space,room_type,other_user_id,metadata)
 VALUES(?,?,?,?,?,'','','','',true,false,false,false,?,?,?) ON CONFLICT(bridge_id,id,receiver) DO NOTHING`, c.BridgeID, portal, receiver, room, title, roomType, otherUser, string(metadata)); e != nil {
		return e
	}
	// Keep cached Telegram metadata and identifiers. Only bind the previously
	// unmapped dialog to the imported room; the bridge will refresh its profile.
	if _, e = tx.ExecContext(ctx, `UPDATE portal SET mxid=?, name=?, name_set=true, avatar_set=false, topic_set=false, in_space=false, room_type=?, other_user_id=? WHERE bridge_id=? AND id=? AND receiver=? AND (mxid IS NULL OR mxid='')`, room, title, roomType, otherUser, c.BridgeID, portal, receiver); e != nil {
		return e
	}
	e = j.Each(func(r importer.Record) error {
		if r.EventID == "" {
			return errors.New("unsent message in journal")
		}
		m := r.Message
		ghost := "@" + ghostPrefix + m.Sender + ":" + server
		if _, err := tx.ExecContext(ctx, `INSERT INTO ghost(bridge_id,id,name,avatar_id,avatar_hash,avatar_mxc,name_set,avatar_set,contact_info_set,is_bot,identifiers,metadata) VALUES(?,?,?,'','','',true,false,false,false,'[]','{}') ON CONFLICT(bridge_id,id) DO NOTHING`, c.BridgeID, m.Sender, m.SenderName); err != nil {
			return err
		}
		messageID := func(n int64) string {
			if kind == "channel" {
				return fmt.Sprintf("%d.%d", peerID, n)
			}
			return fmt.Sprint(n)
		}
		var reply any
		if m.ReplyTo > 0 {
			reply = messageID(m.ReplyTo)
		}
		meta := map[string]any{}
		if r.Media.State == "ready" {
			meta["content_uri"] = r.Media.MXC
		}
		raw, _ := json.Marshal(meta)
		var old string
		err := tx.QueryRowContext(ctx, "SELECT mxid FROM message WHERE bridge_id=? AND room_receiver=? AND id=? AND part_id=''", c.BridgeID, receiver, messageID(m.ID)).Scan(&old)
		if err == nil {
			if old != r.EventID {
				return errors.New("bridge already maps this Telegram message to a different event")
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO message(bridge_id,id,part_id,mxid,room_id,room_receiver,sender_id,sender_mxid,timestamp,edit_count,double_puppeted,reply_to_id,metadata) VALUES(?,?,'',?,?,?,?,?,?,0,false,?,?)`, c.BridgeID, messageID(m.ID), r.EventID, portal, receiver, m.Sender, ghost, m.Time*1000000, reply, string(raw))
		return err
	})
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO user_portal(bridge_id,user_mxid,login_id,portal_id,portal_receiver,in_space,preferred) VALUES(?,?,?,?,?,false,true) ON CONFLICT DO NOTHING`, c.BridgeID, c.Owner, c.LoginID, portal, receiver); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO backfill_task(bridge_id,portal_id,portal_receiver,user_login_id,batch_count,is_done,queue_done,next_dispatch_min_ts) VALUES(?,?,?,?,0,true,true,0) ON CONFLICT(bridge_id,portal_id,portal_receiver) DO UPDATE SET is_done=true,queue_done=true`, c.BridgeID, portal, receiver, c.LoginID); e != nil {
		return e
	}
	if e = c.Stopped(ctx); e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	return j.Set("phase", "handed-off")
}
