package importer

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type AttachmentPolicy struct {
	MaxAttempts            int `json:"max_attempts"`
	MaxConsecutiveFailures int `json:"max_consecutive_failures"`
}

func (p AttachmentPolicy) Defaults() (AttachmentPolicy, error) {
	if p.MaxAttempts < 0 || p.MaxConsecutiveFailures < 0 {
		return p, errors.New("attachment retry limits must be positive (or zero for defaults)")
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 5
	}
	if p.MaxConsecutiveFailures == 0 {
		p.MaxConsecutiveFailures = 3
	}
	return p, nil
}

type AttachmentFailures struct {
	Consecutive   int    `json:"consecutive"`
	Blocked       bool   `json:"blocked"`
	Limit         int    `json:"limit,omitempty"`
	LastMessageID int64  `json:"last_message_id,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	ResetAt       string `json:"reset_at,omitempty"`
}

// A latched stop is deliberately not transient. Reconnecting or restarting the
// process must not turn a source outage into an unlimited run of placeholders.
type AttachmentLimitError struct{ Failures AttachmentFailures }

func (e *AttachmentLimitError) Error() string {
	return fmt.Sprintf("stopped after %d consecutive failed attachments (limit %d, last message %d); inspect the failure and run reset-attachment-failures before resuming", e.Failures.Consecutive, e.Failures.Limit, e.Failures.LastMessageID)
}
func (e *AttachmentLimitError) Retryable() bool { return false }

type rowQueryer interface {
	QueryRow(string, ...any) *sql.Row
}

func attachmentFailures(q rowQueryer) (AttachmentFailures, error) {
	var state AttachmentFailures
	var raw string
	err := q.QueryRow("SELECT value FROM settings WHERE key='attachment_failures'").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		// Upgrade an existing journal once, preserving any trailing failures.
		err = q.QueryRow(`SELECT COUNT(*),COALESCE(MAX(id),0) FROM messages
WHERE event_id!='' AND json_extract(media,'$.state')='failed'
AND id>COALESCE((SELECT MAX(id) FROM messages WHERE event_id!='' AND json_extract(media,'$.state')='ready'),0)`).Scan(&state.Consecutive, &state.LastMessageID)
		return state, err
	}
	if err != nil {
		return state, err
	}
	err = json.Unmarshal([]byte(raw), &state)
	return state, err
}

func (j *Journal) AttachmentFailures() (AttachmentFailures, error) {
	return attachmentFailures(j.db)
}

func (j *Journal) CheckAttachmentFailures(limit int) (AttachmentFailures, error) {
	s, err := j.AttachmentFailures()
	if err != nil {
		return s, err
	}
	if !s.Blocked && s.Consecutive >= limit {
		s.Blocked, s.Limit = true, limit
		raw, err := json.Marshal(s)
		if err != nil {
			return s, err
		}
		if err = j.Set("attachment_failures", string(raw)); err != nil {
			return s, err
		}
	}
	if s.Blocked {
		return s, &AttachmentLimitError{Failures: s}
	}
	return s, nil
}

// Explicit operator acknowledgement, under the journal's exclusive process
// lock. It does not modify sent messages, media, or upload checkpoints.
func (j *Journal) ResetAttachmentFailures() error {
	s, err := j.AttachmentFailures()
	if err != nil {
		return err
	}
	previous, err := json.Marshal(s)
	if err != nil {
		return err
	}
	s.Consecutive, s.Blocked = 0, false
	s.ResetAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tx, err := j.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for key, value := range map[string]string{"attachment_failures": string(raw), "attachment_failures_before_reset": string(previous)} {
		if _, err = tx.Exec("INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (j *Journal) SentWithAttachmentLimit(id int64, event string, limit int) error {
	if event == "" {
		return errors.New("empty Matrix event ID")
	}
	tx, err := j.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var previous string
	var raw []byte
	if err = tx.QueryRow("SELECT event_id,media FROM messages WHERE id=?", id).Scan(&previous, &raw); err != nil {
		return err
	}
	if previous != "" {
		if previous != event {
			return errors.New("message already has a different Matrix event")
		}
		return nil
	}
	var media Media
	if err = json.Unmarshal(raw, &media); err != nil {
		return err
	}
	if media.State == "ready" || media.State == "failed" {
		s, err := attachmentFailures(tx)
		if err != nil {
			return err
		}
		s.LastMessageID, s.Limit = id, limit
		if media.State == "ready" {
			s.Consecutive, s.LastError = 0, ""
		} else {
			s.Consecutive++
			s.LastError = media.Error
			if s.Consecutive >= limit {
				s.Blocked = true
			}
		}
		raw, err = json.Marshal(s)
		if err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT INTO settings VALUES('attachment_failures',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", string(raw)); err != nil {
			return err
		}
	}
	if _, err = tx.Exec("UPDATE messages SET event_id=? WHERE id=?", event, id); err != nil {
		return err
	}
	return tx.Commit()
}
