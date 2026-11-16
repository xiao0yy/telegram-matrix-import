package importer

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Engine struct {
	Journal       *Journal
	Source        Source
	Matrix        Matrix
	Objects       Objects
	Identity      string
	Attempts      int
	PartSize      int
	RetryDelay    time.Duration
	Progress      func(map[string]any)
	BeforeMessage func(context.Context) error
	current       *Record
	through       int64
}

func (e *Engine) Run(ctx context.Context) error {
	if e.Attempts < 1 {
		e.Attempts = 5
	}
	if e.PartSize == 0 {
		e.PartSize = 8 << 20
	}
	if e.PartSize < 5<<20 || e.PartSize%(1<<20) != 0 {
		return errors.New("part size must be a multiple of 1 MiB and at least 5 MiB")
	}
	if err := e.Journal.Bind(e.Identity); err != nil {
		return err
	}
	boundary, err := e.Journal.Get("boundary")
	if err != nil {
		return err
	}
	if boundary == "" {
		n, err := e.Source.Latest(ctx)
		if err != nil {
			return err
		}
		boundary = strconv.FormatInt(n, 10)
		if err = e.Journal.Set("boundary", boundary); err != nil {
			return err
		}
	}
	through, err := strconv.ParseInt(boundary, 10, 64)
	if err != nil {
		return err
	}
	e.through = through
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		record, err := e.Journal.Next()
		if err != nil {
			return err
		}
		if record != nil {
			e.current = record
			if e.BeforeMessage != nil {
				if err = e.BeforeMessage(ctx); err != nil {
					return err
				}
			}
			e.report()
			if err = e.deliver(ctx, record); err != nil {
				return fmt.Errorf("message %d: %w", record.Message.ID, err)
			}
			e.report()
			continue
		}
		s, err := e.Journal.Get("cursor")
		if err != nil {
			return err
		}
		var after int64
		if s != "" {
			after, err = strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
		}
		if after >= through {
			break
		}
		page, err := e.Source.Page(ctx, after, through, 50)
		if err != nil {
			return err
		}
		cursor := after
		for _, m := range page {
			if m.ID <= cursor || m.ID > through {
				return errors.New("source returned unordered or out-of-range messages")
			}
			cursor = m.ID
		}
		if len(page) == 0 {
			cursor = through
		}
		if err = e.Journal.AddPage(page, cursor); err != nil {
			return err
		}
	}
	if err = e.Journal.Set("phase", "imported"); err != nil {
		return err
	}
	// Cleanup failures stay in the journal and are visible in status; they do not
	// retroactively invalidate an already published timeline.
	if err = e.Cleanup(ctx); err != nil {
		return fmt.Errorf("timeline imported; cleanup remains pending: %w", err)
	}
	e.report()
	return nil
}

func (e *Engine) report() {
	if e.Progress != nil && e.current != nil {
		e.Progress(map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano), "message_id": e.current.Message.ID, "through_telegram_id": e.through, "attachment_bytes_uploaded": e.current.Media.Offset, "attachment_state": e.current.Media.State, "attachment_attempts": e.current.Media.Attempts, "attachment_error_kind": e.current.Media.ErrorKind})
	}
}
func (e *Engine) transaction(id int64) string {
	h := sha256.Sum256([]byte(e.Identity + ":" + strconv.FormatInt(id, 10)))
	return "tg-import-" + hex.EncodeToString(h[:])
}
func (e *Engine) deliver(ctx context.Context, r *Record) error {
	if len(r.Payload) == 0 {
		if r.Message.File != nil && r.Media.State != "ready" && r.Media.State != "failed" {
			var lastError error
			if r.Media.ErrorKind == "storage" && r.Media.Attempts >= e.Attempts {
				r.Media.Attempts = 0
			}
			for r.Media.Attempts < e.Attempts {
				err := e.transfer(ctx, r)
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.Media.Attempts++
				lastError = err
				r.Media.Error = err.Error()
				var attachmentErr *AttachmentError
				r.Media.ErrorKind = "storage"
				if errors.As(err, &attachmentErr) {
					r.Media.ErrorKind = "attachment"
				}
				if saveErr := e.Journal.SaveMedia(r.Message.ID, r.Media); saveErr != nil {
					return saveErr
				}
				e.report()
				if r.Media.Attempts < e.Attempts {
					timer := time.NewTimer(e.RetryDelay)
					select {
					case <-ctx.Done():
						timer.Stop()
						return ctx.Err()
					case <-timer.C:
					}
				}
			}
			if r.Media.State != "ready" {
				if r.Media.ErrorKind != "attachment" {
					return fmt.Errorf("media infrastructure failed; checkpoint retained: %w", lastError)
				}
				// Register cleanup before committing the failed timeline entry. A crash
				// between these writes can only leave a managed cleanup job.
				if err := e.Journal.QueueCleanup(r.Message.ID, r.Media); err != nil {
					return err
				}
				r.Media.State = "failed"
				if err := e.Journal.SaveMedia(r.Message.ID, r.Media); err != nil {
					return err
				}
			}
		}
		content := map[string]any{"msgtype": "m.text", "body": r.Message.Text}
		if r.Message.File != nil {
			f := r.Message.File
			if r.Media.State == "failed" {
				body := "[附件导入失败] " + f.Name
				if r.Message.Text != "" {
					body = r.Message.Text + "\n" + body
				}
				content["body"] = body
				content["com.telegram.import.failure"] = map[string]any{"message_id": r.Message.ID, "file_name": f.Name, "attempts": r.Media.Attempts}
			} else {
				kind := "m.file"
				switch {
				case strings.HasPrefix(f.MIME, "image/"):
					kind = "m.image"
				case strings.HasPrefix(f.MIME, "video/"):
					kind = "m.video"
				case strings.HasPrefix(f.MIME, "audio/"):
					kind = "m.audio"
				}
				content["msgtype"] = kind
				content["body"] = f.Name
				content["filename"] = f.Name
				if r.Message.Text != "" {
					content["body"] = r.Message.Text
				}
				content["info"] = map[string]any{"size": f.Size, "mimetype": f.MIME}
				content["file"] = map[string]any{"url": r.Media.MXC, "v": "v2", "key": map[string]any{"kty": "oct", "key_ops": []string{"encrypt", "decrypt"}, "alg": "A256CTR", "k": base64.RawURLEncoding.EncodeToString(r.Media.AESKey), "ext": true}, "iv": base64.RawStdEncoding.EncodeToString(r.Media.IV), "hashes": map[string]string{"sha256": r.Media.SHA256}}
			}
		}
		if r.Message.ReplyTo > 0 {
			event, err := e.Journal.Event(r.Message.ReplyTo)
			if err != nil {
				return err
			}
			if event != "" {
				content["m.relates_to"] = map[string]any{"m.in_reply_to": map[string]string{"event_id": event}}
			}
		}
		content["com.telegram.import.source"] = map[string]any{"id": r.Message.ID, "sender": r.Message.Sender}
		payload, err := e.Matrix.Prepare(ctx, r.Message, content)
		if err != nil {
			return err
		}
		if err = e.Journal.SavePayload(r.Message.ID, payload); err != nil {
			return err
		}
		r.Payload = payload
	}
	event, err := e.Matrix.Send(ctx, r.Message, e.transaction(r.Message.ID), json.RawMessage(r.Payload))
	if err != nil {
		return err
	}
	return e.Journal.Sent(r.Message.ID, event)
}

func (e *Engine) Cleanup(ctx context.Context) error {
	var failures []error
	var cursor int64
	for {
		entries, err := e.Journal.CleanupRecords(cursor)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		for id, m := range entries {
			if id > cursor {
				cursor = id
			}
			var cleanupErr error
			if m.Key != "" {
				uploads, err := e.Objects.Uploads(ctx, m.Key)
				cleanupErr = errors.Join(cleanupErr, err)
				for _, u := range uploads {
					cleanupErr = errors.Join(cleanupErr, e.Objects.Abort(ctx, m.Key, u))
				}
			}
			// A ready object remains available. Only its original registration version
			// is disposable. A failed object has never been referenced by an event.
			if m.State != "ready" && m.MXC != "" {
				cleanupErr = errors.Join(cleanupErr, e.Matrix.Delete(ctx, m.MXC))
			}
			if m.State != "ready" && m.Key != "" {
				versions, err := e.Objects.Versions(ctx, m.Key)
				cleanupErr = errors.Join(cleanupErr, err)
				for _, version := range versions {
					cleanupErr = errors.Join(cleanupErr, e.Objects.DeleteVersion(ctx, m.Key, version))
				}
			} else if m.Key != "" && m.DummyVersion != "" {
				cleanupErr = errors.Join(cleanupErr, e.Objects.DeleteVersion(ctx, m.Key, m.DummyVersion))
			}
			if err = e.Journal.CleanupResult(id, cleanupErr); err != nil {
				return err
			}
			if cleanupErr != nil {
				failures = append(failures, fmt.Errorf("message %d: %w", id, cleanupErr))
			}
		}
	}
	return errors.Join(failures...)
}
