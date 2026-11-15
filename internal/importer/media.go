package importer

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
)

func (e *Engine) transfer(ctx context.Context, r *Record) error {
	m := &r.Media
	f := r.Message.File
	save := func() error { return e.Journal.SaveMedia(r.Message.ID, *m) }
	if f.Size < 1 {
		return &AttachmentError{errors.New("source attachment has no usable size")}
	}
	if m.MXC == "" {
		v, err := e.Matrix.Reserve(ctx)
		if err != nil {
			return err
		}
		m.MXC = v
		m.Name = fmt.Sprintf("import-%d.bin", r.Message.ID)
		m.Key, err = e.Matrix.RegistrationKey(m.MXC, m.Name)
		if err != nil {
			return err
		}
		m.AESKey = make([]byte, 32)
		m.IV = make([]byte, 16)
		if _, err = rand.Read(m.AESKey); err != nil {
			return err
		}
		if _, err = rand.Read(m.IV[:8]); err != nil {
			return err
		}
		if err = save(); err != nil {
			return err
		}
	}
	if !m.Registered {
		key, err := e.Matrix.Register(ctx, m.MXC, m.Name)
		if err != nil {
			return err
		}
		if key != m.Key {
			return errors.New("homeserver media key format differs from verified Tuwunel 2.0.0 format")
		}
		obj, err := e.Objects.Head(ctx, key)
		if err != nil {
			return err
		}
		if obj.Size != 1 || obj.Version == "" {
			return errors.New("registration object is not the expected versioned one-byte S3 object")
		}
		m.DummyVersion = obj.Version
		m.Registered = true
		if err = save(); err != nil {
			return err
		}
	}
	partSize := e.PartSize
	// S3 permits 10,000 parts. Align to Telegram's 1 MiB resume boundary.
	if need := (f.Size + 9999) / 10000; need > int64(partSize) {
		partSize = int((need + (1 << 20) - 1) / (1 << 20) * (1 << 20))
	}
	if partSize > 64<<20 {
		return errors.New("attachment exceeds supported multipart geometry")
	}
	if m.State == "completing" {
		obj, err := e.Objects.Head(ctx, m.Key)
		if err != nil {
			// A lost completion response is ambiguous. Do not start another
			// upload until HEAD establishes whether the first one succeeded.
			return err
		}
		if obj.ImportID == m.MXC && obj.Size == f.Size {
			m.FinalVersion = obj.Version
			return e.finishMedia(r)
		}
		if obj.Version != m.DummyVersion {
			return errors.New("media object changed unexpectedly while completing upload")
		}
	}
	if m.Upload == "" {
		uploads, err := e.Objects.Uploads(ctx, m.Key)
		if err != nil {
			return err
		}
		// No acknowledged upload ID means no acknowledged part can exist. Remove
		// any initiation whose response was lost, then start a single new upload.
		for _, u := range uploads {
			if err = e.Objects.Abort(ctx, m.Key, u); err != nil {
				return err
			}
		}
		u, err := e.Objects.Start(ctx, m.Key, m.MXC)
		if err != nil {
			return err
		}
		m.Upload = u
		if err = save(); err != nil {
			return err
		}
	}
	remote, err := e.Objects.Parts(ctx, m.Key, m.Upload)
	reset := errors.Is(err, ErrUploadGone)
	if err == nil {
		for i, p := range m.Parts {
			if i >= len(remote) || remote[i] != p {
				reset = true
				break
			}
		}
	}
	if reset {
		if err = e.Objects.Abort(ctx, m.Key, m.Upload); err != nil {
			return err
		}
		m.Upload = ""
		m.Offset = 0
		m.Parts = nil
		m.HashState = nil
		m.SHA256 = ""
		m.State = ""
		m.FinalVersion = ""
		if err = save(); err != nil {
			return err
		}
		return errors.New("remote multipart checkpoint expired; restarting this attachment")
	}
	if err != nil {
		return err
	}
	h := sha256.New()
	if len(m.HashState) > 0 {
		if err = h.(encoding.BinaryUnmarshaler).UnmarshalBinary(m.HashState); err != nil {
			return err
		}
	}
	if m.Offset < f.Size {
		block, err := aes.NewCipher(m.AESKey)
		if err != nil {
			return err
		}
		iv := append([]byte(nil), m.IV...)
		binary.BigEndian.PutUint64(iv[8:], uint64(m.Offset/16))
		writer := &partWriter{ctx: ctx, e: e, r: r, h: h, stream: cipher.NewCTR(block, iv), buffer: make([]byte, 0, partSize), partSize: partSize}
		if err = e.Source.Download(ctx, r.Message, m.Offset, writer); err != nil {
			if writer.err != nil {
				return writer.err
			}
			var unavailable *SourceUnavailable
			if errors.As(err, &unavailable) {
				return err
			}
			return &AttachmentError{err}
		}
		if m.Offset+int64(len(writer.buffer)) != f.Size {
			return &AttachmentError{errors.New("Telegram returned an incomplete attachment")}
		}
		if len(writer.buffer) > 0 {
			if err = writer.flush(); err != nil {
				return err
			}
		}
	}
	m.SHA256 = base64.RawStdEncoding.EncodeToString(h.Sum(nil))
	m.State = "completing"
	if err = save(); err != nil {
		return err
	}
	version, err := e.Objects.Complete(ctx, m.Key, m.Upload, m.Parts)
	if err != nil {
		return err
	}
	m.FinalVersion = version
	return e.finishMedia(r)
}

func (e *Engine) finishMedia(r *Record) error {
	if r.Media.FinalVersion == "" || r.Media.FinalVersion == r.Media.DummyVersion {
		return errors.New("S3 did not identify a distinct completed media version")
	}
	r.Media.State = "ready"
	r.Media.Error = ""
	// Queue before saving ready so a crash cannot forget the superseded tiny
	// version. Cleanup distinguishes this from failed-media cleanup.
	if err := e.Journal.QueueCleanup(r.Message.ID, r.Media); err != nil {
		return err
	}
	return e.Journal.SaveMedia(r.Message.ID, r.Media)
}

type partWriter struct {
	ctx      context.Context
	e        *Engine
	r        *Record
	h        hash.Hash
	stream   cipher.Stream
	buffer   []byte
	partSize int
	err      error
}

func (w *partWriter) Write(p []byte) (int, error) {
	n := len(p)
	if w.r.Media.Offset+int64(len(w.buffer))+int64(n) > w.r.Message.File.Size {
		return 0, errors.New("Telegram attachment exceeds its recorded size")
	}
	consumed := 0
	for len(p) > 0 {
		take := min(len(p), w.partSize-len(w.buffer))
		start := len(w.buffer)
		w.buffer = w.buffer[:start+take]
		w.stream.XORKeyStream(w.buffer[start:], p[:take])
		p = p[take:]
		consumed += take
		if len(w.buffer) == w.partSize {
			if err := w.flush(); err != nil {
				w.err = err
				return consumed, err
			}
		}
	}
	return n, nil
}
func (w *partWriter) flush() error {
	m := &w.r.Media
	etag, err := w.e.Objects.PutPart(w.ctx, m.Key, m.Upload, len(m.Parts)+1, w.buffer)
	if err != nil {
		return err
	}
	_, _ = w.h.Write(w.buffer)
	state, err := w.h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return err
	}
	// Do not advance the in-memory checkpoint until SQLite commits as well.
	next := *m
	next.Parts = append(append([]Part(nil), m.Parts...), Part{len(m.Parts) + 1, etag, int64(len(w.buffer))})
	next.Offset += int64(len(w.buffer))
	next.HashState = state
	if err = w.e.Journal.SaveMedia(w.r.Message.ID, next); err != nil {
		return err
	}
	*m = next
	w.buffer = w.buffer[:0]
	w.e.report()
	return nil
}
