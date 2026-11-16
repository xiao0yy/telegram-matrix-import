package importer

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/xiao0yy/telegram-matrix-import/internal/retry"
)

type source struct {
	messages []Message
	data     []byte
	offsets  []int64
	cancel   context.CancelFunc
	fail     bool
}

// Exercise the real durable engine in the automatic runner: nobody invokes
// Run a second time after the outage and the same journal remains locked.
func TestAutomaticResumeKeepsConfirmedPartsAndTimeline(t *testing.T) {
	e, path, s, m, o := fixture(t)
	defer e.Journal.Close()
	e.Source = &timedOutSource{source: s}
	waits := 0
	p := retry.Policy{Wait: func(context.Context, time.Duration) error {
		waits++
		r, err := e.Journal.Next()
		if err != nil || r.Media.Offset != 8<<20 || r.Media.State == "failed" || len(r.Payload) != 0 {
			t.Fatalf("checkpoint lost during wait: %v", err)
		}
		if other, err := Open(path); err == nil {
			other.Close()
			t.Fatal("retry released the journal lock")
		}
		e.Source = s
		return nil
	}}
	if err := p.Run(context.Background(), e.Run); err != nil {
		t.Fatal(err)
	}
	if waits != 1 || fmt.Sprint(m.order) != "[1 4 9]" || len(o.data["key"]) != len(s.data) {
		t.Fatalf("incorrect automatic resume: waits=%d order=%v", waits, m.order)
	}
}

func (s *source) Latest(context.Context) (int64, error) { return s.messages[len(s.messages)-1].ID, nil }
func (s *source) Page(_ context.Context, after, through int64, n int) ([]Message, error) {
	var out []Message
	for _, m := range s.messages {
		if m.ID > after && m.ID <= through {
			out = append(out, m)
			if len(out) == n {
				break
			}
		}
	}
	return out, nil
}
func (s *source) Download(ctx context.Context, m Message, off int64, w io.Writer) error {
	s.offsets = append(s.offsets, off)
	if s.fail {
		return errors.New("file unavailable")
	}
	for i := off; i < int64(len(s.data)); {
		end := min(i+1<<20, int64(len(s.data)))
		if _, e := w.Write(s.data[i:end]); e != nil {
			return e
		}
		i = end
		if s.cancel != nil && i >= 8<<20 {
			cancel := s.cancel
			s.cancel = nil
			cancel()
			return ctx.Err()
		}
	}
	return nil
}

type objects struct {
	obj              map[string]Object
	data             map[string][]byte
	parts            map[int][]byte
	lostComplete     bool
	failCompleteHead int
	completeCalls    int
	deleted          []string
	failDelete       bool
	failStorage      bool
	lostPart         bool
	uploads          []string
	aborted          int
}

func (o *objects) Head(_ context.Context, key string) (Object, error) {
	if o.completeCalls > 0 && o.failCompleteHead > 0 {
		o.failCompleteHead--
		return Object{}, errors.New("completion lookup unavailable")
	}
	v, ok := o.obj[key]
	if !ok {
		return v, errors.New("missing")
	}
	return v, nil
}
func (o *objects) Start(_ context.Context, key, id string) (string, error) {
	o.parts = map[int][]byte{}
	o.uploads = []string{"upload"}
	return "upload", nil
}
func (o *objects) Uploads(context.Context, string) ([]string, error) {
	return append([]string(nil), o.uploads...), nil
}
func (o *objects) Parts(context.Context, string, string) ([]Part, error) {
	var p []Part
	for n, b := range o.parts {
		p = append(p, Part{n, fmt.Sprint(n), int64(len(b))})
	}
	sort.Slice(p, func(i, j int) bool { return p[i].Number < p[j].Number })
	return p, nil
}
func (o *objects) PutPart(_ context.Context, key, u string, n int, b []byte) (string, error) {
	if o.failStorage {
		return "", errors.New("storage unavailable")
	}
	o.parts[n] = append([]byte(nil), b...)
	if o.lostPart {
		o.lostPart = false
		return "", errors.New("part response lost")
	}
	return fmt.Sprint(n), nil
}
func (o *objects) Complete(_ context.Context, key, u string, parts []Part) (string, error) {
	o.completeCalls++
	var b []byte
	for _, p := range parts {
		b = append(b, o.parts[p.Number]...)
	}
	o.data[key] = b
	o.obj[key] = Object{Size: int64(len(b)), Version: "final", ImportID: "mxc://test/1"}
	o.uploads = nil
	if o.lostComplete {
		o.lostComplete = false
		return "", errors.New("complete response lost")
	}
	return "final", nil
}
func (o *objects) Abort(context.Context, string, string) error {
	o.uploads = nil
	o.aborted++
	return nil
}
func (o *objects) DeleteVersion(_ context.Context, k, v string) error {
	if o.failDelete {
		return errors.New("delete unavailable")
	}
	o.deleted = append(o.deleted, v)
	return nil
}
func (o *objects) Versions(context.Context, string) ([]string, error) { return []string{"dummy"}, nil }

type matrix struct {
	objects  *objects
	sent     map[string]string
	payload  map[string][]byte
	order    []int64
	prepared int
	lostSend bool
	deleted  int
}

func (m *matrix) Reserve(context.Context) (string, error)        { return "mxc://test/1", nil }
func (m *matrix) RegistrationKey(string, string) (string, error) { return "key", nil }
func (m *matrix) Register(context.Context, string, string) (string, error) {
	m.objects.obj["key"] = Object{Size: 1, Version: "dummy"}
	return "key", nil
}
func (m *matrix) Prepare(_ context.Context, _ Message, c map[string]any) (json.RawMessage, error) {
	m.prepared++
	return json.Marshal(c)
}
func (m *matrix) Send(_ context.Context, msg Message, txn string, p json.RawMessage) (string, error) {
	if old, ok := m.sent[txn]; ok {
		if !bytes.Equal(m.payload[txn], p) {
			return "", errors.New("ciphertext changed on retry")
		}
		return old, nil
	}
	id := fmt.Sprintf("$%d", msg.ID)
	m.sent[txn] = id
	m.payload[txn] = append([]byte(nil), p...)
	m.order = append(m.order, msg.ID)
	if m.lostSend {
		m.lostSend = false
		return "", errors.New("send response lost")
	}
	return id, nil
}
func (m *matrix) Delete(context.Context, string) error { m.deleted++; return nil }
func fixture(t *testing.T) (*Engine, string, *source, *matrix, *objects) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	j, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	s := &source{messages: []Message{{ID: 1, Sender: "1", Text: "before"}, {ID: 4, Sender: "2", Text: "caption", File: &File{ID: "f", Name: "video.mp4", MIME: "video/mp4", Size: 12 << 20}}, {ID: 9, Sender: "1", Text: "after", ReplyTo: 4}}, data: bytes.Repeat([]byte{12, 7, 88, 0}, 3<<20)}
	o := &objects{obj: map[string]Object{}, data: map[string][]byte{}}
	m := &matrix{objects: o, sent: map[string]string{}, payload: map[string][]byte{}}
	engine := &Engine{Journal: j, Source: s, Matrix: m, Objects: o, Identity: "fixture", Attempts: 3, PartSize: 8 << 20}
	return engine, path, s, m, o
}
func reopen(t *testing.T, e *Engine, path string) {
	t.Helper()
	if err := e.Journal.Close(); err != nil {
		t.Fatal(err)
	}
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	e.Journal = j
}
func TestResumeAfterCancellationStreamsCiphertextAndPreservesOrder(t *testing.T) {
	e, path, s, m, o := fixture(t)
	defer func() { e.Journal.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	reopen(t, e, path)
	r, _ := e.Journal.Next()
	if r.Media.Offset != 8<<20 || r.Media.Attempts != 0 {
		t.Fatalf("wrong durable checkpoint: %+v", r.Media)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(s.offsets) != "[0 8388608]" {
		t.Fatal(s.offsets)
	}
	if fmt.Sprint(m.order) != "[1 4 9]" {
		t.Fatal(m.order)
	}
	var media Media
	e.Journal.Each(func(r Record) error {
		if r.Message.ID == 4 {
			media = r.Media
		}
		return nil
	})
	ciphertext := o.data["key"]
	h := sha256.Sum256(ciphertext)
	if media.SHA256 != base64.RawStdEncoding.EncodeToString(h[:]) {
		t.Fatal("wrong whole-file hash")
	}
	plain := make([]byte, len(ciphertext))
	block, _ := aes.NewCipher(media.AESKey)
	cipher.NewCTR(block, media.IV).XORKeyStream(plain, ciphertext)
	if !bytes.Equal(plain, s.data) {
		t.Fatal("resumed ciphertext cannot be decrypted")
	}
	if fmt.Sprint(o.deleted) != "[dummy]" {
		t.Fatal("tiny version was not reclaimed", o.deleted)
	}
}

type timedOutSource struct{ *source }

func (s *timedOutSource) Download(ctx context.Context, m Message, offset int64, w io.Writer) error {
	s.offsets = append(s.offsets, offset)
	if offset == 0 {
		if _, err := w.Write(s.data[:8<<20]); err != nil {
			return err
		}
	}
	return &SourceUnavailable{Err: context.DeadlineExceeded}
}

func TestDownloadTimeoutPreservesCheckpointAndPausesWithoutPlaceholder(t *testing.T) {
	e, path, s, m, o := fixture(t)
	defer func() { e.Journal.Close() }()
	e.Source = &timedOutSource{s}
	if err := e.Run(context.Background()); err == nil {
		t.Fatal("repeated network timeout must pause the import")
	}
	r, err := e.Journal.Next()
	if err != nil || r == nil {
		t.Fatal("pending message lost", err)
	}
	if r.Message.ID != 4 || r.Media.Offset != 8<<20 || r.Media.State == "failed" || len(r.Payload) != 0 || fmt.Sprint(m.order) != "[1]" {
		t.Fatalf("network timeout skipped or discarded the attachment: offset=%d state=%s order=%v", r.Media.Offset, r.Media.State, m.order)
	}
	reopen(t, e, path)
	e.Source = s
	if err = e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(s.offsets) != "[0 8388608 8388608 8388608]" || fmt.Sprint(m.order) != "[1 4 9]" || len(o.data["key"]) != len(s.data) {
		t.Fatalf("resume did not reuse checkpoint in order: offsets=%v order=%v", s.offsets, m.order)
	}
}
func TestLostResponsesDoNotDuplicateObjectsOrMessages(t *testing.T) {
	e, path, _, m, o := fixture(t)
	defer func() { e.Journal.Close() }()
	m.lostSend = true
	o.lostPart = true
	o.lostComplete = true
	if err := e.Run(context.Background()); err == nil {
		t.Fatal("expected first send error")
	}
	reopen(t, e, path)
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.prepared != 3 || len(m.sent) != 3 || o.completeCalls != 1 {
		t.Fatalf("not idempotent: prepares %d events %d completes %d", m.prepared, len(m.sent), o.completeCalls)
	}
}
func TestPermanentFailureKeepsCaptionPositionAndManagedCleanup(t *testing.T) {
	e, path, s, m, o := fixture(t)
	defer func() { e.Journal.Close() }()
	s.fail = true
	o.failDelete = true
	if err := e.Run(context.Background()); err == nil {
		t.Fatal("cleanup failure must be visible")
	}
	if fmt.Sprint(m.order) != "[1 4 9]" {
		t.Fatal(m.order)
	}
	var found bool
	for _, raw := range m.payload {
		var c map[string]any
		json.Unmarshal(raw, &c)
		if c["body"] == "caption\n[附件导入失败] video.mp4" {
			found = true
		}
	}
	if !found {
		t.Fatal("failure marker lost caption or filename")
	}
	stats, _ := e.Journal.Status()
	if stats["cleanup_pending"] != int64(1) || stats["attachments_failed"] != int64(1) {
		t.Fatal(stats)
	}
	reopen(t, e, path)
	o.failDelete = false
	if err := e.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, _ = e.Journal.Status()
	if stats["cleanup_pending"] != int64(0) {
		t.Fatal(stats)
	}
}
func TestJournalRejectsConcurrentRunnerAndDifferentIdentity(t *testing.T) {
	e, path, _, _, _ := fixture(t)
	defer e.Journal.Close()
	if j, err := Open(path); err == nil {
		j.Close()
		t.Fatal("parallel writer accepted")
	}
	if err := e.Journal.Bind("a"); err != nil {
		t.Fatal(err)
	}
	if err := e.Journal.Bind("b"); err == nil {
		t.Fatal("identity mismatch accepted")
	}
}

func TestStorageOutagePausesTimelineAndResumesWithoutFailureMarker(t *testing.T) {
	e, path, _, m, o := fixture(t)
	defer func() { e.Journal.Close() }()
	o.failStorage = true
	if err := e.Run(context.Background()); err == nil {
		t.Fatal("storage outage was ignored")
	}
	if fmt.Sprint(m.order) != "[1]" {
		t.Fatal("timeline passed unavailable storage", m.order)
	}
	r, err := e.Journal.Next()
	if err != nil || r.Media.State == "failed" {
		t.Fatal(r, err)
	}
	reopen(t, e, path)
	o.failStorage = false
	if err = e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, err := e.Journal.Status()
	if err != nil || stats["attachments_failed"] != int64(0) || len(m.sent) != 3 {
		t.Fatal(stats, err)
	}
}

func TestExpiredMultipartRestartsOnlyCurrentAttachment(t *testing.T) {
	e, path, s, m, o := fixture(t)
	defer func() { e.Journal.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	reopen(t, e, path)
	// Simulate an object store lifecycle rule removing the old multipart parts.
	o.parts = map[int][]byte{}
	o.uploads = nil
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o.aborted != 1 || len(s.offsets) != 2 || s.offsets[1] != 0 || len(m.sent) != 3 {
		t.Fatal("bad expired-upload recovery", o.aborted, s.offsets, len(m.sent))
	}
}

func TestLostCompletionAndHeadOutageDoNotRewriteUploadedMedia(t *testing.T) {
	e, path, s, m, o := fixture(t)
	defer func() { e.Journal.Close() }()
	o.lostComplete = true
	o.failCompleteHead = 2
	if err := e.Run(context.Background()); err == nil {
		t.Fatal("uncertain upload completion should pause the timeline")
	}
	if o.completeCalls != 1 || len(s.offsets) != 1 || len(m.sent) != 1 {
		t.Fatal("rewrote media or advanced past uncertain completion", o.completeCalls, s.offsets, m.order)
	}
	reopen(t, e, path)
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o.completeCalls != 1 || len(s.offsets) != 1 || len(m.sent) != 3 {
		t.Fatal("resume should reuse the completed object", o.completeCalls, s.offsets, m.order)
	}
}
