package importer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/xiao0yy/telegram-matrix-import/internal/retry"
)

type failingFiles struct {
	*source
	calls map[int64]int
	err   error
}

func (s *failingFiles) Download(_ context.Context, m Message, _ int64, _ io.Writer) error {
	s.calls[m.ID]++
	return &AttachmentError{Err: s.err}
}

func TestConsecutiveAttachmentFailuresStopAndStayStoppedAcrossRestart(t *testing.T) {
	e, path, s, m, _ := fixture(t)
	defer func() { e.Journal.Close() }()
	e.Attempts, e.ConsecutiveFailureLimit = 2, 3
	file := s.messages[1].File
	s.messages = []Message{{ID: 1, File: file}, {ID: 2, Text: "between"}, {ID: 3, File: file}, {ID: 4, File: file}, {ID: 5, File: file}}
	failing := &failingFiles{source: s, calls: map[int64]int{}, err: context.DeadlineExceeded}
	e.Source = failing
	err := (retry.Policy{Wait: func(context.Context, time.Duration) error {
		t.Fatal("attachment stop was automatically retried")
		return nil
	}}).Run(context.Background(), e.Run)
	var limit *AttachmentLimitError
	if !errors.As(err, &limit) || limit.Failures.Consecutive != 3 || retry.Temporary(err) {
		t.Fatal("missing permanent failure stop", err)
	}
	if fmt.Sprint(m.order) != "[1 2 3 4]" || failing.calls[1] != 2 || failing.calls[3] != 2 || failing.calls[4] != 2 || failing.calls[5] != 0 {
		t.Fatal("wrong attempt budget or timeline order", failing.calls, m.order)
	}
	stats, err := e.Journal.Status()
	if err != nil || stats["attachments_failed"] != int64(3) || stats["cleanup_pending"] != int64(3) {
		t.Fatal("failed files lost cleanup ownership", stats, err)
	}
	reopen(t, e, path)
	// Even raising the limit must not silently clear a previously latched stop.
	e.ConsecutiveFailureLimit = 10
	if err = e.Run(context.Background()); !errors.As(err, &limit) || failing.calls[5] != 0 {
		t.Fatal("restart bypassed failure stop", err)
	}
	if err = e.Journal.ResetAttachmentFailures(); err != nil {
		t.Fatal(err)
	}
	if err = e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(m.order) != "[1 2 3 4 5]" || failing.calls[1] != 2 || failing.calls[5] != 2 {
		t.Fatal("explicit resume repeated sent messages", failing.calls, m.order)
	}
}

func TestAttachmentStreakIsAtomicWithSendAndSuccessResetsIt(t *testing.T) {
	e, path, _, _, _ := fixture(t)
	defer func() { e.Journal.Close() }()
	if err := e.Journal.AddPage([]Message{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}}, 5); err != nil {
		t.Fatal(err)
	}
	commit := func(id int64, state string) {
		t.Helper()
		if err := e.Journal.SaveMedia(id, Media{State: state}); err != nil {
			t.Fatal(err)
		}
		if err := e.Journal.SentWithAttachmentLimit(id, fmt.Sprintf("$%d", id), 3); err != nil {
			t.Fatal(err)
		}
	}
	commit(1, "failed")
	commit(2, "") // A text message does not reset the streak.
	commit(3, "failed")
	reopen(t, e, path)
	// A replay after a lost response must not count the same attachment twice.
	if err := e.Journal.SentWithAttachmentLimit(3, "$3", 3); err != nil {
		t.Fatal(err)
	}
	s, err := e.Journal.CheckAttachmentFailures(3)
	if err != nil || s.Consecutive != 2 {
		t.Fatal(s, err)
	}
	commit(4, "ready")
	commit(5, "failed")
	s, err = e.Journal.CheckAttachmentFailures(3)
	if err != nil || s.Consecutive != 1 || s.LastMessageID != 5 {
		t.Fatal(s, err)
	}
}

func TestStorageErrorsDoNotSpendAttachmentAttempts(t *testing.T) {
	e, path, s, _, o := fixture(t)
	defer func() { e.Journal.Close() }()
	o.failStorage = true
	if err := e.Run(context.Background()); err == nil {
		t.Fatal("storage failure ignored")
	}
	r, err := e.Journal.Next()
	if err != nil || r.Media.Attempts != 0 {
		t.Fatal("storage failure consumed file budget", r, err)
	}
	reopen(t, e, path)
	o.failStorage = false
	s.fail = true
	if err = e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Infrastructure retries leave the full file budget available after recovery.
	if len(s.offsets) != e.Attempts*2 {
		t.Fatal("wrong independent retry budgets", s.offsets)
	}
}

func TestCanceledAttachmentDoesNotSpendRetryBudget(t *testing.T) {
	e, path, s, _, _ := fixture(t)
	defer func() { e.Journal.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	reopen(t, e, path)
	r, err := e.Journal.Next()
	state, stateErr := e.Journal.AttachmentFailures()
	if err != nil || stateErr != nil || r.Media.Attempts != 0 || state.Consecutive != 0 {
		t.Fatal("cancellation counted as a failed attachment", err, stateErr, state)
	}
}

func TestAttachmentAttemptBudgetSurvivesInterruptedRetry(t *testing.T) {
	e, path, s, m, _ := fixture(t)
	defer func() { e.Journal.Close() }()
	failing := &failingFiles{source: s, calls: map[int64]int{}, err: context.DeadlineExceeded}
	e.Source = failing
	ctx, cancel := context.WithCancel(context.Background())
	e.Progress = func(progress map[string]any) {
		if progress["attachment_attempts"] == 1 {
			cancel()
		}
	}
	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	reopen(t, e, path)
	e.Progress = nil
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if failing.calls[4] != e.Attempts || fmt.Sprint(m.order) != "[1 4 9]" {
		t.Fatal("restart reset attempts or changed timeline", failing.calls, m.order)
	}
}

func TestExistingJournalFailureStreakIsNotLostOnUpgrade(t *testing.T) {
	e, _, _, _, _ := fixture(t)
	defer e.Journal.Close()
	if err := e.Journal.AddPage([]Message{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}}, 5); err != nil {
		t.Fatal(err)
	}
	// Simulate rows committed by the old binary without the new counter.
	for id, state := range map[int64]string{1: "failed", 2: "ready", 3: "failed", 4: "", 5: "failed"} {
		if err := e.Journal.SaveMedia(id, Media{State: state}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.Journal.db.Exec("UPDATE messages SET event_id=? WHERE id=?", fmt.Sprintf("$%d", id), id); err != nil {
			t.Fatal(err)
		}
	}
	s, err := e.Journal.CheckAttachmentFailures(2)
	var limit *AttachmentLimitError
	if !errors.As(err, &limit) || s.Consecutive != 2 || s.LastMessageID != 5 || !s.Blocked {
		t.Fatal("upgrade lost prior consecutive failures", s, err)
	}
}
