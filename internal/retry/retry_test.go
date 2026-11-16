package retry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestAutomaticBackoffAndRecovery(t *testing.T) {
	var waits []time.Duration
	var states []Status
	calls := 0
	p := Policy{Initial: time.Second, Maximum: 4 * time.Second,
		Wait:   func(ctx context.Context, d time.Duration) error { waits = append(waits, d); return nil },
		Report: func(s Status) error { states = append(states, s); return nil },
	}
	err := p.Run(context.Background(), func(context.Context) error {
		calls++
		if calls < 5 {
			return fmt.Errorf("download: %w", context.DeadlineExceeded)
		}
		return nil
	})
	if err != nil || fmt.Sprint(waits) != "[1s 2s 4s 4s]" || states[len(states)-1].State != "complete" {
		t.Fatalf("automatic resume failed: %v %v %v", err, waits, states)
	}
	if states[1].NextRetry.IsZero() || states[1].Error == "" {
		t.Fatal("waiting status omitted deadline or reason")
	}
}

func TestPermanentFailureAndCancellationDoNotRestart(t *testing.T) {
	for _, stop := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		last := ""
		p := Policy{Report: func(s Status) error { last = s.State; return nil }, Wait: func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }}
		err := p.Run(ctx, func(context.Context) error {
			calls++
			if stop {
				return context.DeadlineExceeded
			}
			return errors.New("credentials rejected")
		})
		cancel()
		want := "blocked"
		if stop {
			want = "stopped"
		}
		if err == nil || calls != 1 || last != want {
			t.Fatalf("unexpected restart: %v %d %s", err, calls, last)
		}
	}
}

type limited struct{}

func (limited) Error() string             { return "limited" }
func (limited) Retryable() bool           { return true }
func (limited) RetryAfter() time.Duration { return time.Hour }

func TestServerCooldownIsNotCapped(t *testing.T) {
	calls := 0
	p := Policy{Wait: func(_ context.Context, d time.Duration) error {
		if d != time.Hour {
			t.Fatal(d)
		}
		return nil
	}}
	if err := p.Run(context.Background(), func(context.Context) error {
		calls++
		if calls == 1 {
			return limited{}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestErrorClassification(t *testing.T) {
	for _, test := range []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("wrapped: %w", syscall.ECONNRESET), true},
		{&net.DNSError{IsTimeout: true}, true},
		{&net.DNSError{IsNotFound: true}, false},
		{context.Canceled, false}, {errors.New("permission denied"), false},
	} {
		if got := Temporary(test.err); got != test.want {
			t.Errorf("%v: %v", test.err, got)
		}
	}
}

func TestSDKGracefulShutdownDoesNotReportCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	last := ""
	p := Policy{Report: func(s Status) error { last = s.State; return nil }}
	err := p.Run(ctx, func(context.Context) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || last != "stopped" {
		t.Fatalf("cancellation reported as completion: %v %s", err, last)
	}
}
