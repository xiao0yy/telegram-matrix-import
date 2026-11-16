// Package retry resumes idempotent commands after transient failures. The
// caller keeps its journal lock throughout the wait and reopens connections.
package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"
)

func HTTPStatus(status int) bool {
	return status == 408 || status == 429 || status >= 500 && status <= 599
}

func RetryAfter(value string) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		return max(0, time.Until(when))
	}
	return 0
}

func Temporary(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var classified interface{ Retryable() bool }
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return dns.IsTimeout || dns.IsTemporary
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return true
	}
	for _, cause := range []error{syscall.ECONNRESET, syscall.ECONNREFUSED, syscall.EPIPE, syscall.ENETUNREACH, syscall.EHOSTUNREACH} {
		if errors.Is(err, cause) {
			return true
		}
	}
	return false
}

type Status struct {
	State     string    `json:"state"`
	Attempt   int       `json:"attempt"`
	Updated   time.Time `json:"updated_at"`
	NextRetry time.Time `json:"next_retry_at,omitempty"`
	Error     string    `json:"error,omitempty"`
}

type Policy struct {
	Initial   time.Duration
	Maximum   time.Duration
	Retryable func(error) bool
	Report    func(Status) error
	Wait      func(context.Context, time.Duration) error
}

func Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p Policy) Run(ctx context.Context, run func(context.Context) error) error {
	if p.Initial <= 0 {
		p.Initial = 30 * time.Second
	}
	if p.Maximum < p.Initial {
		p.Maximum = max(p.Initial, 5*time.Minute)
	}
	if p.Retryable == nil {
		p.Retryable = Temporary
	}
	if p.Wait == nil {
		p.Wait = Wait
	}
	report := func(s Status) error {
		s.Updated = time.Now().UTC()
		if p.Report != nil {
			return p.Report(s)
		}
		return nil
	}
	delay := p.Initial
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			_ = report(Status{State: "stopped", Attempt: attempt, Error: err.Error()})
			return err
		}
		if err := report(Status{State: "running", Attempt: attempt}); err != nil {
			return err
		}
		err := run(ctx)
		if err == nil {
			return report(Status{State: "complete", Attempt: attempt})
		}
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			_ = report(Status{State: "stopped", Attempt: attempt, Error: err.Error()})
			return err
		}
		if !p.Retryable(err) {
			_ = report(Status{State: "blocked", Attempt: attempt, Error: err.Error()})
			return err
		}
		// Honor a service-specified cooldown even if it exceeds our backoff cap.
		wait := delay
		var limited interface{ RetryAfter() time.Duration }
		if errors.As(err, &limited) {
			wait = max(wait, limited.RetryAfter())
		}
		if err = report(Status{State: "retry_wait", Attempt: attempt,
			NextRetry: time.Now().UTC().Add(wait), Error: err.Error()}); err != nil {
			return err
		}
		if err = p.Wait(ctx, wait); err != nil {
			_ = report(Status{State: "stopped", Attempt: attempt, Error: err.Error()})
			return err
		}
		delay = min(delay*2, p.Maximum)
	}
}
