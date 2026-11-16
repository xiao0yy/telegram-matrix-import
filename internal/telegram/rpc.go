package telegram

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const requestTimeout = 2 * time.Minute

// WithRequestPolicy also wraps download pools: those invokers bypass the
// primary client's middleware. Limit each RPC, not the complete attachment,
// so a large file can make progress for as long as it needs.
func WithRequestPolicy(next tg.Invoker, report func(map[string]any)) tg.Invoker {
	return &requestInvoker{next: next, timeout: requestTimeout, report: report}
}

type requestInvoker struct {
	next      tg.Invoker
	timeout   time.Duration
	report    func(map[string]any)
	wait      func(context.Context, time.Duration) error
	onTimeout func(error)
}

func downloadRequests(ctx context.Context, next tg.Invoker, timeout time.Duration, report func(map[string]any)) (context.Context, tg.Invoker, context.CancelFunc) {
	child, cancel := context.WithCancelCause(ctx)
	// The SDK downloader retries request timeouts internally without a bound.
	// Cancel this attempt so control returns to the durable outer retry, which
	// closes the stale pool and creates a fresh connection.
	rpc := &requestInvoker{next: next, timeout: timeout, report: report, onTimeout: cancel}
	return child, rpc, func() { cancel(context.Canceled) }
}

func (p *requestInvoker) event(state string, in bin.Encoder, duration time.Duration) {
	if p.report != nil {
		// Only the request type is logged, never request fields or raw responses.
		p.report(map[string]any{"event": "telegram_request", "state": state,
			"request": fmt.Sprintf("%T", in), "seconds": duration.Seconds(),
			"at": time.Now().UTC().Format(time.RFC3339Nano)})
	}
}

func waitRequest(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *requestInvoker) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		call, cancel := context.WithTimeout(ctx, p.timeout)
		err := p.next.Invoke(call, in, out)
		timedOut := errors.Is(call.Err(), context.DeadlineExceeded)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var netErr net.Error
		if timedOut || errors.Is(err, context.DeadlineExceeded) || tgerr.Is(err, tg.ErrTimeout) || (errors.As(err, &netErr) && netErr.Timeout()) {
			p.event("timeout", in, p.timeout)
			failure := fmt.Errorf("Telegram %T request timed out (limit %s): %w", in, p.timeout, context.DeadlineExceeded)
			if p.onTimeout != nil {
				p.onTimeout(failure)
			}
			return failure
		}
		wait, limited := tgerr.AsFloodWait(err)
		if !limited {
			return err
		}
		// Telegram's required cooldown is outside the RPC deadline. Do not
		// repeatedly time out and reconnect to evade a server-issued flood wait.
		wait += time.Second
		p.event("rate_limited", in, wait)
		pause := p.wait
		if pause == nil {
			pause = waitRequest
		}
		if err = pause(ctx, wait); err != nil {
			return err
		}
		p.event("retry_after_rate_limit", in, 0)
	}
}
