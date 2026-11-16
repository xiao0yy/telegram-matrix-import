package telegram

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	tgclient "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
)

type invokeFunc func(context.Context, bin.Encoder, bin.Decoder) error

func (f invokeFunc) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	return f(ctx, in, out)
}

func TestDownloadPoolRequestCannotWaitForever(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var state string
	next := invokeFunc(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
		if _, ok := in.(*tg.UploadGetFileRequest); !ok {
			t.Errorf("unexpected request type: %T", in)
		}
		<-ctx.Done()
		return ctx.Err()
	})
	downloadCtx, rpc, cancelDownload := downloadRequests(ctx, next, 10*time.Millisecond, func(v map[string]any) { state, _ = v["state"].(string) })
	defer cancelDownload()
	// Exercise the actual SDK path used by independent download pools, rather
	// than only testing a primary-client middleware which pools bypass.
	client := &tgclient.Client{}
	_, err := client.DownloadVia(rpc, &tg.InputDocumentFileLocation{}).
		WithSize(1<<20).WithThreads(1).Stream(downloadCtx, io.Discard)
	if err == nil || !errors.Is(context.Cause(downloadCtx), context.DeadlineExceeded) || ctx.Err() != nil || state != "timeout" {
		t.Fatalf("download did not honor per-request deadline: %v; parent=%v; state=%s", err, ctx.Err(), state)
	}
}

func TestServerTimeoutAlsoEscapesSDKRetryLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	calls := 0
	next := invokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		calls++
		return &tgerr.Error{Code: 500, Type: tg.ErrTimeout}
	})
	downloadCtx, rpc, cancelDownload := downloadRequests(ctx, next, time.Second, nil)
	defer cancelDownload()
	client := &tgclient.Client{}
	_, err := client.DownloadVia(rpc, &tg.InputDocumentFileLocation{}).
		WithSize(1<<20).WithThreads(1).Stream(downloadCtx, io.Discard)
	if err == nil || ctx.Err() != nil || calls != 1 || !tgerr.Is(context.Cause(downloadCtx), tg.ErrTimeout) {
		t.Fatalf("server timeout stayed in SDK retry loop: %v calls=%d", err, calls)
	}
}

func TestDownloadFailuresUseFileBudgetExceptAccountAndRateLimits(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		attachment bool
	}{
		{"server timeout", &requestTimeoutError{Request: "*tg.UploadGetFileRequest", Server: true, Cause: &tgerr.Error{Code: 500, Type: tg.ErrTimeout}}, true},
		{"network timeout", &requestTimeoutError{Request: "*tg.UploadGetFileRequest", Cause: context.DeadlineExceeded}, true},
		{"server outage", &tgerr.Error{Code: 500, Type: "INTERNAL"}, true},
		{"file missing", &tgerr.Error{Code: 400, Type: "FILE_ID_INVALID"}, true},
		{"authorization", &tgerr.Error{Code: 401, Type: "AUTH_KEY_UNREGISTERED"}, false},
		{"permission", &tgerr.Error{Code: 403, Type: "CHAT_ADMIN_REQUIRED"}, false},
		{"session", &tgerr.Error{Code: 406, Type: "AUTH_KEY_DUPLICATED"}, false},
		{"flood wait", &tgerr.Error{Code: 420, Type: "FLOOD_WAIT", Argument: 60}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := downloadFailure(test.err)
			var attachment *importer.AttachmentError
			var unavailable *importer.SourceUnavailable
			if errors.As(err, &attachment) != test.attachment || errors.As(err, &unavailable) == test.attachment || !errors.Is(err, test.err) {
				t.Fatalf("wrong classification: %v", err)
			}
		})
	}
}

func TestFloodWaitUsesParentContextAndFreshRequestDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	parentDeadline, _ := ctx.Deadline()
	calls := 0
	var states []string
	rpc := &requestInvoker{
		timeout: 20 * time.Millisecond,
		next: invokeFunc(func(call context.Context, in bin.Encoder, out bin.Decoder) error {
			deadline, ok := call.Deadline()
			if !ok || !deadline.Before(parentDeadline) || call.Err() != nil {
				t.Fatal("request does not have a fresh bounded context")
			}
			calls++
			if calls == 1 {
				return &tgerr.Error{Code: 420, Type: "FLOOD_WAIT", Argument: 2}
			}
			return nil
		}),
		wait: func(waitCtx context.Context, duration time.Duration) error {
			if waitCtx != ctx || duration != 3*time.Second {
				t.Fatal("server cooldown must use parent context, outside RPC timeout")
			}
			return nil
		},
		report: func(v map[string]any) { states = append(states, v["state"].(string)) },
	}
	if err := rpc.Invoke(ctx, &tg.UploadGetFileRequest{}, &tg.UploadFileBox{}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(states) != 2 || states[0] != "rate_limited" || states[1] != "retry_after_rate_limit" {
		t.Fatalf("incorrect flood-wait handling: calls=%d states=%v", calls, states)
	}
}

func TestCancelFloodWaitDoesNotRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	rpc := &requestInvoker{
		timeout: time.Second,
		next: invokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
			calls++
			return &tgerr.Error{Code: 420, Type: "FLOOD_WAIT", Argument: 60}
		}),
		report: func(v map[string]any) { cancel() },
	}
	if err := rpc.Invoke(ctx, &tg.UploadGetFileRequest{}, &tg.UploadFileBox{}); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("canceled flood wait retried: %v calls=%d", err, calls)
	}
}
