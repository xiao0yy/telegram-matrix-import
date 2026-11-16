package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
	"github.com/xiao0yy/telegram-matrix-import/internal/matrix"
	"github.com/xiao0yy/telegram-matrix-import/internal/retry"
	"github.com/xiao0yy/telegram-matrix-import/internal/s3"
	"maunium.net/go/mautrix"
)

func TestTransientAndPermanentCommandErrors(t *testing.T) {
	for _, test := range []struct {
		err  error
		want bool
	}{
		{&s3.Error{Status: 503}, true}, {&s3.Error{Status: 403}, false},
		{&s3.Error{Status: 200, Code: "SlowDown"}, true},
		{&matrix.APIError{Status: 429, Delay: time.Minute}, true}, {&matrix.APIError{Status: 401}, false},
		{mautrix.HTTPError{Response: &http.Response{StatusCode: 502}}, true},
		{mautrix.HTTPError{Response: &http.Response{StatusCode: 403}}, false},
		{&tgerr.Error{Code: 500, Type: "TIMEOUT"}, true},
		{&tgerr.Error{Code: 401, Type: "AUTH_KEY_UNREGISTERED"}, false},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), true},
		{errors.New("bridge must remain stopped"), false},
	} {
		if got := retry.Temporary(classifyFailure(test.err)); got != test.want {
			t.Errorf("%T: got %v, want %v", test.err, got, test.want)
		}
	}
}
