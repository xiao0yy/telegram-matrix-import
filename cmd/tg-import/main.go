package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gotd/td/session"
	tgclient "github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/xiao0yy/telegram-matrix-import/internal/bridge"
	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
	"github.com/xiao0yy/telegram-matrix-import/internal/matrix"
	"github.com/xiao0yy/telegram-matrix-import/internal/retry"
	"github.com/xiao0yy/telegram-matrix-import/internal/s3"
	source "github.com/xiao0yy/telegram-matrix-import/internal/telegram"
	"golang.org/x/term"
	"maunium.net/go/mautrix"
)

var version = "dev"
var commit = "unknown"

type config struct {
	Attachments importer.AttachmentPolicy `json:"attachments"`
	State       string                    `json:"state"`
	Telegram    struct {
		APIID               int         `json:"api_id"`
		APIHash             string      `json:"api_hash"`
		Peer                source.Peer `json:"peer"`
		BorrowBridgeSession bool        `json:"borrow_bridge_session"`
	} `json:"telegram"`
	Matrix matrix.Config `json:"matrix"`
	S3     s3.Client     `json:"s3"`
	Bridge bridge.Config `json:"bridge"`
}

func load(path string) (config, error) {
	var c config
	s, e := os.Stat(path)
	if e != nil {
		return c, e
	}
	if s.Mode().Perm()&0077 != 0 {
		return c, errors.New("config must have mode 0600")
	}
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	if c.Attachments, e = c.Attachments.Defaults(); e != nil {
		return c, e
	}
	if !filepath.IsAbs(c.State) {
		return c, errors.New("state must be an absolute private directory")
	}
	if e = os.MkdirAll(c.State, 0700); e != nil {
		return c, e
	}
	s, e = os.Stat(c.State)
	if e != nil {
		return c, e
	}
	if s.Mode().Perm()&0077 != 0 {
		return c, errors.New("state directory must have mode 0700")
	}
	return c, nil
}
func emit(v any)                    { _ = json.NewEncoder(os.Stdout).Encode(v) }
func requestEvent(v map[string]any) { _ = json.NewEncoder(os.Stderr).Encode(v) }
func main() {
	syscall.Umask(0077)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e := run(ctx); e != nil {
		fmt.Fprintln(os.Stderr, e)
		if retry.Temporary(classifyFailure(e)) {
			os.Exit(75)
		}
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	flags := flag.NewFlagSet("tg-import", flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: tg-import [-config config.json] <command>")
		fmt.Fprintln(flags.Output(), "Commands: version, login, dialogs, inspect, create-room, run, catch-up, status, reset-attachment-failures, cleanup, handoff")
		flags.PrintDefaults()
	}
	path := flags.String("config", "config.json", "private JSON configuration")
	if e := flags.Parse(os.Args[1:]); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	args := flags.Args()
	if len(args) != 1 {
		return errors.New("usage: tg-import -config config.json login|dialogs|inspect|create-room|run|catch-up|status|reset-attachment-failures|cleanup|handoff")
	}
	cmd := args[0]
	if cmd == "version" {
		emit(map[string]string{"version": version, "commit": commit})
		return nil
	}
	c, e := load(*path)
	if e != nil {
		return e
	}
	open := importer.Open
	if cmd == "status" {
		open = importer.OpenReadOnly
	}
	j, e := open(filepath.Join(c.State, "journal.db"))
	if e != nil {
		return e
	}
	defer j.Close()
	if cmd == "status" {
		s, e := j.Status()
		if e != nil {
			return e
		}
		phase, _ := j.Get("phase")
		boundary, _ := j.Get("boundary")
		s["phase"] = phase
		s["through_telegram_id"] = boundary
		if raw, err := j.Get("execution"); err != nil {
			return err
		} else if raw != "" {
			s["execution"] = json.RawMessage(raw)
		}
		failures, err := j.AttachmentFailures()
		if err != nil {
			return err
		}
		s["attachment_failures"] = failures
		s["attachment_policy"] = c.Attachments
		emit(s)
		return nil
	}
	if cmd == "reset-attachment-failures" {
		if e = j.ResetAttachmentFailures(); e != nil {
			return e
		}
		emit(map[string]string{"next": "resume the original run or catch-up command with the bridge stopped; existing failed messages are unchanged"})
		return nil
	}
	// Keep the exclusive journal lock while waiting. Each attempt reopens the
	// Matrix/Telegram clients but reuses the same journal and event transactions.
	if cmd == "run" || cmd == "catch-up" || cmd == "cleanup" || cmd == "handoff" {
		policy := retry.Policy{Report: func(status retry.Status) error {
			raw, err := json.Marshal(struct {
				Command string `json:"command"`
				retry.Status
			}{cmd, status})
			if err != nil {
				return err
			}
			if err = j.Set("execution", string(raw)); err != nil {
				return err
			}
			requestEvent(map[string]any{"event": "import_execution", "execution": json.RawMessage(raw)})
			return nil
		}}
		return policy.Run(ctx, func(ctx context.Context) error { return classifyFailure(execute(ctx, c, j, cmd)) })
	}
	return execute(ctx, c, j, cmd)
}

func execute(ctx context.Context, c config, j *importer.Journal, cmd string) error {
	if saved, err := j.Get("peer"); err != nil {
		return err
	} else if saved != "" {
		var peer source.Peer
		if err = json.Unmarshal([]byte(saved), &peer); err != nil {
			return err
		}
		if c.Telegram.Peer.ID != 0 && c.Telegram.Peer.ID != peer.ID {
			return errors.New("configured peer differs from this import")
		}
		if c.Telegram.Peer.Username != "" && !strings.EqualFold(strings.TrimPrefix(c.Telegram.Peer.Username, "@"), peer.Username) {
			return errors.New("configured username differs from this import")
		}
		c.Telegram.Peer = peer
	}
	mx, e := matrix.New(c.Matrix, filepath.Join(c.State, "matrix"))
	if e != nil {
		return e
	}
	defer mx.Close()
	jobHash := sha256.Sum256([]byte(c.State + fmt.Sprint(c.Telegram.Peer.ID)))
	jobID := "tg-import-" + hex.EncodeToString(jobHash[:12])
	if cmd == "create-room" {
		if c.Telegram.Peer.ID == 0 {
			return errors.New("run inspect first to identify the conversation")
		}
		if e = c.Bridge.Stopped(ctx); e != nil {
			return e
		}
		room, e := mx.CreateRoom(ctx, jobID, c.Telegram.Peer.Title, c.Bridge.Owner)
		if e != nil {
			return e
		}
		if e = j.Set("room", room); e != nil {
			return e
		}
		emit(map[string]string{"room_id": room, "next": "join the invited encrypted room, then run"})
		return nil
	}
	if mx.Room == "" {
		mx.Room, e = j.Get("room")
		if e != nil {
			return e
		}
	}
	identityRaw, _ := json.Marshal([]any{c.Telegram.APIID, c.Telegram.Peer.Kind, c.Telegram.Peer.ID, c.Bridge.LoginID, c.Matrix.Server, mx.Room, c.Matrix.GhostPrefix, c.S3.Endpoint, c.S3.Bucket, c.Matrix.MediaPrefix})
	identityHash := sha256.Sum256(identityRaw)
	identity := hex.EncodeToString(identityHash[:])
	engine := &importer.Engine{Journal: j, Matrix: mx, Objects: &c.S3, Identity: identity, Attempts: c.Attachments.MaxAttempts, ConsecutiveFailureLimit: c.Attachments.MaxConsecutiveFailures, RetryDelay: 5 * time.Second, Progress: func(progress map[string]any) { emit(progress) }, BeforeMessage: c.Bridge.Stopped}
	if cmd == "cleanup" {
		if e = j.Bind(identity); e != nil {
			return e
		}
		return engine.Cleanup(ctx)
	}
	if cmd == "handoff" {
		if e = j.Bind(identity); e != nil {
			return e
		}
		return c.Bridge.Apply(ctx, j, c.Telegram.Peer.Kind, c.Telegram.Peer.ID, c.Telegram.Peer.Title, mx.Room, c.Matrix.Server, c.Matrix.GhostPrefix, filepath.Join(c.State, "bridge-backups"), func(ctx context.Context) error {
			return mx.CopyRoomKeys(ctx, c.Bridge.Database, c.Bridge.CryptoAccount, c.Bridge.CryptoPickleKey)
		})
	}
	if cmd != "login" && cmd != "dialogs" && cmd != "inspect" && cmd != "run" && cmd != "catch-up" {
		return errors.New("unknown command")
	}
	if cmd == "run" || cmd == "catch-up" {
		if e = c.Bridge.Stopped(ctx); e != nil {
			return e
		}
		if mx.Room == "" {
			return errors.New("create a dedicated import room first")
		}
		if e = mx.CheckImportRoom(ctx, jobID, c.Bridge.Owner); e != nil {
			return e
		}
		if e = mx.InitCrypto(ctx); e != nil {
			return e
		}
	}
	storage := source.AtomicStorage{Path: filepath.Join(c.State, "telegram.session")}
	if c.Telegram.BorrowBridgeSession {
		if cmd == "login" {
			return errors.New("borrowed sessions cannot run login; stop the bridge and use inspect")
		}
		borrowed, err := c.Bridge.Session(ctx)
		if err != nil {
			return err
		}
		loader := session.Loader{Storage: storage}
		old, err := loader.Load(ctx)
		if err == nil {
			if !bytes.Equal(old.AuthKey, borrowed.AuthKey) {
				return errors.New("bridge login changed; start a separate import state")
			}
		} else if errors.Is(err, session.ErrNotFound) {
			h := sha1.Sum(borrowed.AuthKey) // MTProto auth_key_id is the final 64 bits of SHA-1.
			if err = loader.Save(ctx, &session.Data{DC: borrowed.DC, Addr: borrowed.Addr, AuthKey: borrowed.AuthKey, AuthKeyID: h[12:], Salt: borrowed.Salt}); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	client := tgclient.NewClient(c.Telegram.APIID, c.Telegram.APIHash, tgclient.Options{SessionStorage: storage, NoUpdates: true, Middlewares: []tgclient.Middleware{tgclient.MiddlewareFunc(func(next tg.Invoker) tgclient.InvokeFunc {
		return source.WithRequestPolicy(next, requestEvent).Invoke
	})}})
	return client.Run(ctx, func(ctx context.Context) error {
		if cmd == "login" {
			if e := client.Auth().IfNecessary(ctx, auth.NewFlow(terminalAuth{}, auth.SendCodeOptions{})); e != nil {
				return e
			}
			self, e := client.Self(ctx)
			if e != nil {
				return e
			}
			emit(map[string]any{"telegram_user_id": self.ID})
			return nil
		}
		status, e := client.Auth().Status(ctx)
		if e != nil {
			return e
		}
		if !status.Authorized {
			return errors.New("run login first")
		}
		if cmd == "dialogs" {
			return dialogs(ctx, client)
		}
		self, e := client.Self(ctx)
		if e != nil {
			return e
		}
		if fmt.Sprint(self.ID) != c.Bridge.LoginID {
			return errors.New("Telegram account differs from bridge login")
		}
		peer, err := source.Resolve(ctx, client, c.Telegram.Peer)
		if err != nil {
			return err
		}
		src := &source.Source{Client: client, Peer: peer, SelfID: self.ID, Report: requestEvent}
		if cmd == "inspect" {
			latest, err := src.Latest(ctx)
			if err != nil {
				return err
			}
			page, err := src.Page(ctx, 0, latest, 5)
			if err != nil {
				return err
			}
			peer.AccessHash = 0 // Resolve fresh access hashes on every connection.
			raw, err := json.Marshal(peer)
			if err != nil {
				return err
			}
			if err = j.Set("peer", string(raw)); err != nil {
				return err
			}
			first := int64(0)
			if len(page) > 0 {
				first = page[0].ID
			}
			emit(map[string]any{"peer": peer, "latest_message_id": latest, "first_message_id": first, "first_page_messages": len(page)})
			return nil
		}
		engine.Source = src
		if cmd == "catch-up" {
			if e = j.Bind(identity); e != nil {
				return e
			}
			phase, _ := j.Get("phase")
			if phase != "imported" {
				return errors.New("catch-up requires a completed import before bridge handoff")
			}
			latest, e := src.Latest(ctx)
			if e != nil {
				return e
			}
			if e = j.Set("boundary", fmt.Sprint(latest)); e != nil {
				return e
			}
		}
		if phase, _ := j.Get("phase"); phase == "handed-off" {
			return errors.New("bridge already owns this room; import is closed")
		}
		return engine.Run(ctx)
	})
}

type temporaryFailure struct {
	err   error
	delay time.Duration
}

func (e *temporaryFailure) Error() string             { return e.err.Error() }
func (e *temporaryFailure) Unwrap() error             { return e.err }
func (e *temporaryFailure) Retryable() bool           { return true }
func (e *temporaryFailure) RetryAfter() time.Duration { return e.delay }

func classifyFailure(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	var explicit interface{ Retryable() bool }
	if errors.As(err, &explicit) && !explicit.Retryable() {
		return err
	}
	if rpc, ok := tgerr.As(err); ok && rpc.Code >= 500 && rpc.Code <= 599 {
		return &temporaryFailure{err: err}
	}
	if delay, ok := tgerr.AsFloodWait(err); ok {
		return &temporaryFailure{err: err, delay: delay + time.Second}
	}
	var httpErr mautrix.HTTPError
	if errors.As(err, &httpErr) && httpErr.Response != nil && retry.HTTPStatus(httpErr.Response.StatusCode) {
		return &temporaryFailure{err: err, delay: retry.RetryAfter(httpErr.Response.Header.Get("Retry-After"))}
	}
	return err
}

type terminalAuth struct{}

func readSecret(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt+": ")
	b, e := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	return strings.TrimSpace(string(b)), e
}
func (terminalAuth) Phone(context.Context) (string, error) { return readSecret("Telegram phone") }
func (terminalAuth) Password(context.Context) (string, error) {
	return readSecret("Telegram 2FA password")
}
func (terminalAuth) Code(context.Context, *tg.AuthSentCode) (string, error) {
	return readSecret("Telegram login code")
}
func (terminalAuth) AcceptTermsOfService(context.Context, tg.HelpTermsOfService) error {
	return errors.New("only existing Telegram accounts are supported")
}
func (terminalAuth) SignUp(context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("account creation is not supported")
}
func dialogs(ctx context.Context, c *tgclient.Client) error {
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	enc := json.NewEncoder(w)
	return source.VisitConversations(ctx, c, func(p source.Peer) error {
		return enc.Encode(map[string]any{"kind": p.Kind, "id": p.ID, "title": p.Title})
	})
}
