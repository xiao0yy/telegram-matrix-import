package matrix

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
	"github.com/xiao0yy/telegram-matrix-import/internal/retry"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/cryptohelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type Config struct {
	URL            string `json:"url"`
	Server         string `json:"server"`
	Room           string `json:"room"`
	Bot            string `json:"bot"`
	ASToken        string `json:"as_token"`
	AdminToken     string `json:"admin_token"`
	AdminUser      string `json:"admin_user"`
	GhostPrefix    string `json:"ghost_prefix"`
	MediaPrefix    string `json:"media_prefix"`
	ClientIPHeader string `json:"client_ip_header,omitempty"`
}
type Client struct {
	Config
	HTTP     *http.Client
	bot      *mautrix.Client
	crypto   *cryptohelper.CryptoHelper
	ghosts   map[string]bool
	stateDir string
}
type APIError struct {
	Status  int
	Code    string
	Message string
	Delay   time.Duration
}

func (e *APIError) Retryable() bool           { return retry.HTTPStatus(e.Status) }
func (e *APIError) RetryAfter() time.Duration { return e.Delay }

func (e *APIError) Error() string {
	return fmt.Sprintf("Matrix HTTP %d (%s): %s", e.Status, e.Code, e.Message)
}
func (c *Client) request(ctx context.Context, method, path, user string, body []byte, kind string, limit int64, admin bool) ([]byte, http.Header, error) {
	u, err := url.Parse(c.URL + path)
	if err != nil {
		return nil, nil, err
	}
	q := u.Query()
	if user != "" {
		q.Set("user_id", user)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	token := c.ASToken
	if admin {
		token = c.AdminToken
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", kind)
	req.Header.Set("Cache-Control", "no-store")
	res, err := c.HTTP.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return nil, nil, err
	}
	if int64(len(b)) > limit {
		return nil, nil, errors.New("Matrix response exceeded bounded response limit")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var x struct {
			Code         string `json:"errcode"`
			Message      string `json:"error"`
			RetryAfterMS int64  `json:"retry_after_ms"`
		}
		_ = json.Unmarshal(b, &x)
		if x.Message == "" {
			x.Message = strings.TrimSpace(string(b))
		}
		for _, secret := range []string{c.ASToken, c.AdminToken} {
			if secret != "" {
				x.Message = strings.ReplaceAll(x.Message, secret, "[redacted]")
			}
		}
		if len(x.Message) > 1000 {
			x.Message = x.Message[:1000]
		}
		delay := retry.RetryAfter(res.Header.Get("Retry-After"))
		if x.RetryAfterMS > 0 && x.RetryAfterMS <= int64((1<<63-1)/time.Millisecond) {
			delay = max(delay, time.Duration(x.RetryAfterMS)*time.Millisecond)
		}
		return nil, res.Header, &APIError{Status: res.StatusCode, Code: x.Code, Message: x.Message, Delay: delay}
	}
	return b, res.Header, nil
}
func New(cfg Config, state string) (*Client, error) {
	u, e := url.Parse(cfg.URL)
	if e != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid homeserver URL")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || u.Scheme != "http" || u.Path != "" {
		return nil, errors.New("media registration must connect directly to Tuwunel over a loopback HTTP listener")
	}
	if cfg.Server == "" || cfg.Bot == "" || cfg.ASToken == "" || cfg.GhostPrefix == "" || cfg.MediaPrefix == "" {
		return nil, errors.New("incomplete Matrix configuration")
	}
	cfg.ClientIPHeader = http.CanonicalHeaderKey(cfg.ClientIPHeader)
	switch cfg.ClientIPHeader {
	case "", "X-Real-Ip", "X-Forwarded-For", "Cf-Connecting-Ip":
	default:
		return nil, errors.New("unsupported client_ip_header; use X-Real-IP, X-Forwarded-For or CF-Connecting-IP")
	}
	transport := &localTransport{header: cfg.ClientIPHeader, host: u.Host, base: http.DefaultTransport}
	return &Client{Config: cfg, stateDir: state, ghosts: map[string]bool{}, HTTP: &http.Client{Transport: transport, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// The appservice and crypto SDK share this transport. Headers identifying a
// local client must never be forwarded to a different origin.
type localTransport struct {
	header string
	host   string
	base   http.RoundTripper
}

func (t *localTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "http" || req.URL.Host != t.host {
		return nil, errors.New("Matrix request must use the configured loopback origin")
	}
	if t.header != "" {
		req = req.Clone(req.Context())
		req.Header.Set(t.header, "127.0.0.1")
	}
	return t.base.RoundTrip(req)
}
func (c *Client) InitCrypto(ctx context.Context) error {
	b, _, err := c.request(ctx, "GET", "/_matrix/client/v3/rooms/"+url.PathEscape(c.Room)+"/state/m.room.encryption/", c.Bot, nil, "application/json", 1<<20, false)
	if err != nil {
		return err
	}
	var encryption struct {
		Algorithm string `json:"algorithm"`
	}
	if err = json.Unmarshal(b, &encryption); err != nil {
		return err
	}
	if encryption.Algorithm != "m.megolm.v1.aes-sha2" {
		return errors.New("target room must use Matrix Megolm encryption")
	}
	if err = os.MkdirAll(c.stateDir, 0700); err != nil {
		return err
	}
	path := filepath.Join(c.stateDir, "crypto.key")
	key, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		key, err = createCryptoKey(path)
	}
	if err != nil {
		return err
	}
	if len(key) != 32 {
		return errors.New("invalid importer crypto key; restore the original key before continuing")
	}
	cli, err := mautrix.NewClient(c.URL, id.UserID(c.Bot), c.ASToken)
	if err != nil {
		return err
	}
	cli.Client = c.HTTP
	cli.SetAppServiceUserID = true
	cli.SetAppServiceDeviceID = true
	helper, err := cryptohelper.NewCryptoHelper(cli, key, filepath.Join(c.stateDir, "crypto.db"))
	if err != nil {
		return err
	}
	helper.DBAccountID = "telegram-history-importer"
	helper.LoginAs = &mautrix.ReqLogin{Type: mautrix.AuthTypeAppservice, Identifier: mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: c.Bot}, InitialDeviceDisplayName: "Telegram history importer"}
	if err = helper.Init(ctx); err != nil {
		helper.Close()
		return err
	}
	if _, err = cli.State(ctx, id.RoomID(c.Room)); err == nil {
		_, err = cli.Members(ctx, id.RoomID(c.Room))
	}
	if err != nil {
		helper.Close()
		return err
	}
	c.bot = cli
	c.crypto = helper
	return nil
}
func createCryptoKey(path string) ([]byte, error) {
	if _, err := os.Stat(filepath.Join(filepath.Dir(path), "crypto.db")); !os.IsNotExist(err) {
		return nil, errors.New("crypto database exists without its key; restore the original key")
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".crypto-key-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(key); err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return nil, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	return key, dir.Sync()
}

func (c *Client) Close() error {
	if c.crypto != nil {
		return c.crypto.Close()
	}
	return nil
}

func (c *Client) CheckImportRoom(ctx context.Context, job, owner string) error {
	b, _, err := c.request(ctx, "GET", "/_matrix/client/v3/rooms/"+url.PathEscape(c.Room)+"/state/m.room.create/", c.Bot, nil, "", 1<<20, false)
	if err != nil {
		return err
	}
	var create map[string]any
	if err = json.Unmarshal(b, &create); err != nil {
		return err
	}
	if create["com.telegram.import.job"] != job {
		return errors.New("target room was not created by this import job")
	}
	b, _, err = c.request(ctx, "GET", "/_matrix/client/v3/rooms/"+url.PathEscape(c.Room)+"/state/m.room.member/"+url.PathEscape(owner), c.Bot, nil, "", 1<<20, false)
	if err != nil {
		return err
	}
	var membership struct {
		Membership string `json:"membership"`
	}
	if err = json.Unmarshal(b, &membership); err != nil {
		return err
	}
	if membership.Membership != "join" {
		return errors.New("owner must join the encrypted import room before messages are imported")
	}
	return nil
}
func (c *Client) CreateRoom(ctx context.Context, job, title, owner string) (string, error) {
	b, _, err := c.request(ctx, "GET", "/_matrix/client/v3/joined_rooms", c.Bot, nil, "", 4<<20, false)
	if err != nil {
		return "", err
	}
	var joined struct {
		Rooms []string `json:"joined_rooms"`
	}
	if err = json.Unmarshal(b, &joined); err != nil {
		return "", err
	}
	for _, room := range joined.Rooms {
		raw, _, err := c.request(ctx, "GET", "/_matrix/client/v3/rooms/"+url.PathEscape(room)+"/state/m.room.create/", c.Bot, nil, "", 1<<20, false)
		if err != nil {
			return "", err
		}
		var create map[string]any
		if err = json.Unmarshal(raw, &create); err != nil {
			return "", err
		}
		if create["com.telegram.import.job"] == job {
			return room, nil
		}
	}
	body, _ := json.Marshal(map[string]any{"name": title, "visibility": "private", "room_version": "11", "preset": "private_chat", "creation_content": map[string]string{"com.telegram.import.job": job}, "invite": []string{owner}, "power_level_content_override": map[string]any{"users": map[string]int{c.Bot: 100, owner: 100}}, "initial_state": []any{map[string]any{"type": "m.room.encryption", "state_key": "", "content": map[string]string{"algorithm": "m.megolm.v1.aes-sha2"}}, map[string]any{"type": "m.room.history_visibility", "state_key": "", "content": map[string]string{"history_visibility": "shared"}}}})
	b, _, err = c.request(ctx, "POST", "/_matrix/client/v3/createRoom", c.Bot, body, "application/json", 1<<20, false)
	if err != nil {
		return "", err
	}
	var r struct {
		ID string `json:"room_id"`
	}
	err = json.Unmarshal(b, &r)
	return r.ID, err
}

func (c *Client) Ghost(sender string) (string, error) {
	if !regexp.MustCompile(`^(?:channel-)?[0-9]+$`).MatchString(sender) {
		return "", errors.New("invalid Telegram sender ID")
	}
	return "@" + c.GhostPrefix + sender + ":" + c.Server, nil
}
func (c *Client) ensureGhost(ctx context.Context, m importer.Message) (string, error) {
	user, err := c.Ghost(m.Sender)
	if err != nil {
		return "", err
	}
	if c.ghosts[user] {
		return user, nil
	}
	body, _ := json.Marshal(map[string]any{"type": "m.login.application_service", "username": strings.TrimSuffix(strings.TrimPrefix(user, "@"), ":"+c.Server), "inhibit_login": true})
	_, _, err = c.request(ctx, "POST", "/_matrix/client/v3/register", "", body, "application/json", 1<<20, false)
	var apiErr *APIError
	if err != nil && !(errors.As(err, &apiErr) && apiErr.Code == "M_USER_IN_USE") {
		return "", err
	}
	body, _ = json.Marshal(map[string]string{"displayname": m.SenderName})
	if _, _, err = c.request(ctx, "PUT", "/_matrix/client/v3/profile/"+url.PathEscape(user)+"/displayname", user, body, "application/json", 1<<20, false); err != nil {
		return "", err
	}
	// Invite from the importing bot, then join as the registered bridge ghost.
	body, _ = json.Marshal(map[string]string{"user_id": user})
	_, _, inviteErr := c.request(ctx, "POST", "/_matrix/client/v3/rooms/"+url.PathEscape(c.Room)+"/invite", c.Bot, body, "application/json", 1<<20, false)
	_, _, err = c.request(ctx, "POST", "/_matrix/client/v3/join/"+url.PathEscape(c.Room), user, []byte("{}"), "application/json", 1<<20, false)
	if err != nil {
		return "", errors.Join(inviteErr, err)
	}
	c.ghosts[user] = true
	if c.bot != nil {
		if _, err = c.bot.Members(ctx, id.RoomID(c.Room)); err != nil {
			return "", err
		}
	}
	return user, nil
}
func (c *Client) Reserve(ctx context.Context) (string, error) {
	b, _, e := c.request(ctx, "POST", "/_matrix/media/v1/create", c.Bot, []byte("{}"), "application/json", 1<<20, false)
	if e != nil {
		return "", e
	}
	var r struct {
		URI string `json:"content_uri"`
	}
	e = json.Unmarshal(b, &r)
	if e == nil {
		_, e = c.mediaPath(r.URI)
	}
	return r.URI, e
}
func (c *Client) mediaPath(mxc string) (string, error) {
	u, e := url.Parse(mxc)
	if e != nil || u.Scheme != "mxc" || u.Host != c.Server || u.RawQuery != "" || u.Fragment != "" || strings.Count(u.Path, "/") != 1 || len(u.Path) < 2 {
		return "", errors.New("invalid local MXC")
	}
	return url.PathEscape(u.Host) + "/" + url.PathEscape(strings.TrimPrefix(u.Path, "/")), nil
}

// Key is specific to Tuwunel 2.0.0's mediaid_file serialization. Register
// verifies the stored tiny object via S3 before any multipart overwrite.
func Key(prefix, mxc, disposition, mime string) string {
	raw := bytes.Join([][]byte{[]byte(mxc), make([]byte, 8), []byte(disposition), []byte(mime)}, []byte{255})
	hash := sha256.Sum256(raw)
	return prefix + base64.RawURLEncoding.EncodeToString(hash[:])
}
func (c *Client) RegistrationKey(mxc, name string) (string, error) {
	if _, err := c.mediaPath(mxc); err != nil {
		return "", err
	}
	if !regexp.MustCompile(`^import-[0-9]+\.bin$`).MatchString(name) {
		return "", errors.New("invalid registration filename")
	}
	return Key(c.MediaPrefix, mxc, "attachment; filename="+name, "application/octet-stream"), nil
}
func (c *Client) Register(ctx context.Context, mxc, name string) (string, error) {
	p, e := c.mediaPath(mxc)
	if e != nil {
		return "", e
	}
	_, _, e = c.request(ctx, "PUT", "/_matrix/media/v3/upload/"+p+"?filename="+url.QueryEscape(name), c.Bot, []byte{0}, "application/octet-stream", 1<<20, false)
	var apiErr *APIError
	if e != nil && !(errors.As(e, &apiErr) && apiErr.Status == 409) {
		return "", e
	}
	b, h, e := c.request(ctx, "GET", "/_matrix/client/v1/media/download/"+p, c.Bot, nil, "", 2, false)
	if e != nil {
		return "", e
	}
	if !bytes.Equal(b, []byte{0}) || h.Get("Content-Type") != "application/octet-stream" || !strings.Contains(h.Get("Content-Disposition"), name) {
		return "", errors.New("media registration verification failed")
	}
	return Key(c.MediaPrefix, mxc, h.Get("Content-Disposition"), h.Get("Content-Type")), nil
}
func (c *Client) Prepare(ctx context.Context, m importer.Message, content map[string]any) (json.RawMessage, error) {
	if c.crypto == nil {
		return nil, errors.New("Matrix encryption is not initialized")
	}
	if _, err := c.ensureGhost(ctx, m); err != nil {
		return nil, err
	}
	// The bridge uses one encryption device and sends on behalf of its ghosts.
	// Use the same pattern, with a separate durable importer crypto store.
	enc, err := c.crypto.Encrypt(ctx, id.RoomID(c.Room), event.EventMessage, content)
	if err != nil {
		return nil, err
	}
	return json.Marshal(enc)
}
func (c *Client) Send(ctx context.Context, m importer.Message, txn string, p json.RawMessage) (string, error) {
	sender, err := c.Ghost(m.Sender)
	if err != nil {
		return "", err
	}
	path := "/_matrix/client/v3/rooms/" + url.PathEscape(c.Room) + "/send/m.room.encrypted/" + url.PathEscape(txn) + "?ts=" + fmt.Sprint(m.Time)
	b, _, err := c.request(ctx, "PUT", path, sender, p, "application/json", 1<<20, false)
	if err != nil {
		return "", err
	}
	var r struct {
		ID string `json:"event_id"`
	}
	err = json.Unmarshal(b, &r)
	return r.ID, err
}
func (c *Client) Delete(ctx context.Context, mxc string) error {
	p, e := c.mediaPath(mxc)
	if e != nil {
		return e
	}
	if c.AdminToken == "" {
		return errors.New("media cleanup requires a homeserver administrator token")
	}
	_, _, e = c.request(ctx, "DELETE", "/_synapse/admin/v1/media/"+p, c.AdminUser, nil, "application/json", 1<<20, true)
	var apiErr *APIError
	if errors.As(e, &apiErr) && apiErr.Status == 404 {
		return nil
	}
	return e
}
