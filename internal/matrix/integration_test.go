package matrix

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
	"github.com/xiao0yy/telegram-matrix-import/internal/s3"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto"
	"maunium.net/go/mautrix/crypto/cryptohelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Requires the empty, loopback-only fixture in tests/tuwunel_fixture.py. Never
// substitute a production homeserver or bucket for this fixture.
func TestIsolatedTuwunelNativeMXCReadsDirectS3Object(t *testing.T) {
	if os.Getenv("TG_IMPORT_ISOLATED_TEST") != "1" {
		t.Skip("start isolated fixture and set TG_IMPORT_ISOLATED_TEST=1")
	}
	ctx := context.Background()
	cfg := Config{URL: "http://127.0.0.1:19792", Server: "import.test", Bot: "@telegrambot:import.test", ASToken: "fixture-as-token", AdminToken: "fixture-as-token", AdminUser: "@owner:import.test", GhostPrefix: "telegram_", MediaPrefix: "media/"}
	c, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Ensure the appservice sender exists before using its media APIs.
	_, _, err = c.request(ctx, "POST", "/_matrix/client/v3/register", "", []byte(`{"type":"m.login.application_service","username":"telegrambot","inhibit_login":true}`), "application/json", 1<<20, false)
	if err != nil {
		if e, ok := err.(*APIError); !ok || e.Code != "M_USER_IN_USE" {
			t.Fatal(err)
		}
	}
	uri, err := c.Reserve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key, err := c.Register(ctx, uri, "import-1.bin")
	if err != nil {
		t.Fatal(err)
	}
	object := &s3.Client{Endpoint: "http://127.0.0.1:19791", Bucket: "fixture", Region: "us-east-1", Access: "fixture-access", Secret: "fixture-secret"}
	before, err := object.Head(ctx, key)
	if err != nil {
		t.Fatalf("native S3 key mismatch: %v", err)
	}
	if before.Size != 1 || before.Version == "" {
		t.Fatal(before)
	}
	data := bytes.Repeat([]byte("ciphertext-fixture"), 1000)
	upload, err := object.Start(ctx, key, uri)
	if err != nil {
		t.Fatal(err)
	}
	etag, err := object.PutPart(ctx, key, upload, 1, data)
	if err != nil {
		t.Fatal(err)
	}
	final, err := object.Complete(ctx, key, upload, []importer.Part{{Number: 1, ETag: etag, Size: int64(len(data))}})
	if err != nil {
		t.Fatal(err)
	}
	if final == before.Version || final == "" {
		t.Fatal("no new version")
	}
	path, _ := c.mediaPath(uri)
	got, _, err := c.request(ctx, "GET", "/_matrix/client/v1/media/download/"+path, c.Bot, nil, "", 1<<20, false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("MXC still serves the registration bytes")
	}
	after, err := object.Head(ctx, key)
	if err != nil || after.ImportID != uri || after.Size != int64(len(data)) {
		t.Fatal(after, err)
	}
	if err = object.DeleteVersion(ctx, key, before.Version); err != nil {
		t.Fatal(err)
	}
	if err = c.Delete(ctx, uri); err != nil {
		t.Fatal(err)
	}
	versions, err := object.Versions(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range versions {
		if err = object.DeleteVersion(ctx, key, v); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("native MXC served the directly uploaded bytes; exact-version cleanup passed")
}

func TestIsolatedEncryptedGhostMessageIsIdempotent(t *testing.T) {
	if os.Getenv("TG_IMPORT_ISOLATED_TEST") != "1" {
		t.Skip("requires isolated fixture")
	}
	ctx := context.Background()
	c, err := New(Config{URL: "http://127.0.0.1:19792", Server: "import.test", Bot: "@telegrambot:import.test", ASToken: "fixture-as-token", GhostPrefix: "telegram_", MediaPrefix: "media/"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	room, err := c.CreateRoom(ctx, fmt.Sprintf("fixture-%d", time.Now().UnixNano()), "Import test", "@owner:import.test")
	if err != nil {
		t.Fatal(err)
	}
	c.Room = room
	if _, _, err = c.request(ctx, "POST", "/_matrix/client/v3/join/"+room, "@owner:import.test", []byte("{}"), "application/json", 1<<20, false); err != nil {
		t.Fatal(err)
	}
	if err = c.InitCrypto(ctx); err != nil {
		t.Fatal(err)
	}
	reader, err := mautrix.NewClient(c.URL, "@owner:import.test", "")
	if err != nil {
		t.Fatal(err)
	}
	readerCrypto, err := cryptohelper.NewCryptoHelper(reader, []byte("fixture-only-encryption-key"), filepath.Join(t.TempDir(), "reader.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer readerCrypto.Close()
	readerCrypto.LoginAs = &mautrix.ReqLogin{Type: mautrix.AuthTypePassword, Identifier: mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: "owner"}, Password: "fixture-password", InitialDeviceDisplayName: "Importer test recipient"}
	if err = readerCrypto.Init(ctx); err != nil {
		t.Fatal(err)
	}
	message := importer.Message{ID: 1, Sender: "12345", SenderName: "Test sender", Time: 1600000000000, Text: "fixture"}
	payload, err := c.Prepare(ctx, message, map[string]any{"msgtype": "m.text", "body": "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]any
	if err = json.Unmarshal(payload, &content); err != nil {
		t.Fatal(err)
	}
	if content["algorithm"] != "m.megolm.v1.aes-sha2" {
		t.Fatal("not encrypted")
	}
	first, err := c.Send(ctx, message, "fixture-stable-transaction", payload)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Send(ctx, message, "fixture-stable-transaction", payload)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("duplicate event on retry")
	}
	sync, err := reader.SyncRequest(ctx, 0, "", "", false, event.PresenceOffline)
	if err != nil {
		t.Fatal(err)
	}
	readerCrypto.Machine().ProcessSyncResponse(ctx, sync, "")
	got, err := reader.GetEvent(ctx, id.RoomID(room), id.EventID(first))
	if err != nil {
		t.Fatal(err)
	}
	if got.Sender != "@telegram_12345:import.test" || got.Timestamp != message.Time {
		t.Fatal("sender or original timestamp changed", got.Sender, got.Timestamp)
	}
	if err = got.Content.ParseRaw(got.Type); err != nil {
		t.Fatal(err)
	}
	plain, err := readerCrypto.Decrypt(ctx, got)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Content.AsMessage().Body != "fixture" {
		t.Fatal("recipient decrypted the wrong content")
	}
	// Transfer just this room's history keys to a separate bridge-shaped store,
	// protected with a different pickle key. Its account must not be replaced.
	bridgePath := filepath.Join(t.TempDir(), "bridge-crypto.db")
	bridgeDB, err := dbutil.NewWithDialect(bridgePath, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer bridgeDB.Close()
	bridgeStore := crypto.NewSQLCryptoStore(bridgeDB, nil, "bridge-fixture", "bridge-device", []byte("different-bridge-key"))
	if err = bridgeStore.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	bridgeAccount := crypto.NewOlmAccount()
	if err = bridgeStore.PutAccount(ctx, bridgeAccount); err != nil {
		t.Fatal(err)
	}
	if err = c.CopyRoomKeys(ctx, bridgePath, "bridge-fixture", "different-bridge-key"); err != nil {
		t.Fatal(err)
	}
	session, err := bridgeStore.GetGroupSession(ctx, id.RoomID(room), id.SessionID(content["session_id"].(string)))
	if err != nil || session == nil {
		t.Fatal(session, err)
	}
	decoded, _, err := session.Internal.Decrypt([]byte(content["ciphertext"].(string)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(decoded, []byte(`"body":"fixture"`)) {
		t.Fatal("handed-off key cannot decrypt imported event")
	}
	t.Log("recipient decrypted the ghost's message; historical timestamp and transaction idempotency verified")
}
