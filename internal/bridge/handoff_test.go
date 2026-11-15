package bridge

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// Read the resulting rows through the actual bridge SDK, not a second copy
// of our SQL or an invented test schema.
func TestHandoffWithRealBridgeSchema(t *testing.T) {
	ctx := context.Background()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\nprintf 'false false sha256:fixture no\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	for _, kind := range []string{"user", "chat", "channel"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "bridge.db")
			raw, err := dbutil.NewWithDialect(path, "sqlite3")
			if err != nil {
				t.Fatal(err)
			}
			db := database.New("", database.MetaTypes{}, raw)
			defer db.Close()
			if err = db.Upgrade(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(ctx, `INSERT INTO "user"(bridge_id,mxid) VALUES('', '@owner:test'); INSERT INTO user_login(bridge_id,user_mxid,id,remote_name,metadata) VALUES('','@owner:test','123','Owner','{"untouched":"login"}')`); err != nil {
				t.Fatal(err)
			}
			journal, err := importer.Open(filepath.Join(dir, "journal.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			msgs := []importer.Message{{ID: 10, Sender: "123", SenderName: "Owner", Time: 1600000000123, Text: "one"}, {ID: 20, Sender: "456", SenderName: "Bot", Time: 1600000001123, Text: "two", ReplyTo: 10}}
			if err = journal.AddPage(msgs, 20); err != nil {
				t.Fatal(err)
			}
			for _, m := range msgs {
				if err = journal.Sent(m.ID, fmt.Sprintf("$%d", m.ID)); err != nil {
					t.Fatal(err)
				}
			}
			if err = journal.Set("phase", "imported"); err != nil {
				t.Fatal(err)
			}
			cfg := Config{Database: path, Container: "fixture", Image: "sha256:fixture", LoginID: "123", Owner: "@owner:test"}
			apply := func(room string) error {
				return cfg.Apply(ctx, journal, kind, 456, "test", room, "test", "telegram_", filepath.Join(dir, "backups"))
			}
			if err = apply("!room:test"); err != nil {
				t.Fatal(err)
			}
			receiver := networkid.UserLoginID("123")
			msgID := networkid.MessageID("20")
			if kind == "channel" {
				receiver = ""
				msgID = "456.20"
			}
			key := networkid.PortalKey{ID: networkid.PortalID(kind + ":456"), Receiver: receiver}
			portal, err := db.Portal.GetByKey(ctx, key)
			if err != nil || portal == nil {
				t.Fatal(portal, err)
			}
			if portal.MXID != "!room:test" {
				t.Fatal(portal.MXID)
			}
			if kind == "user" && (portal.RoomType != database.RoomTypeDM || portal.OtherUserID != "456") {
				t.Fatal("DM mapping missing", portal.RoomType, portal.OtherUserID)
			}
			msg, err := db.Message.GetPartByID(ctx, receiver, msgID, "")
			if err != nil || msg == nil {
				t.Fatal(msg, err)
			}
			if msg.MXID != "$20" || msg.SenderID != "456" || msg.Timestamp.UnixMilli() != msgs[1].Time {
				t.Fatal("bridge cannot read imported message", msg)
			}
			var metadata string
			if err = db.QueryRow(ctx, "SELECT metadata FROM user_login WHERE id='123'").Scan(&metadata); err != nil || metadata != "{\"untouched\":\"login\"}" {
				t.Fatal("login changed", metadata, err)
			}
			// Simulate a crash after the bridge transaction commits but before
			// the importer records completion: reapplying must be idempotent.
			if err = journal.Set("phase", "imported"); err != nil {
				t.Fatal(err)
			}
			if err = apply("!room:test"); err != nil {
				t.Fatal(err)
			}
			var count int
			if err = db.QueryRow(ctx, "SELECT COUNT(*) FROM message").Scan(&count); err != nil || count != 2 {
				t.Fatal(count, err)
			}
			if err = journal.Set("phase", "imported"); err != nil {
				t.Fatal(err)
			}
			if err = apply("!different:test"); err == nil {
				t.Fatal("accepted a conflicting room")
			}
		})
	}
}
