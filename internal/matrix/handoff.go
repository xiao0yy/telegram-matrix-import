package matrix

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/crypto"
	"maunium.net/go/mautrix/id"
)

// CopyRoomKeys lets the bridge serve historical key requests after this
// importer exits. Only this room's inbound Megolm keys cross the boundary.
// Both stores are opened with their own encryption keys through the SDK.
func (c *Client) CopyRoomKeys(ctx context.Context, path, account, pickle string) error {
	if c.Room == "" || pickle == "" {
		return errors.New("room and bridge crypto pickle key are required")
	}
	sourcePath := filepath.Join(c.stateDir, "crypto.db")
	if _, err := os.Stat(sourcePath); err != nil {
		return err
	}
	if _, err := os.Stat(path); err != nil {
		return err
	}
	key, err := os.ReadFile(filepath.Join(c.stateDir, "crypto.key"))
	if err != nil {
		return err
	}
	fromDB, err := dbutil.NewWithDialect(sourcePath, "sqlite3")
	if err != nil {
		return err
	}
	defer fromDB.Close()
	toDB, err := dbutil.NewWithDialect(path, "sqlite3")
	if err != nil {
		return err
	}
	defer toDB.Close()
	from := crypto.NewSQLCryptoStore(fromDB, nil, "telegram-history-importer", "", key)
	to := crypto.NewSQLCryptoStore(toDB, nil, account, "", []byte(pickle))
	bridgeAccount, err := to.GetAccount(ctx)
	if err != nil {
		return err
	}
	if bridgeAccount == nil {
		return errors.New("bridge encryption account not found")
	}
	return from.GetGroupSessionsForRoom(ctx, id.RoomID(c.Room)).Iter(func(s *crypto.InboundGroupSession) (bool, error) {
		if s.Internal == nil {
			return true, nil
		}
		if s.RoomID != id.RoomID(c.Room) {
			return false, errors.New("unexpected room in key export")
		}
		return true, to.PutGroupSession(ctx, s)
	})
}
