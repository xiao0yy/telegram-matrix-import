package bridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
)

// LoginSession exposes only the existing Telegram session to a local importer
// while the bridge is stopped. No update cursors or login state are modified.
type LoginSession struct {
	AuthKey []byte `json:"auth_key"`
	DC      int    `json:"dc_id"`
	Addr    string `json:"server_address"`
	Salt    int64  `json:"salt"`
}

func (c Config) Session(ctx context.Context) (LoginSession, error) {
	var out LoginSession
	if err := c.Stopped(ctx); err != nil {
		return out, err
	}
	u := url.URL{Scheme: "file", Path: c.Database}
	db, err := sql.Open("sqlite3", u.String()+"?mode=ro")
	if err != nil {
		return out, err
	}
	defer db.Close()
	var raw []byte
	var owner string
	if err = db.QueryRowContext(ctx, "SELECT user_mxid,metadata FROM user_login WHERE bridge_id=? AND id=?", c.BridgeID, c.LoginID).Scan(&owner, &raw); err != nil {
		return out, err
	}
	if owner != c.Owner {
		return out, errors.New("bridge login owner mismatch")
	}
	var meta struct {
		Session LoginSession `json:"session"`
	}
	if err = json.Unmarshal(raw, &meta); err != nil {
		return out, err
	}
	out = meta.Session
	if len(out.AuthKey) != 256 || out.DC < 1 {
		return LoginSession{}, errors.New("bridge has no usable Telegram login")
	}
	return out, nil
}
