package telegram

import (
	"context"
	"os"
	"path/filepath"

	"github.com/gotd/td/session"
)

// AtomicStorage preserves the last usable Telegram session if the process
// exits or the host restarts during a write.
type AtomicStorage struct{ Path string }

func (s AtomicStorage) LoadSession(ctx context.Context) ([]byte, error) {
	b, err := os.ReadFile(s.Path)
	if os.IsNotExist(err) {
		return nil, session.ErrNotFound
	}
	return b, err
}
func (s AtomicStorage) StoreSession(ctx context.Context, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(s.Path), ".session-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), s.Path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(s.Path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
