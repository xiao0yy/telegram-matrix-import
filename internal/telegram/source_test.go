package telegram

import (
	"context"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

type historyRPC struct {
	t        *testing.T
	requests int
}

func (h *historyRPC) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	r := in.(*tg.MessagesGetHistoryRequest)
	if _, ok := r.Peer.(*tg.InputPeerUser); !ok {
		h.t.Fatal("DM is not a user peer")
	}
	if r.OffsetID != 1 || r.AddOffset != -50 || r.MaxID != 31 || r.MinID != 0 {
		h.t.Fatal("incorrect oldest-first bounds", r.OffsetID, r.AddOffset, r.MaxID, r.MinID)
	}
	res := &tg.MessagesMessages{Messages: []tg.MessageClass{
		&tg.Message{ID: 30, Date: 1600000003, PeerID: &tg.PeerUser{UserID: 456}, Message: "incoming"},
		&tg.Message{ID: 20, Out: true, Date: 1600000002, PeerID: &tg.PeerUser{UserID: 456}, Message: "outgoing"},
		&tg.Message{ID: 10, Date: 1600000001, PeerID: &tg.PeerUser{UserID: 456}, FromID: &tg.PeerUser{UserID: 456}, Message: "explicit"},
	}, Users: []tg.UserClass{&tg.User{ID: 123, FirstName: "Owner"}, &tg.User{ID: 456, FirstName: "Bot"}}}
	b := &bin.Buffer{}
	if err := res.Encode(b); err != nil {
		return err
	}
	h.requests++
	return out.Decode(b)
}
func TestDMHistoryOrderAndSender(t *testing.T) {
	h := &historyRPC{t: t}
	s := Source{API: tg.NewClient(h), SelfID: 123, Peer: Peer{Kind: "user", ID: 456, AccessHash: 789}}
	page, err := s.Page(context.Background(), 0, 30, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 3 || page[0].ID != 10 || page[1].Sender != "123" || page[2].Sender != "456" || page[2].SenderName != "Bot" {
		t.Fatal(page)
	}
}
