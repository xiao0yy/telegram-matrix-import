package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader/fileref"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
)

// Peer values come from dialogs resolved by this Telegram session. Access
// hashes must never be copied from a different account or session.
type Peer struct {
	Kind       string `json:"kind"`
	ID         int64  `json:"id"`
	AccessHash int64  `json:"access_hash"`
	Title      string `json:"title"`
	Username   string `json:"username,omitempty"`
}

func (p Peer) Input() (tg.InputPeerClass, error) {
	switch p.Kind {
	case "channel":
		return &tg.InputPeerChannel{ChannelID: p.ID, AccessHash: p.AccessHash}, nil
	case "user":
		return &tg.InputPeerUser{UserID: p.ID, AccessHash: p.AccessHash}, nil
	case "chat":
		return &tg.InputPeerChat{ChatID: p.ID}, nil
	default:
		return nil, errors.New("supported peers are users, basic groups and non-forum supergroups")
	}
}

type Source struct {
	Client  *telegram.Client
	Peer    Peer
	SelfID  int64
	API     *tg.Client
	Report  func(map[string]any)
	Journal *importer.Journal
}

func (s *Source) api() *tg.Client {
	if s.API != nil {
		return s.API
	}
	return s.Client.API()
}

// VisitConversations resolves access hashes using this login and paginates every
// dialog. The callback may return an error to stop iteration early.
func VisitConversations(ctx context.Context, c *telegram.Client, fn func(Peer) error) error {
	it := dialogs.NewQueryBuilder(c.API()).GetDialogs().BatchSize(100).Iter()
	for it.Next(ctx) {
		v := it.Value()
		switch p := v.Peer.(type) {
		case *tg.InputPeerUser:
			u, ok := v.Entities.User(p.UserID)
			if !ok {
				continue
			}
			name := strings.TrimSpace(u.FirstName + " " + u.LastName)
			if u.Username != "" {
				name += " (@" + u.Username + ")"
			}
			if err := fn(Peer{Kind: "user", ID: p.UserID, AccessHash: p.AccessHash, Title: name}); err != nil {
				return err
			}
		case *tg.InputPeerChannel:
			ch, ok := v.Entities.Channel(p.ChannelID)
			if !ok || !ch.Megagroup {
				continue
			}
			if ch.Forum {
				continue
			}
			if err := fn(Peer{Kind: "channel", ID: p.ChannelID, AccessHash: p.AccessHash, Title: ch.Title}); err != nil {
				return err
			}
		case *tg.InputPeerChat:
			ch, ok := v.Entities.Chat(p.ChatID)
			if !ok {
				continue
			}
			if err := fn(Peer{Kind: "chat", ID: p.ChatID, Title: ch.Title}); err != nil {
				return err
			}
		}
	}
	return it.Err()
}
func Resolve(ctx context.Context, c *telegram.Client, wanted Peer) (Peer, error) {
	if wanted.Username != "" {
		r, err := c.API().ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: strings.TrimPrefix(wanted.Username, "@")})
		if err != nil {
			return Peer{}, err
		}
		if p, ok := r.Peer.(*tg.PeerUser); ok {
			for _, v := range r.Users {
				if u, ok := v.(*tg.User); ok && u.ID == p.UserID {
					if wanted.ID != 0 && wanted.ID != u.ID {
						return Peer{}, errors.New("username now belongs to another Telegram account")
					}
					return Peer{Kind: "user", ID: u.ID, AccessHash: u.AccessHash, Title: strings.TrimSpace(u.FirstName + " " + u.LastName), Username: u.Username}, nil
				}
			}
		}
		return Peer{}, errors.New("username is not a supported individual conversation")
	}
	var result Peer
	found := errors.New("found peer")
	err := VisitConversations(ctx, c, func(p Peer) error {
		if p.Kind == wanted.Kind && p.ID == wanted.ID {
			result = p
			return found
		}
		return nil
	})
	if errors.Is(err, found) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	return result, errors.New("conversation not available in this login, or unsupported forum/channel")
}

type location struct {
	Kind       string `json:"kind"`
	ID         int64  `json:"id"`
	AccessHash int64  `json:"access_hash"`
	Reference  []byte `json:"reference"`
	Thumb      string `json:"thumb"`
	DC         int    `json:"dc"`
}

func (l location) Input() tg.InputFileLocationClass {
	if l.Kind == "photo" {
		return &tg.InputPhotoFileLocation{ID: l.ID, AccessHash: l.AccessHash, FileReference: l.Reference, ThumbSize: l.Thumb}
	}
	return &tg.InputDocumentFileLocation{ID: l.ID, AccessHash: l.AccessHash, FileReference: l.Reference}
}
func messages(r tg.MessagesMessagesClass) ([]tg.MessageClass, []tg.UserClass, []tg.ChatClass, error) {
	switch v := r.(type) {
	case *tg.MessagesMessages:
		return v.Messages, v.Users, v.Chats, nil
	case *tg.MessagesMessagesSlice:
		return v.Messages, v.Users, v.Chats, nil
	case *tg.MessagesChannelMessages:
		return v.Messages, v.Users, v.Chats, nil
	default:
		return nil, nil, nil, fmt.Errorf("unexpected history response %T", r)
	}
}
func (s *Source) Latest(ctx context.Context) (int64, error) {
	p, e := s.Peer.Input()
	if e != nil {
		return 0, e
	}
	r, e := s.api().MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: p, Limit: 1})
	if e != nil {
		return 0, e
	}
	ms, _, _, e := messages(r)
	if e != nil || len(ms) == 0 {
		return 0, e
	}
	return int64(ms[0].GetID()), nil
}
func (s *Source) Page(ctx context.Context, after, through int64, limit int) ([]importer.Message, error) {
	p, e := s.Peer.Input()
	if e != nil {
		return nil, e
	}
	for {
		r, e := s.api().MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: p, OffsetID: int(after + 1), AddOffset: -limit, Limit: limit, MinID: int(after), MaxID: int(through + 1)})
		if e != nil {
			return nil, e
		}
		ms, users, chats, e := messages(r)
		if e != nil {
			return nil, e
		}
		names := map[string]string{}
		for _, u := range users {
			if u, ok := u.(*tg.User); ok {
				names[strconv.FormatInt(u.ID, 10)] = strings.TrimSpace(u.FirstName + " " + u.LastName)
			}
		}
		for _, c := range chats {
			if c, ok := c.(*tg.Channel); ok {
				names["channel-"+strconv.FormatInt(c.ID, 10)] = c.Title
			}
		}
		var out []importer.Message
		for _, m := range ms {
			if int64(m.GetID()) <= after || int64(m.GetID()) > through {
				continue
			}
			msg := importer.Message{ID: int64(m.GetID())}
			switch v := m.(type) {
			case *tg.Message:
				msg.Time = int64(v.Date) * 1000
				msg.Text = v.Message
				switch peer := v.FromID.(type) {
				case *tg.PeerUser:
					msg.Sender = strconv.FormatInt(peer.UserID, 10)
				case *tg.PeerChannel:
					msg.Sender = "channel-" + strconv.FormatInt(peer.ChannelID, 10)
				}
				if reply, ok := v.ReplyTo.(*tg.MessageReplyHeader); ok {
					msg.ReplyTo = int64(reply.ReplyToMsgID)
				}
				switch media := v.Media.(type) {
				case *tg.MessageMediaDocument:
					if d, ok := media.Document.(*tg.Document); ok {
						name := fmt.Sprintf("document-%d", d.ID)
						for _, a := range d.Attributes {
							if a, ok := a.(*tg.DocumentAttributeFilename); ok {
								name = a.FileName
							}
						}
						l, _ := json.Marshal(location{Kind: "document", ID: d.ID, AccessHash: d.AccessHash, Reference: d.FileReference, DC: d.DCID})
						msg.File = &importer.File{ID: fmt.Sprint(d.ID), Name: name, MIME: d.MimeType, Size: d.Size, Location: l}
					} else {
						msg.Text += "\n[附件已无法从 Telegram 获取]"
					}
				case *tg.MessageMediaPhoto:
					if photo, ok := media.Photo.(*tg.Photo); ok {
						var size int64
						var thumb string
						for _, sz := range photo.Sizes {
							switch sz := sz.(type) {
							case *tg.PhotoSize:
								if int64(sz.Size) > size {
									size = int64(sz.Size)
									thumb = sz.Type
								}
							case *tg.PhotoSizeProgressive:
								for _, n := range sz.Sizes {
									if int64(n) > size {
										size = int64(n)
										thumb = sz.Type
									}
								}
							}
						}
						l, _ := json.Marshal(location{Kind: "photo", ID: photo.ID, AccessHash: photo.AccessHash, Reference: photo.FileReference, Thumb: thumb, DC: photo.DCID})
						msg.File = &importer.File{ID: fmt.Sprint(photo.ID), Name: fmt.Sprintf("photo-%d.jpg", photo.ID), MIME: "image/jpeg", Size: size, Location: l}
					} else {
						msg.Text += "\n[照片已无法从 Telegram 获取]"
					}
				case nil, *tg.MessageMediaEmpty, *tg.MessageMediaWebPage:
				default:
					msg.Text += "\n[此消息类型暂不支持导入，原内容请在 Telegram 查看]"
				}
			case *tg.MessageService:
				msg.Time = int64(v.Date) * 1000
				msg.Text = "[Telegram 系统消息，详细内容请在 Telegram 查看]"
			default:
				continue
			}
			if msg.Sender == "" && s.Peer.Kind == "user" {
				msg.Sender = strconv.FormatInt(s.Peer.ID, 10)
				if v, ok := m.(*tg.Message); ok && v.Out {
					msg.Sender = strconv.FormatInt(s.SelfID, 10)
				}
			}
			if msg.Sender == "" {
				msg.Sender = "channel-" + strconv.FormatInt(s.Peer.ID, 10)
			}
			msg.SenderName = names[msg.Sender]
			if msg.SenderName == "" {
				msg.SenderName = msg.Sender
			}
			out = append(out, msg)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		if len(out) == 0 && len(ms) > 0 {
			cursor := after
			for _, m := range ms {
				if int64(m.GetID()) > cursor && int64(m.GetID()) <= through {
					cursor = int64(m.GetID())
				}
			}
			if cursor > after && cursor < through {
				after = cursor
				continue
			}
		}
		return out, nil
	}
}
func (s *Source) Download(ctx context.Context, m importer.Message, offset int64, w io.Writer) error {
	var l location
	if e := json.Unmarshal(m.File.Location, &l); e != nil {
		return e
	}
	p, e := s.Peer.Input()
	if e != nil {
		return e
	}
	connect, cancel := context.WithTimeout(ctx, requestTimeout)
	pool, e := s.Client.DC(connect, l.DC, 1)
	cancel()
	if e != nil {
		return &importer.SourceUnavailable{Err: e}
	}
	defer pool.Close()
	// The downloader refreshes expired file references from the original
	// message and rejects a replacement with a different Telegram file ID.
	report := func(v map[string]any) {
		v["message_id"] = m.ID
		if s.Report != nil {
			s.Report(v)
		}
	}
	downloadCtx, rpc, cancelDownload := downloadRequests(ctx, pool, requestTimeout, report)
	defer cancelDownload()
	b := s.Client.DownloadVia(rpc, l.Input()).WithOffset(offset).WithSize(m.File.Size).WithThreads(1).WithRefresh(fileref.Message(s.Client.API(), p, int(m.ID), l.Input()))
	_, e = b.Stream(downloadCtx, w)
	if e != nil && context.Cause(downloadCtx) != nil {
		e = context.Cause(downloadCtx)
	}
	if e == nil || errors.Is(e, fileref.ErrNotFound) || tgerr.Is(e, "FILE_REFERENCE_EXPIRED", "FILE_REFERENCE_INVALID", "LOCATION_INVALID", "FILE_ID_INVALID", "MEDIA_EMPTY", "DOCUMENT_INVALID") {
		return e
	}
	return downloadFailure(e, func() error { return s.probeDownload(ctx, m, l) })
}

// A server TIMEOUT can describe one broken Telegram file or an unavailable
// storage DC. Only treat it as a file failure when a different, previously
// completed file on the same DC and of the same kind is still downloadable.
func downloadFailure(err error, probe func() error) error {
	var timeout *requestTimeoutError
	if errors.As(err, &timeout) && timeout.Server && timeout.Request == "*tg.UploadGetFileRequest" {
		if probe() == nil {
			return &importer.AttachmentError{Err: err}
		}
	}
	return &importer.SourceUnavailable{Err: err}
}

func (s *Source) probeDownload(ctx context.Context, failed importer.Message, storage location) error {
	if s.Journal == nil {
		return errors.New("no completed attachment available for source health probe")
	}
	known, err := s.Journal.ReadyFileBefore(failed.ID, func(f *importer.File) bool {
		var l location
		return f.ID != failed.File.ID && json.Unmarshal(f.Location, &l) == nil && l.DC == storage.DC && l.Kind == storage.Kind
	})
	if err != nil {
		return err
	}
	if known == nil {
		return errors.New("no completed attachment on the affected Telegram storage DC")
	}
	// Refresh only this file's reference. Never substitute a new/edited file.
	page, err := s.Page(ctx, known.ID-1, known.ID, 1)
	if err != nil {
		return err
	}
	if len(page) != 1 || page[0].ID != known.ID || page[0].File == nil || page[0].File.ID != known.File.ID {
		return errors.New("source health probe file is no longer available")
	}
	var l location
	if err = json.Unmarshal(page[0].File.Location, &l); err != nil {
		return err
	}
	if l.DC != storage.DC || l.Kind != storage.Kind {
		return errors.New("source health probe changed storage location")
	}
	connect, cancel := context.WithTimeout(ctx, requestTimeout)
	pool, err := s.Client.DC(connect, l.DC, 1)
	cancel()
	if err != nil {
		return err
	}
	defer pool.Close()
	// Four KiB is enough to establish that this download path works; no full
	// attachment is downloaded, uploaded, stored or inserted into the timeline.
	file, err := tg.NewClient(WithRequestPolicy(pool, s.Report)).UploadGetFile(ctx, &tg.UploadGetFileRequest{Location: l.Input(), Limit: 4096})
	if err != nil {
		return err
	}
	data, ok := file.(*tg.UploadFile)
	if !ok || len(data.Bytes) == 0 {
		return errors.New("source health probe returned no file bytes")
	}
	if s.Report != nil {
		s.Report(map[string]any{"event": "telegram_download_probe", "state": "healthy", "message_id": failed.ID, "bytes": len(data.Bytes)})
	}
	return nil
}
