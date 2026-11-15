package importer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
)

var ErrUploadGone = errors.New("multipart upload no longer exists")

// SourceUnavailable marks account, connectivity or service failures which
// should pause the import instead of discarding individual attachments.
type SourceUnavailable struct{ Err error }

func (e *SourceUnavailable) Error() string { return e.Err.Error() }
func (e *SourceUnavailable) Unwrap() error { return e.Err }

// Message is a source record, not a Matrix event. IDs are scoped to one peer.
type Message struct {
	ID         int64  `json:"id"`
	Sender     string `json:"sender"`
	SenderName string `json:"sender_name"`
	Time       int64  `json:"time_ms"`
	Text       string `json:"text"`
	ReplyTo    int64  `json:"reply_to,omitempty"`
	File       *File  `json:"file,omitempty"`
}
type File struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	MIME     string          `json:"mime"`
	Size     int64           `json:"size"`
	Location json.RawMessage `json:"location"`
}
type Source interface {
	Page(context.Context, int64, int64, int) ([]Message, error)
	Latest(context.Context) (int64, error)
	Download(context.Context, Message, int64, io.Writer) error
}
type Matrix interface {
	Reserve(context.Context) (string, error)
	Register(context.Context, string, string) (string, error)
	RegistrationKey(string, string) (string, error)
	Prepare(context.Context, Message, map[string]any) (json.RawMessage, error)
	Send(context.Context, Message, string, json.RawMessage) (string, error)
	Delete(context.Context, string) error
}
type Part struct {
	Number int    `json:"number"`
	ETag   string `json:"etag"`
	Size   int64  `json:"size"`
}
type Object struct {
	Size     int64
	Version  string
	ETag     string
	ImportID string
}
type Objects interface {
	Head(context.Context, string) (Object, error)
	Start(context.Context, string, string) (string, error)
	Uploads(context.Context, string) ([]string, error)
	Parts(context.Context, string, string) ([]Part, error)
	PutPart(context.Context, string, string, int, []byte) (string, error)
	Complete(context.Context, string, string, []Part) (string, error)
	Abort(context.Context, string, string) error
	DeleteVersion(context.Context, string, string) error
	Versions(context.Context, string) ([]string, error)
}
type Media struct {
	MXC          string `json:"mxc"`
	Key          string `json:"key"`
	Name         string `json:"name"`
	Registered   bool   `json:"registered"`
	DummyVersion string `json:"dummy_version"`
	FinalVersion string `json:"final_version"`
	Upload       string `json:"upload"`
	Offset       int64  `json:"offset"`
	Parts        []Part `json:"parts"`
	AESKey       []byte `json:"aes_key"`
	IV           []byte `json:"iv"`
	HashState    []byte `json:"hash_state"`
	SHA256       string `json:"sha256"`
	State        string `json:"state"`
	Attempts     int    `json:"attempts"`
	Error        string `json:"error,omitempty"`
	ErrorKind    string `json:"error_kind,omitempty"`
}
type AttachmentError struct{ Err error }

func (e *AttachmentError) Error() string { return e.Err.Error() }
func (e *AttachmentError) Unwrap() error { return e.Err }

type Record struct {
	Message Message
	Media   Media
	Payload json.RawMessage
	EventID string
}
