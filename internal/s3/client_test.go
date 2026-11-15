package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
)

func TestCompleteRejectsHTTP200EmbeddedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `<Error><Code>InvalidPart</Code></Error>`) }))
	defer srv.Close()
	c := Client{Endpoint: srv.URL, Bucket: "bucket", Region: "test", Access: "access", Secret: "secret"}
	_, err := c.Complete(context.Background(), "key", "upload", nil)
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Code != "InvalidPart" {
		t.Fatal(err)
	}
}

func TestVersionCleanupIsExactKeyAndTraversesPages(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("prefix") != "media/key" {
			t.Error("wrong cleanup prefix")
		}
		if calls == 1 {
			fmt.Fprint(w, `<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>media/key</NextKeyMarker><NextVersionIdMarker>v1</NextVersionIdMarker><Version><Key>media/key-neighbor</Key><VersionId>never-delete</VersionId></Version><Version><Key>media/key</Key><VersionId>v1</VersionId></Version></ListVersionsResult>`)
		} else {
			if r.URL.Query().Get("version-id-marker") != "v1" {
				t.Error("missing page cursor")
			}
			fmt.Fprint(w, `<ListVersionsResult><IsTruncated>false</IsTruncated><DeleteMarker><Key>media/key</Key><VersionId>marker</VersionId></DeleteMarker></ListVersionsResult>`)
		}
	}))
	defer srv.Close()
	c := Client{Endpoint: srv.URL, Bucket: "bucket", Region: "test", Access: "access", Secret: "secret"}
	got, err := c.Versions(context.Background(), "media/key")
	if err != nil || fmt.Sprint(got) != "[v1 marker]" || calls != 2 {
		t.Fatal(got, err, calls)
	}
}

func TestExpiredUploadIsDistinctFromGeneralStorageFailure(t *testing.T) {
	code := "NoSuchUpload"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		fmt.Fprintf(w, "<Error><Code>%s</Code></Error>", code)
	}))
	defer srv.Close()
	c := Client{Endpoint: srv.URL, Bucket: "bucket", Region: "test", Access: "access", Secret: "secret"}
	_, err := c.Parts(context.Background(), "key", "upload")
	if !errors.Is(err, importer.ErrUploadGone) {
		t.Fatal(err)
	}
	code = "NoSuchBucket"
	_, err = c.Parts(context.Background(), "key", "upload")
	if errors.Is(err, importer.ErrUploadGone) {
		t.Fatal("missing bucket mistaken for expired upload")
	}
}
