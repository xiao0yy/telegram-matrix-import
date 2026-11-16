// Package s3 provides a bounded, serial multipart transport. It never buffers a
// complete attachment and never lists or deletes outside an exact media key.
package s3

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xiao0yy/telegram-matrix-import/internal/importer"
	"github.com/xiao0yy/telegram-matrix-import/internal/retry"
)

type Client struct {
	Endpoint string       `json:"endpoint"`
	Bucket   string       `json:"bucket"`
	Region   string       `json:"region"`
	Access   string       `json:"access_key"`
	Secret   string       `json:"secret_key"`
	HTTP     *http.Client `json:"-"`
}
type Error struct {
	Status int
	Code   string
	Delay  time.Duration
}

func (e *Error) Error() string             { return fmt.Sprintf("S3 HTTP %d (%s)", e.Status, e.Code) }
func (e *Error) RetryAfter() time.Duration { return e.Delay }
func (e *Error) Retryable() bool {
	return retry.HTTPStatus(e.Status) || e.Code == "RequestTimeout" || e.Code == "SlowDown" || e.Code == "InternalError" || e.Code == "ServiceUnavailable"
}
func absent(err error) bool  { var e *Error; return errors.As(err, &e) && e.Status == 404 }
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func mac(k []byte, s string) []byte {
	h := hmac.New(sha256.New, k)
	h.Write([]byte(s))
	return h.Sum(nil)
}

func (c *Client) request(ctx context.Context, method, key string, q url.Values, body []byte, headers map[string]string) ([]byte, http.Header, error) {
	base, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, nil, err
	}
	ip := net.ParseIP(base.Hostname())
	if base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Scheme != "https" && !(base.Scheme == "http" && ip != nil && ip.IsLoopback())) {
		return nil, nil, errors.New("invalid S3 endpoint")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/" + c.Bucket + "/" + key
	base.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	req, err := http.NewRequestWithContext(ctx, method, base.String(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	day := stamp[:8]
	hash := digest(body)
	values := map[string]string{"host": base.Host, "x-amz-date": stamp, "x-amz-content-sha256": hash}
	for k, v := range headers {
		values[strings.ToLower(k)] = strings.TrimSpace(v)
	}
	names := make([]string, 0, len(values))
	for k := range values {
		names = append(names, k)
	}
	sort.Strings(names)
	canonical := ""
	for _, k := range names {
		canonical += k + ":" + values[k] + "\n"
		if k != "host" {
			req.Header.Set(k, values[k])
		}
	}
	signed := strings.Join(names, ";")
	scope := day + "/" + c.Region + "/s3/aws4_request"
	raw := method + "\n" + base.EscapedPath() + "\n" + base.RawQuery + "\n" + canonical + "\n" + signed + "\n" + hash
	keyBytes := mac(mac(mac(mac([]byte("AWS4"+c.Secret), day), c.Region), "s3"), "aws4_request")
	sig := hex.EncodeToString(mac(keyBytes, "AWS4-HMAC-SHA256\n"+stamp+"\n"+scope+"\n"+digest([]byte(raw))))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.Access+"/"+scope+", SignedHeaders="+signed+", Signature="+sig)
	req.Header.Set("Accept-Encoding", "identity")
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("S3 transport: %w", err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil {
		return nil, nil, err
	}
	if len(b) > 2<<20 {
		return nil, nil, errors.New("S3 response exceeds metadata limit")
	}
	var apiError struct {
		XMLName xml.Name
		Code    string
	}
	_ = xml.Unmarshal(b, &apiError)
	if res.StatusCode < 200 || res.StatusCode >= 300 || apiError.XMLName.Local == "Error" {
		return nil, nil, &Error{Status: res.StatusCode, Code: apiError.Code, Delay: retry.RetryAfter(res.Header.Get("Retry-After"))}
	}
	return b, res.Header, nil
}
func (c *Client) Head(ctx context.Context, key string) (importer.Object, error) {
	_, h, e := c.request(ctx, "HEAD", key, nil, nil, nil)
	if e != nil {
		return importer.Object{}, e
	}
	n, e := strconv.ParseInt(h.Get("Content-Length"), 10, 64)
	return importer.Object{Size: n, Version: h.Get("X-Amz-Version-Id"), ETag: h.Get("ETag"), ImportID: h.Get("X-Amz-Meta-Import-Id")}, e
}
func (c *Client) Start(ctx context.Context, key, identity string) (string, error) {
	b, _, e := c.request(ctx, "POST", key, url.Values{"uploads": {""}}, nil, map[string]string{"Content-Type": "application/octet-stream", "X-Amz-Meta-Import-Id": identity})
	if e != nil {
		return "", e
	}
	var r struct {
		Upload string `xml:"UploadId"`
	}
	e = xml.Unmarshal(b, &r)
	if e == nil && r.Upload == "" {
		e = errors.New("S3 returned no upload ID")
	}
	return r.Upload, e
}
func (c *Client) PutPart(ctx context.Context, key, upload string, n int, b []byte) (string, error) {
	_, h, e := c.request(ctx, "PUT", key, url.Values{"uploadId": {upload}, "partNumber": {strconv.Itoa(n)}}, b, nil)
	if e == nil && h.Get("ETag") == "" {
		e = errors.New("S3 returned no part ETag")
	}
	return h.Get("ETag"), e
}
func (c *Client) Complete(ctx context.Context, key, upload string, parts []importer.Part) (string, error) {
	type part struct {
		Number int    `xml:"PartNumber"`
		ETag   string `xml:"ETag"`
	}
	body := struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []part   `xml:"Part"`
	}{Parts: make([]part, len(parts))}
	for i, p := range parts {
		body.Parts[i] = part{p.Number, p.ETag}
	}
	b, e := xml.Marshal(body)
	if e != nil {
		return "", e
	}
	_, h, e := c.request(ctx, "POST", key, url.Values{"uploadId": {upload}}, b, nil)
	return h.Get("X-Amz-Version-Id"), e
}
func (c *Client) Abort(ctx context.Context, key, upload string) error {
	_, _, e := c.request(ctx, "DELETE", key, url.Values{"uploadId": {upload}}, nil, nil)
	if absent(e) {
		return nil
	}
	return e
}
func (c *Client) DeleteVersion(ctx context.Context, key, version string) error {
	if version == "" {
		return errors.New("refusing unversioned delete")
	}
	_, _, e := c.request(ctx, "DELETE", key, url.Values{"versionId": {version}}, nil, nil)
	if absent(e) {
		return nil
	}
	return e
}
func (c *Client) Parts(ctx context.Context, key, upload string) ([]importer.Part, error) {
	var out []importer.Part
	cursor := ""
	for pages := 0; pages < 100; pages++ {
		q := url.Values{"uploadId": {upload}, "max-parts": {"1000"}}
		if cursor != "" {
			q.Set("part-number-marker", cursor)
		}
		b, _, err := c.request(ctx, "GET", key, q, nil, nil)
		if err != nil {
			var apiErr *Error
			if errors.As(err, &apiErr) && apiErr.Code == "NoSuchUpload" {
				return nil, importer.ErrUploadGone
			}
			return nil, err
		}
		var p struct {
			Truncated bool   `xml:"IsTruncated"`
			Next      string `xml:"NextPartNumberMarker"`
			Parts     []struct {
				Number int `xml:"PartNumber"`
				ETag   string
				Size   int64
			} `xml:"Part"`
		}
		if err = xml.Unmarshal(b, &p); err != nil {
			return nil, err
		}
		for _, v := range p.Parts {
			out = append(out, importer.Part{Number: v.Number, ETag: v.ETag, Size: v.Size})
		}
		if !p.Truncated {
			return out, nil
		}
		if p.Next == "" || p.Next == cursor {
			return nil, errors.New("invalid S3 part pagination")
		}
		cursor = p.Next
	}
	return nil, errors.New("S3 part pagination limit exceeded")
}
func (c *Client) Uploads(ctx context.Context, key string) ([]string, error) {
	var out []string
	km, um := "", ""
	for pages := 0; pages < 100; pages++ {
		q := url.Values{"uploads": {""}, "prefix": {key}, "max-uploads": {"1000"}}
		if km != "" {
			q.Set("key-marker", km)
			q.Set("upload-id-marker", um)
		}
		b, _, err := c.request(ctx, "GET", "", q, nil, nil)
		if err != nil {
			return nil, err
		}
		var p struct {
			Truncated bool   `xml:"IsTruncated"`
			Key       string `xml:"NextKeyMarker"`
			Upload    string `xml:"NextUploadIdMarker"`
			Uploads   []struct {
				Key string
				ID  string `xml:"UploadId"`
			} `xml:"Upload"`
		}
		if err = xml.Unmarshal(b, &p); err != nil {
			return nil, err
		}
		for _, v := range p.Uploads {
			if v.Key == key {
				out = append(out, v.ID)
			}
		}
		if !p.Truncated {
			return out, nil
		}
		if p.Key == "" || (p.Key == km && p.Upload == um) {
			return nil, errors.New("invalid S3 upload pagination")
		}
		km, um = p.Key, p.Upload
	}
	return nil, errors.New("S3 upload pagination limit exceeded")
}
func (c *Client) Versions(ctx context.Context, key string) ([]string, error) {
	var out []string
	km, vm := "", ""
	for pages := 0; pages < 100; pages++ {
		q := url.Values{"versions": {""}, "prefix": {key}, "max-keys": {"1000"}}
		if km != "" {
			q.Set("key-marker", km)
			q.Set("version-id-marker", vm)
		}
		b, _, err := c.request(ctx, "GET", "", q, nil, nil)
		if err != nil {
			return nil, err
		}
		type version struct {
			Key string
			ID  string `xml:"VersionId"`
		}
		var p struct {
			Truncated bool      `xml:"IsTruncated"`
			Key       string    `xml:"NextKeyMarker"`
			Version   string    `xml:"NextVersionIdMarker"`
			Versions  []version `xml:"Version"`
			Markers   []version `xml:"DeleteMarker"`
		}
		if err = xml.Unmarshal(b, &p); err != nil {
			return nil, err
		}
		for _, v := range append(p.Versions, p.Markers...) {
			if v.Key == key {
				out = append(out, v.ID)
			}
		}
		if !p.Truncated {
			return out, nil
		}
		if p.Key == "" || (p.Key == km && p.Version == vm) {
			return nil, errors.New("invalid S3 version pagination")
		}
		km, vm = p.Key, p.Version
	}
	return nil, errors.New("S3 version pagination limit exceeded")
}
