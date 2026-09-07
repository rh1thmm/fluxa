// Package httpcap implements Fluxa's generic durable HTTP capability.
package httpcap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

const FingerprintVersion = 1

type Request struct {
	Method, URL, IdempotencyHeader string
	Headers                        map[string]string
	Body                           []byte
	UseIdempotency                 bool
}
type Response struct {
	Status  int         `json:"status"`
	Body    string      `json:"body"`
	Headers http.Header `json:"headers"`
}
type Descriptor struct {
	FingerprintVersion      int    `json:"fingerprint_version"`
	RuntimeAPIVersion       int    `json:"runtime_api_version"`
	Capability              string `json:"capability"`
	TaskInstanceKey         string `json:"task_instance_key"`
	Ordinal                 int    `json:"ordinal"`
	Method, URL, BodySHA256 string
	Headers                 [][2]string `json:"headers"`
	IdempotencyHeader       string      `json:"idempotency_header,omitempty"`
}
type ErrorKind string

const (
	ErrorTransport ErrorKind = "transport"
	ErrorTimeout   ErrorKind = "timeout"
	ErrorBuild     ErrorKind = "build"
	ErrorRead      ErrorKind = "read"
)

type DispatchError struct {
	Kind      ErrorKind
	Ambiguous bool
	Err       error
}

func (e *DispatchError) Error() string { return e.Err.Error() }

func Canonicalize(r Request, taskKey string, ordinal int) (Descriptor, string, error) {
	u, err := url.Parse(r.URL)
	if err != nil {
		return Descriptor{}, "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return Descriptor{}, "", fmt.Errorf("HTTP URL must be absolute")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	if (u.Scheme == "http" && strings.HasSuffix(u.Host, ":80")) || (u.Scheme == "https" && strings.HasSuffix(u.Host, ":443")) {
		u.Host = strings.Split(u.Host, ":")[0]
	}
	q := u.Query()
	u.RawQuery = q.Encode()
	u.Fragment = ""
	headers := canonicalHeaders(r.Headers)
	body := r.Body
	if strings.EqualFold(header(r.Headers, "content-type"), "application/json") {
		if normalized, ok := canonicalJSON(r.Body); ok {
			body = normalized
		}
	}
	d := Descriptor{FingerprintVersion: FingerprintVersion, RuntimeAPIVersion: 1, Capability: "http", TaskInstanceKey: taskKey, Ordinal: ordinal, Method: strings.ToUpper(r.Method), URL: u.String(), Headers: headers, BodySHA256: digest(body)}
	if r.UseIdempotency {
		d.IdempotencyHeader = strings.ToLower(r.IdempotencyHeader)
	}
	b, err := json.Marshal(d)
	if err != nil {
		return Descriptor{}, "", err
	}
	return d, digest(b), nil
}
func canonicalHeaders(in map[string]string) [][2]string {
	type h struct{ k, v string }
	var hs []h
	for k, v := range in {
		key := strings.ToLower(strings.TrimSpace(k))
		value := strings.TrimSpace(v)
		if isSensitive(key) {
			value = "sha256:" + digest([]byte(value))
		}
		hs = append(hs, h{key, value})
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].k < hs[j].k || hs[i].k == hs[j].k && hs[i].v < hs[j].v })
	out := make([][2]string, len(hs))
	for i, h := range hs {
		out[i] = [2]string{h.k, h.v}
	}
	return out
}
func header(headers map[string]string, key string) string {
	for k, v := range headers {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}
func isSensitive(key string) bool {
	return key == "authorization" || key == "proxy-authorization" || key == "cookie" || key == "set-cookie" || strings.Contains(key, "token") || strings.Contains(key, "secret") || strings.Contains(key, "api-key")
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func canonicalJSON(raw []byte) ([]byte, bool) {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return nil, false
	}
	b, err := json.Marshal(normalize(v))
	return b, err == nil
}
func normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(x))
		for _, k := range keys {
			out[k] = normalize(x[k])
		}
		return out
	case []any:
		for i := range x {
			x[i] = normalize(x[i])
		}
		return x
	default:
		return v
	}
}

func Dispatch(ctx context.Context, client *http.Client, r Request, key string) (Response, *DispatchError) {
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(r.Method), r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return Response{}, &DispatchError{Kind: ErrorBuild, Err: err}
	}
	for k, v := range r.Headers {
		req.Header.Set(k, v)
	}
	if r.UseIdempotency {
		req.Header.Set(r.IdempotencyHeader, key)
	}
	resp, err := client.Do(req)
	if err != nil {
		kind := ErrorTransport
		if ctx.Err() != nil {
			kind = ErrorTimeout
		}
		ambiguous := unsafeMethod(r.Method)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			kind = ErrorTimeout
		}
		return Response{}, &DispatchError{Kind: kind, Ambiguous: ambiguous, Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return Response{}, &DispatchError{Kind: ErrorRead, Ambiguous: unsafeMethod(r.Method), Err: err}
	}
	return Response{Status: resp.StatusCode, Body: string(body), Headers: resp.Header}, nil
}
func unsafeMethod(method string) bool {
	m := strings.ToUpper(method)
	return m != "GET" && m != "HEAD" && m != "OPTIONS"
}
