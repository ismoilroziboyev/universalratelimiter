// Package nethttp provides rate limiting for net/http and compatible routers.
package nethttp

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	"github.com/ismoilroziboyev/universalratelimiter/internal/httpmiddleware"
	"github.com/ismoilroziboyev/universalratelimiter/middleware"
)

// Request provides the original HTTP request and its response writer.
// Custom response callbacks can write to Writer and return nil.
type Request struct {
	*http.Request
	Writer http.ResponseWriter
}

type Config = middleware.Config[Request]
type Policy = middleware.Policy[Request]

// New builds standard HTTP middleware. The default key is the remote IP.
func New(config Config) (func(http.Handler) http.Handler, error) {
	handler, err := httpmiddleware.New(config, transport)
	if err != nil {
		return nil, err
	}
	return wrap(handler), nil
}

// NewMulti builds middleware with sequential or atomic policy evaluation.
func NewMulti(mode ratelimiter.ExecutionMode, policies ...Policy) (func(http.Handler) http.Handler, error) {
	handler, err := httpmiddleware.NewMulti(mode, policies, transport)
	if err != nil {
		return nil, err
	}
	return wrap(handler), nil
}

func wrap(handler httpmiddleware.Handler[Request]) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = handler(Request{Request: r, Writer: w}, func() error {
				next.ServeHTTP(w, r)
				return nil
			})
		})
	}
}

var transport = httpmiddleware.Transport[Request]{
	Context: func(r Request) context.Context { return r.Context() },
	Key:     ByIP(),
	Header:  func(r Request, key, value string) { r.Writer.Header().Set(key, value) },
	JSON: func(r Request, status int, body any) error {
		r.Writer.Header().Set("Content-Type", "application/json")
		r.Writer.WriteHeader(status)
		return json.NewEncoder(r.Writer).Encode(body)
	},
}

// Global returns a fixed key shared by every request.
func Global(name string) func(Request) (string, error) { return httpmiddleware.Global[Request](name) }

// ByIP uses the transport peer rather than forwarding headers.
func ByIP() func(Request) (string, error) {
	return func(r Request) (string, error) { return httpmiddleware.IP(r.RemoteAddr) }
}

// ByRoute uses the matched ServeMux pattern. Register the middleware on a
// route so routing has already populated the pattern.
func ByRoute() func(Request) (string, error) {
	return func(r Request) (string, error) {
		pattern := r.Pattern
		if _, path, found := strings.Cut(pattern, " "); found {
			pattern = strings.TrimSpace(path)
		}
		return httpmiddleware.Route(r.Method, pattern)
	}
}

// ByMethod groups requests by standard HTTP method.
func ByMethod() func(Request) (string, error) {
	return func(r Request) (string, error) { return httpmiddleware.Method(r.Method) }
}
