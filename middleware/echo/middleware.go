// Package echo provides rate limiting for Echo v5.
package echo

import (
	"context"

	framework "github.com/labstack/echo/v5"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	"github.com/ismoilroziboyev/universalratelimiter/internal/httpmiddleware"
	"github.com/ismoilroziboyev/universalratelimiter/middleware"
)

type Config = middleware.Config[*framework.Context]
type Policy = middleware.Policy[*framework.Context]

// New builds Echo middleware. The default key uses Echo's client IP.
func New(config Config) (framework.MiddlewareFunc, error) {
	handler, err := httpmiddleware.New(config, transport)
	if err != nil {
		return nil, err
	}
	return wrap(handler), nil
}

// NewMulti builds middleware with sequential or atomic policy evaluation.
func NewMulti(mode ratelimiter.ExecutionMode, policies ...Policy) (framework.MiddlewareFunc, error) {
	handler, err := httpmiddleware.NewMulti(mode, policies, transport)
	if err != nil {
		return nil, err
	}
	return wrap(handler), nil
}

func wrap(handler httpmiddleware.Handler[*framework.Context]) framework.MiddlewareFunc {
	return func(next framework.HandlerFunc) framework.HandlerFunc {
		return func(c *framework.Context) error {
			return handler(c, func() error { return next(c) })
		}
	}
}

var transport = httpmiddleware.Transport[*framework.Context]{
	Context: func(c *framework.Context) context.Context { return c.Request().Context() },
	Key:     ByIP(),
	Header:  func(c *framework.Context, key, value string) { c.Response().Header().Set(key, value) },
	JSON:    func(c *framework.Context, status int, body any) error { return c.JSON(status, body) },
}

// Global returns a fixed key shared by every request.
func Global(name string) func(*framework.Context) (string, error) {
	return httpmiddleware.Global[*framework.Context](name)
}

// ByIP uses Echo's configured IP extractor, or the remote IP by default.
func ByIP() func(*framework.Context) (string, error) {
	return func(c *framework.Context) (string, error) { return httpmiddleware.IP(c.RealIP()) }
}

// ByRoute uses Echo's matched route template.
func ByRoute() func(*framework.Context) (string, error) {
	return func(c *framework.Context) (string, error) {
		return httpmiddleware.Route(c.Request().Method, c.RouteInfo().Path)
	}
}

// ByMethod groups requests by standard HTTP method.
func ByMethod() func(*framework.Context) (string, error) {
	return func(c *framework.Context) (string, error) {
		return httpmiddleware.Method(c.Request().Method)
	}
}
