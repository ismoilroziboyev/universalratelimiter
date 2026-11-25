// Package fiber provides rate limiting for Fiber v3.7 and later in the v3 series.
package fiber

import (
	"context"

	framework "github.com/gofiber/fiber/v3"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	"github.com/ismoilroziboyev/universalratelimiter/internal/httpmiddleware"
	"github.com/ismoilroziboyev/universalratelimiter/middleware"
)

type Config = middleware.Config[framework.Ctx]
type Policy = middleware.Policy[framework.Ctx]

// New builds a Fiber handler. The default key uses the remote peer.
func New(config Config) (framework.Handler, error) {
	handler, err := httpmiddleware.New(config, transport)
	if err != nil {
		return nil, err
	}
	return wrap(handler), nil
}

// NewMulti builds middleware with sequential or atomic policy evaluation.
func NewMulti(mode ratelimiter.ExecutionMode, policies ...Policy) (framework.Handler, error) {
	handler, err := httpmiddleware.NewMulti(mode, policies, transport)
	if err != nil {
		return nil, err
	}
	return wrap(handler), nil
}

func wrap(handler httpmiddleware.Handler[framework.Ctx]) framework.Handler {
	return func(c framework.Ctx) error {
		return handler(c, c.Next)
	}
}

var transport = httpmiddleware.Transport[framework.Ctx]{
	Context: func(c framework.Ctx) context.Context { return c.Context() },
	Key:     ByIP(),
	Header:  func(c framework.Ctx, key, value string) { c.Set(key, value) },
	JSON:    func(c framework.Ctx, status int, body any) error { return c.Status(status).JSON(body) },
}

// Global returns a fixed key shared by every request.
func Global(name string) func(framework.Ctx) (string, error) {
	return httpmiddleware.Global[framework.Ctx](name)
}

// ByIP uses the transport peer. Use ByClientIP with a trusted ProxyConfig.
func ByIP() func(framework.Ctx) (string, error) {
	return peerIP
}

func peerIP(c framework.Ctx) (string, error) {
	return httpmiddleware.IP(c.RequestCtx().RemoteIP().String())
}

// ByClientIP uses Fiber's configured proxy settings. Configure TrustProxy
// and IP validation for forwarding chains before using this resolver.
func ByClientIP() func(framework.Ctx) (string, error) {
	return func(c framework.Ctx) (string, error) {
		if !c.App().Config().TrustProxy {
			return peerIP(c)
		}
		return httpmiddleware.IP(c.IP())
	}
}

// ByRoute uses the endpoint template, including from global middleware.
func ByRoute() func(framework.Ctx) (string, error) {
	return func(c framework.Ctx) (string, error) {
		pattern := ""
		if endpoint := c.Endpoint(); endpoint != nil {
			pattern = endpoint.Path
		}
		return httpmiddleware.Route(c.Method(), pattern)
	}
}

// ByMethod groups requests by standard HTTP method.
func ByMethod() func(framework.Ctx) (string, error) {
	return func(c framework.Ctx) (string, error) { return httpmiddleware.Method(c.Method()) }
}
