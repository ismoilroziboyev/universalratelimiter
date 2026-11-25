package fiber_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	fiber "github.com/gofiber/fiber/v3"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	fiberlimit "github.com/ismoilroziboyev/universalratelimiter/middleware/fiber"
	"github.com/ismoilroziboyev/universalratelimiter/storage/memory"
)

type requestKey struct{}

type limiterFunc func(context.Context, string, int64) (ratelimiter.Result, error)

func (f limiterFunc) Allow(ctx context.Context, key string, cost int64) (ratelimiter.Result, error) {
	return f(ctx, key, cost)
}

func TestFiberGlobalMiddlewareUsesRouteTemplate(t *testing.T) {
	store, err := memory.NewWithConfig(memory.Config{Clock: func() time.Time { return time.Unix(100, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	limiter, err := ratelimiter.New(ratelimiter.WithStorage(store),
		ratelimiter.WithSlidingWindowLog(ratelimiter.SlidingWindowLogConfig{Limit: 2, Window: time.Minute}))
	if err != nil {
		t.Fatal(err)
	}
	mw, err := fiberlimit.New(fiberlimit.Config{
		Limiter: limiter, KeyFunc: fiberlimit.ByRoute(), Cost: 2, Headers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	app.Use(mw)
	app.Post("/reports/:id", func(c fiber.Ctx) error { return c.Send(c.Body()) })
	app.Post("/regions/:id", func(c fiber.Ctx) error { return c.Send(c.Body()) })
	for _, tc := range []struct {
		path string
		code int
		body string
	}{
		{"/reports/1", 200, "intact"},
		{"/reports/2", 429, "RATE_LIMIT_EXCEEDED"},
		{"/regions/4", 200, "intact"},
		{"/unmatched/1", 404, ""},
		{"/unmatched/2", 429, "RATE_LIMIT_EXCEEDED"},
	} {
		req := httptest.NewRequest("POST", tc.path, strings.NewReader("intact"))
		res, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || res.StatusCode != tc.code || !strings.Contains(string(raw), tc.body) {
			t.Fatalf("%s returned %d %s (%v)", tc.path, res.StatusCode, raw, err)
		}
		if tc.code == 429 && res.Header.Get("Retry-After") != "60" {
			t.Fatalf("retry header: %v", res.Header)
		}
	}
}

func TestFiberContextAndNativeRejectionHandler(t *testing.T) {
	mw, err := fiberlimit.New(fiberlimit.Config{
		Limiter: limiterFunc(func(ctx context.Context, key string, cost int64) (ratelimiter.Result, error) {
			if ctx.Value(requestKey{}) != "authenticated" || key != "global:tenant" || cost != 1 {
				t.Errorf("unexpected context, key or cost: %v %q %d", ctx.Value(requestKey{}), key, cost)
			}
			return ratelimiter.Result{Limit: 1}, nil
		}),
		KeyFunc: fiberlimit.Global("tenant"),
		OnRejected: func(fiber.Ctx, ratelimiter.Result) error {
			return fiber.NewError(418, "custom rejection")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(context.WithValue(c.Context(), requestKey{}, "authenticated"))
		return c.Next()
	})
	app.Get("/", mw, func(c fiber.Ctx) error { return c.SendStatus(204) })
	res, err := app.Test(httptest.NewRequest("GET", "/", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 418 {
		t.Fatalf("status=%d", res.StatusCode)
	}
}

func TestFiberForwardingRequiresTrustedProxy(t *testing.T) {
	app := fiber.New(fiber.Config{ProxyHeader: "X-Forwarded-For"})
	mw, err := fiberlimit.New(fiberlimit.Config{
		Limiter: limiterFunc(func(_ context.Context, key string, _ int64) (ratelimiter.Result, error) {
			if key == "ip:203.0.113.6" {
				t.Error("used a forwarding header with proxy trust disabled")
			}
			return ratelimiter.Result{Allowed: true}, nil
		}),
		KeyFunc: fiberlimit.ByClientIP(),
	})
	if err != nil {
		t.Fatal(err)
	}
	app.Get("/", mw, func(c fiber.Ctx) error { return c.SendStatus(204) })
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.6")
	res, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		t.Fatalf("status=%d", res.StatusCode)
	}
}
