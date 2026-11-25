package gin_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"
	ginlimit "github.com/ismoilroziboyev/ginuniversalratelimiter/middleware/gin"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/observability"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage/memory"
)

type limiterFunc func(context.Context, string, int64) (ratelimiter.Result, error)

func (f limiterFunc) Allow(ctx context.Context, key string, cost int64) (ratelimiter.Result, error) {
	return f(ctx, key, cost)
}

type failingStore struct{ err error }

func (s failingStore) Allow(context.Context, storage.Request) (storage.Result, error) {
	return storage.Result{}, s.err
}

func router(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if err := r.SetTrustedProxies(nil); err != nil {
		t.Fatal(err)
	}
	return r
}

func serve(r http.Handler, method, url, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, url, strings.NewReader(body))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, request)
	return rec
}

func TestDynamicCostRouteHeadersAndBody(t *testing.T) {
	r := router(t)
	store, err := memory.NewWithConfig(memory.Config{Clock: func() time.Time { return time.UnixMilli(2000) }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucket, err := ratelimiter.New(ratelimiter.WithStorage(store), ratelimiter.WithTokenBucket(ratelimiter.TokenBucketConfig{Capacity: 5, RefillRate: 2}))
	if err != nil {
		t.Fatal(err)
	}
	group := r.Group("/v1", ginlimit.New(bucket, ginlimit.WithKey(ginlimit.ByRoute()), ginlimit.WithHeaders(true),
		ginlimit.WithCostFunc(func(c *gin.Context) (int64, error) {
			value, err := strconv.ParseInt(c.DefaultQuery("items", "1"), 10, 64)
			if err != nil || value < 1 || value > 100 {
				return 0, ginlimit.ErrInvalidCost
			}
			return value, nil
		})))
	group.POST("/report/:id", func(c *gin.Context) {
		raw, err := io.ReadAll(c.Request.Body)
		if err != nil {
			t.Error(err)
		}
		c.String(http.StatusOK, "%s", raw)
	})
	if res := serve(r, "POST", "/v1/report/123?items=4", "example"); res.Code != 200 || res.Body.String() != "example" || res.Header().Get("X-RateLimit-Remaining") != "1" {
		t.Fatalf("first response: %+v %s", res.Result(), res.Body)
	}
	if res := serve(r, "POST", "/v1/report/456?items=4", ""); res.Code != 429 || res.Header().Get("Retry-After") != "2" || !strings.Contains(res.Body.String(), "\"retry_after\":2") {
		t.Fatalf("route-limited response: %+v %s", res.Result(), res.Body)
	}
	if res := serve(r, "POST", "/v1/report/123?items=6", ""); res.Code != 429 || res.Header().Get("Retry-After") != "" || !strings.Contains(res.Body.String(), "\"retry_after\":0") {
		t.Fatalf("permanent denial: %+v %s", res.Result(), res.Body)
	}
	if res := serve(r, "POST", "/v1/report/123?items=0", ""); res.Code != 400 {
		t.Fatalf("invalid cost: %d", res.Code)
	}
}

func TestFailureModesAndHook(t *testing.T) {
	offline := errors.New("storage offline")
	for _, mode := range []ginlimit.FailureMode{ginlimit.FailOpen, ginlimit.FailClosed} {
		t.Run(strconv.Itoa(int(mode)), func(t *testing.T) {
			r := router(t)
			calls := 0
			limiter, err := ratelimiter.New(ratelimiter.WithStorage(failingStore{offline}),
				ratelimiter.WithFixedWindow(ratelimiter.FixedWindowConfig{Limit: 5, Window: time.Second}),
				ratelimiter.WithHooks(observability.Hooks{OnError: func(event observability.Event) {
					calls++
					if !errors.Is(event.Err, offline) {
						t.Error(event)
					}
				}}))
			if err != nil {
				t.Fatal(err)
			}
			r.GET("/", ginlimit.New(limiter, ginlimit.WithFailureMode(mode)), func(c *gin.Context) { c.Status(204) })
			expected := 503
			if mode == ginlimit.FailOpen {
				expected = 204
			}
			if res := serve(r, "GET", "/", ""); res.Code != expected || calls != 1 {
				t.Fatalf("code=%d hook calls=%d", res.Code, calls)
			}
		})
	}
}

func TestCustomHandlersKeySkipAndResponseHeaders(t *testing.T) {
	r := router(t)
	rejected := false
	handler, err := ginlimit.NewWithConfig(ginlimit.Config{
		Limiter: limiterFunc(func(_ context.Context, key string, cost int64) (ratelimiter.Result, error) {
			if key != "custom-identifier" || cost != 3 {
				t.Errorf("%s %d", key, cost)
			}
			return ratelimiter.Result{Limit: 3, RetryAfter: time.Second}, nil
		}),
		KeyFunc: func(c *gin.Context) (string, error) {
			if c.Query("bad") == "1" {
				return "", errors.New("unrecognized identifier")
			}
			return "custom-identifier", nil
		},
		Cost: 3, FailureMode: ginlimit.FailOpen,
		SkipFunc:   func(c *gin.Context) bool { return c.Query("skip") == "1" },
		OnRejected: func(c *gin.Context, _ ratelimiter.Result) { rejected = true; c.Status(418) },
		OnError:    func(c *gin.Context, _ error) { c.Status(422) },
	})
	if err != nil {
		t.Fatal(err)
	}
	r.GET("/", handler, func(c *gin.Context) { c.Status(204) })
	if res := serve(r, "GET", "/?skip=1", ""); res.Code != 204 || rejected {
		t.Fatalf("skip: %d %v", res.Code, rejected)
	}
	if res := serve(r, "GET", "/?bad=1", ""); res.Code != 422 || rejected {
		t.Fatalf("custom error: %d %v", res.Code, rejected)
	}
	if res := serve(r, "GET", "/", ""); res.Code != 418 || !rejected || res.Header().Get("Retry-After") != "" {
		t.Fatalf("custom rejection: %d %v", res.Code, rejected)
	}
}

func TestVerifiedIdentityAndProxyConfiguration(t *testing.T) {
	r := router(t)
	r.GET("/client", func(c *gin.Context) {
		ip, err := ginlimit.ByIP()(c)
		if err != nil || ip != "ip:192.0.2.55" {
			t.Errorf("%q %v", ip, err)
		}
		if _, err = ginlimit.ByAPIKey()(c); !errors.Is(err, ratelimiter.ErrInvalidKey) {
			t.Fatal(err)
		}
		c.Set("api_key", "verified-secret")
		key, err := ginlimit.Composite(ginlimit.ByAPIKey(), ginlimit.ByMethod())(c)
		if err != nil || strings.Contains(key, "verified-secret") || !strings.HasSuffix(key, "method:GET") {
			t.Errorf("%q %v", key, err)
		}
	})
	req := httptest.NewRequest("GET", "/client", nil)
	req.RemoteAddr = "192.0.2.55:1234"
	req.Header.Set("X-Forwarded-For", "203.0.113.1")
	req.Header.Set("X-API-Key", "unverified-secret")
	r.ServeHTTP(httptest.NewRecorder(), req)
}

func TestMultiPolicySelectsRejectionHeaders(t *testing.T) {
	r := router(t)
	first := limiterFunc(func(context.Context, string, int64) (ratelimiter.Result, error) {
		return ratelimiter.Result{Allowed: true, Limit: 10, Remaining: 9}, nil
	})
	second := limiterFunc(func(context.Context, string, int64) (ratelimiter.Result, error) {
		return ratelimiter.Result{Limit: 2, RetryAfter: time.Second}, nil
	})
	r.Use(ginlimit.Multi(
		ginlimit.Policy{Name: "global", Config: ginlimit.Config{Limiter: first, Headers: true}},
		ginlimit.Policy{Name: "user", Config: ginlimit.Config{Limiter: second, Headers: true,
			SkipFunc: func(c *gin.Context) bool { return c.Query("anonymous") == "1" }}},
	))
	r.GET("/", func(c *gin.Context) { c.Status(204) })
	if res := serve(r, "GET", "/", ""); res.Code != 429 || res.Header().Get("X-RateLimit-Limit") != "2" {
		t.Fatalf("%+v", res.Result())
	}
	if res := serve(r, "GET", "/?anonymous=1", ""); res.Code != 204 || res.Header().Get("X-RateLimit-Limit") != "10" {
		t.Fatalf("%+v", res.Result())
	}
}
