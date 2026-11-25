package nethttp_test

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

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	httpmw "github.com/ismoilroziboyev/universalratelimiter/middleware/nethttp"
	"github.com/ismoilroziboyev/universalratelimiter/storage/memory"
)

type limiterFunc func(context.Context, string, int64) (ratelimiter.Result, error)

func (f limiterFunc) Allow(ctx context.Context, key string, cost int64) (ratelimiter.Result, error) {
	return f(ctx, key, cost)
}

func hit(handler http.Handler, method, path, payload string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(payload))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestWeightedServeMuxRoutes(t *testing.T) {
	store, err := memory.NewWithConfig(memory.Config{Clock: func() time.Time { return time.Unix(100, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	limiter, err := ratelimiter.New(ratelimiter.WithStorage(store),
		ratelimiter.WithTokenBucket(ratelimiter.TokenBucketConfig{Capacity: 5, RefillRate: 2}))
	if err != nil {
		t.Fatal(err)
	}
	wrap, err := httpmw.New(httpmw.Config{
		Limiter: limiter, KeyFunc: httpmw.ByRoute(), Headers: true,
		CostFunc: func(r httpmw.Request) (int64, error) {
			return strconv.ParseInt(r.URL.Query().Get("cost"), 10, 64)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /reports/{id}", wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(w, r.Body); err != nil {
			t.Error(err)
		}
	})))

	if res := hit(mux, "POST", "/reports/12?cost=4", "intact"); res.Code != 200 || res.Body.String() != "intact" ||
		res.Header().Get("X-RateLimit-Remaining") != "1" {
		t.Fatalf("first request: %d %v %s", res.Code, res.Header(), res.Body)
	}
	if res := hit(mux, "POST", "/reports/34?cost=4", ""); res.Code != 429 ||
		res.Header().Get("Retry-After") != "2" || !strings.Contains(res.Body.String(), `"retry_after":2`) {
		t.Fatalf("second request: %d %v %s", res.Code, res.Header(), res.Body)
	}
	if res := hit(mux, "POST", "/reports/12?cost=bad", ""); res.Code != 400 {
		t.Fatalf("invalid cost returned %d", res.Code)
	}
	if res := hit(mux, "POST", "/reports/12?cost=6", ""); res.Code != 429 || res.Header().Get("Retry-After") != "" {
		t.Fatalf("permanent denial: %d %v", res.Code, res.Header())
	}
}

func TestFailureModeAndSkip(t *testing.T) {
	offline := errors.New("storage offline")
	for _, mode := range []ratelimiter.FailureMode{ratelimiter.FailOpen, ratelimiter.FailClosed} {
		t.Run(strconv.Itoa(int(mode)), func(t *testing.T) {
			wrap, err := httpmw.New(httpmw.Config{
				Limiter: limiterFunc(func(_ context.Context, key string, cost int64) (ratelimiter.Result, error) {
					if key != "ip:192.0.2.1" || cost != 1 {
						t.Errorf("unexpected key %q or cost %d", key, cost)
					}
					return ratelimiter.Result{}, offline
				}),
				FailureMode: mode,
				SkipFunc:    func(r httpmw.Request) bool { return r.URL.Path == "/health" },
			})
			if err != nil {
				t.Fatal(err)
			}
			handler := wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			req := httptest.NewRequest("GET", "/reports", nil)
			req.Header.Set("X-Forwarded-For", "203.0.113.10")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			want := http.StatusNoContent
			if mode == ratelimiter.FailClosed {
				want = http.StatusServiceUnavailable
			}
			if rec.Code != want {
				t.Fatalf("backend failure returned %d, want %d", rec.Code, want)
			}
			if res := hit(handler, "GET", "/health", ""); res.Code != http.StatusNoContent {
				t.Fatalf("health returned %d", res.Code)
			}
		})
	}
}

func TestAtomicPolicyHeadersAndRollback(t *testing.T) {
	store := memory.New()
	defer store.Close()
	global, err := ratelimiter.New(ratelimiter.WithStorage(store),
		ratelimiter.WithTokenBucket(ratelimiter.TokenBucketConfig{Capacity: 2, RefillRate: 0.1}))
	if err != nil {
		t.Fatal(err)
	}
	user, err := ratelimiter.New(ratelimiter.WithStorage(store),
		ratelimiter.WithSlidingWindowLog(ratelimiter.SlidingWindowLogConfig{Limit: 1, Window: time.Hour}))
	if err != nil {
		t.Fatal(err)
	}
	wrap, err := httpmw.NewMulti(ratelimiter.Atomic,
		httpmw.Policy{Name: "global", Config: httpmw.Config{Limiter: global, KeyFunc: httpmw.Global("api"), Headers: true}},
		httpmw.Policy{Name: "resource", Config: httpmw.Config{Limiter: user, KeyFunc: httpmw.ByRoute(), Headers: true}},
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	mux := http.NewServeMux()
	mux.Handle("GET /reports/{id}", handler)
	mux.Handle("GET /regions/{id}", handler)
	for _, req := range []struct {
		path  string
		code  int
		limit string
	}{
		{"/reports/1", 204, "2"},
		{"/reports/2", 429, "1"},
		{"/regions/1", 204, "2"},
		{"/regions/2", 429, "2"},
	} {
		rec := hit(mux, "GET", req.path, "")
		if rec.Code != req.code || rec.Header().Get("X-RateLimit-Limit") != req.limit {
			t.Fatalf("%s: status=%d headers=%v", req.path, rec.Code, rec.Header())
		}
	}
}
