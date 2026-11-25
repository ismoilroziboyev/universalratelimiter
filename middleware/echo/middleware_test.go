package echo_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	echo "github.com/labstack/echo/v5"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	echolimit "github.com/ismoilroziboyev/universalratelimiter/middleware/echo"
)

type limiterFunc func(context.Context, string, int64) (ratelimiter.Result, error)

func (f limiterFunc) Allow(ctx context.Context, key string, cost int64) (ratelimiter.Result, error) {
	return f(ctx, key, cost)
}

type contextKey struct{}

func TestEchoRouteContextAndNativeError(t *testing.T) {
	app := echo.New()
	hits := 0
	mw, err := echolimit.New(echolimit.Config{
		Limiter: limiterFunc(func(ctx context.Context, key string, cost int64) (ratelimiter.Result, error) {
			if key != "route:GET:/reports/:id" || ctx.Value(contextKey{}) != "request" || cost != 3 {
				t.Errorf("unexpected key, context or cost: %q, %v, %d", key, ctx.Value(contextKey{}), cost)
			}
			hits++
			return ratelimiter.Result{Allowed: hits == 1, Limit: 3}, nil
		}),
		KeyFunc: echolimit.ByRoute(), Cost: 3, Headers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	app.Use(mw)
	app.GET("/reports/:id", func(*echo.Context) error { return echo.NewHTTPError(http.StatusConflict, "downstream") })
	for _, tc := range []struct {
		path string
		code int
		body string
	}{
		{"/reports/1", 409, "downstream"},
		{"/reports/2", 429, "RATE_LIMIT_EXCEEDED"},
	} {
		req := httptest.NewRequest("GET", tc.path, nil)
		req = req.WithContext(context.WithValue(req.Context(), contextKey{}, "request"))
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, req)
		if rec.Code != tc.code || !strings.Contains(rec.Body.String(), tc.body) {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
		}
	}
}
