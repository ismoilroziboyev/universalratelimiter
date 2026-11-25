package ratelimiter_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lim "github.com/ismoilroziboyev/universalratelimiter"
	"github.com/ismoilroziboyev/universalratelimiter/observability"
	"github.com/ismoilroziboyev/universalratelimiter/storage"
	"github.com/ismoilroziboyev/universalratelimiter/storage/memory"
)

func setup(t *testing.T, option lim.Option, options ...lim.Option) (*lim.RateLimiter, *atomic.Int64) {
	t.Helper()
	now := new(atomic.Int64)
	now.Store(100_000)
	store, err := memory.NewWithConfig(memory.Config{Clock: func() time.Time { return time.UnixMilli(now.Load()) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	limit, err := lim.New(append([]lim.Option{lim.WithStorage(store), option}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	return limit, now
}

func check(t *testing.T, limiter lim.Limiter, key string, cost int64, allowed bool, remaining int64, retry time.Duration) lim.Result {
	t.Helper()
	result, err := limiter.Allow(context.Background(), key, cost)
	if err != nil || result.Allowed != allowed || result.Remaining != remaining || result.RetryAfter != retry {
		t.Fatalf("key=%q cost=%d: got %+v, %v; want allowed=%v remaining=%d retry=%v", key, cost, result, err, allowed, remaining, retry)
	}
	return result
}

func TestWeightedBucket(t *testing.T) {
	limiter, now := setup(t, lim.WithWeightedTokenBucket(lim.TokenBucketConfig{Capacity: 4, RefillRate: 2.5}))
	result := check(t, limiter, "reports", 3, true, 1, 0)
	if result.Limit != 4 || !result.ResetAt.Equal(time.UnixMilli(101_200)) {
		t.Fatal(result)
	}
	check(t, limiter, "reports", 2, false, 1, 400*time.Millisecond)
	now.Add(200)
	check(t, limiter, "reports", 2, false, 1, 200*time.Millisecond)
	now.Add(200)
	check(t, limiter, "reports", 2, true, 0, 0)
	now.Add(-50)
	check(t, limiter, "reports", 1, false, 0, 450*time.Millisecond)
	check(t, limiter, "reports", math.MaxInt64, false, 0, 0)
	now.Store(102_000)
	check(t, limiter, "reports", 4, true, 0, 0)
	check(t, limiter, "new", math.MaxInt64, false, 4, 0)
}

func TestWindows(t *testing.T) {
	t.Run("fixed boundary", func(t *testing.T) {
		limiter, now := setup(t, lim.WithFixedWindow(lim.FixedWindowConfig{Limit: 4, Window: time.Second}))
		now.Add(999)
		check(t, limiter, "client", 4, true, 0, 0)
		now.Add(-10)
		check(t, limiter, "client", 1, false, 0, 11*time.Millisecond)
		now.Add(11)
		check(t, limiter, "client", 4, true, 0, 0)
	})
	t.Run("weighted log uses the entry that frees sufficient cost", func(t *testing.T) {
		limiter, now := setup(t, lim.WithSlidingWindowLog(lim.SlidingWindowLogConfig{Limit: 4, Window: time.Second}))
		check(t, limiter, "client", 1, true, 3, 0)
		now.Add(250)
		check(t, limiter, "client", 3, true, 0, 0)
		check(t, limiter, "client", 2, false, 0, 1000*time.Millisecond)
		now.Add(750)
		check(t, limiter, "client", 2, false, 1, 250*time.Millisecond)
		now.Add(250)
		check(t, limiter, "client", 4, true, 0, 0)
	})
	t.Run("counter estimates previous usage", func(t *testing.T) {
		limiter, now := setup(t, lim.WithSlidingWindowCounter(lim.SlidingWindowCounterConfig{Limit: 4, Window: time.Second}))
		check(t, limiter, "client", 4, true, 0, 0)
		check(t, limiter, "client", 1, false, 0, 1250*time.Millisecond)
		now.Add(1249)
		check(t, limiter, "client", 1, false, 0, time.Millisecond)
		now.Add(1)
		check(t, limiter, "client", 1, true, 0, 0)
		now.Add(250)
		check(t, limiter, "client", 1, true, 0, 0)
		now.Add(2000)
		check(t, limiter, "client", 4, true, 0, 0)
	})
}

func TestLeakyBucketAndGCRA(t *testing.T) {
	for name, option := range map[string]lim.Option{
		"leaky": lim.WithLeakyBucket(lim.LeakyBucketConfig{Capacity: 3, LeakRate: 2}),
		"gcra":  lim.WithGCRA(lim.GCRAConfig{Burst: 3, Rate: 2, Per: time.Second}),
	} {
		t.Run(name, func(t *testing.T) {
			limiter, now := setup(t, option)
			check(t, limiter, "client", 3, true, 0, 0)
			check(t, limiter, "client", 2, false, 0, time.Second)
			now.Add(750)
			check(t, limiter, "client", 2, false, 1, 250*time.Millisecond)
			now.Add(250)
			check(t, limiter, "client", 2, true, 0, 0)
		})
	}
}

func TestValidation(t *testing.T) {
	store := memory.New()
	defer store.Close()
	for name, opts := range map[string][]lim.Option{
		"missing storage":   {lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 1, RefillRate: 1})},
		"missing algo":      {lim.WithStorage(store)},
		"nil storage":       {lim.WithStorage((*memory.Store)(nil))},
		"nil option":        {nil},
		"negative capacity": {lim.WithStorage(store), lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: -1, RefillRate: 1})},
		"nan refill":        {lim.WithStorage(store), lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 1, RefillRate: math.NaN()})},
		"infinite refill":   {lim.WithStorage(store), lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 1, RefillRate: math.Inf(1)})},
		"overflow refill":   {lim.WithStorage(store), lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 10, RefillRate: 1e-30})},
		"invalid burst":     {lim.WithStorage(store), lim.WithGCRA(lim.GCRAConfig{Rate: 1, Per: time.Second})},
		"fractional window": {lim.WithStorage(store), lim.WithFixedWindow(lim.FixedWindowConfig{Limit: 10, Window: time.Microsecond})},
		"oversized log":     {lim.WithStorage(store), lim.WithSlidingWindowLog(lim.SlidingWindowLogConfig{Limit: storage.MaxLogLimit + 1, Window: time.Second})},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := lim.New(opts...); !errors.Is(err, lim.ErrInvalidConfig) {
				t.Fatalf("got %v", err)
			}
		})
	}
	limiter, now := setup(t, lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 1, RefillRate: 1}))
	if _, err := limiter.Allow(context.Background(), "client", 0); !errors.Is(err, lim.ErrInvalidCost) {
		t.Fatal(err)
	}
	if _, err := limiter.Allow(context.Background(), "", 1); !errors.Is(err, lim.ErrInvalidKey) {
		t.Fatal(err)
	}
	now.Store(math.MaxInt64)
	if _, err := limiter.Allow(context.Background(), "client", 1); !errors.Is(err, storage.ErrClock) {
		t.Fatal(err)
	}
	now.Store(1 << 52)
	check(t, limiter, "client", 1, true, 0, 0)
	now.Store(0)
	check(t, limiter, "client", 1, false, 0, time.Duration(math.MaxInt64))
}

func TestNoOverdraft(t *testing.T) {
	limiter, _ := setup(t, lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 100, RefillRate: 1}))
	var allowed atomic.Int64
	var group sync.WaitGroup
	for range 192 {
		group.Go(func() {
			result, err := limiter.Allow(context.Background(), "shared", 2)
			if err != nil {
				t.Error(err)
			}
			if result.Allowed {
				allowed.Add(1)
			}
		})
	}
	group.Wait()
	if got := allowed.Load(); got != 50 {
		t.Fatalf("allowed %d, want 50", got)
	}
}

func TestMultiPolicy(t *testing.T) {
	for _, mode := range []lim.ExecutionMode{lim.Sequential, lim.Atomic} {
		t.Run(map[lim.ExecutionMode]string{lim.Sequential: "sequential", lim.Atomic: "atomic"}[mode], func(t *testing.T) {
			store := memory.New()
			defer store.Close()
			var events []observability.Event
			global, err := lim.New(lim.WithStorage(store), lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 2, RefillRate: 0.1}),
				lim.WithHooks(observability.Hooks{OnAllowed: func(event observability.Event) { events = append(events, event) }}))
			if err != nil {
				t.Fatal(err)
			}
			user, err := lim.New(lim.WithStorage(store), lim.WithSlidingWindowLog(lim.SlidingWindowLogConfig{Limit: 1, Window: time.Hour}))
			if err != nil {
				t.Fatal(err)
			}
			multi, err := lim.NewMulti(mode, lim.Policy{Name: "global", Limiter: global}, lim.Policy{Name: "user", Limiter: user})
			if err != nil {
				t.Fatal(err)
			}
			request := func(id string) lim.MultiResult {
				t.Helper()
				result, err := multi.Evaluate(context.Background(), func(name string) (lim.PolicyInput, error) {
					if name == "global" {
						return lim.PolicyInput{Key: "shared", Cost: 1}, nil
					}
					return lim.PolicyInput{Key: id, Cost: 1}, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			if result := request("same"); !result.Allowed || result.Policy != "global" {
				t.Fatal(result)
			}
			if result := request("same"); result.Allowed || result.Policy != "user" {
				t.Fatal(result)
			}
			if events[1].Policy != "global" || events[1].Charged != (mode == lim.Sequential) {
				t.Fatal(events)
			}
			if result := request("other"); result.Allowed != (mode == lim.Atomic) || result.Policy != "global" {
				t.Fatal(result)
			}
		})
	}
}

func TestMultiRejectsUnsupportedConfiguration(t *testing.T) {
	left, _ := setup(t, lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 1, RefillRate: 1}))
	right, _ := setup(t, lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 1, RefillRate: 1}))
	if _, err := lim.NewMulti(lim.Atomic, lim.Policy{Name: "left", Limiter: left}, lim.Policy{Name: "right", Limiter: right}); !errors.Is(err, lim.ErrUnsupportedAtomic) {
		t.Fatal(err)
	}
	if _, err := lim.NewMulti(lim.Sequential, lim.Policy{Name: "same", Limiter: left}, lim.Policy{Name: "same", Limiter: right}); !errors.Is(err, lim.ErrInvalidConfig) {
		t.Fatal(err)
	}
}
