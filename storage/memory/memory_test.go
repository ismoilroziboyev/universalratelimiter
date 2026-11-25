package memory

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ismoilroziboyev/universalratelimiter/storage"
)

func TestCleanupCapacityAndClose(t *testing.T) {
	var millis atomic.Int64
	millis.Store(100_000)
	store, err := NewWithConfig(Config{
		MaxKeys: 1, CleanupInterval: time.Millisecond,
		Clock: func() time.Time { return time.UnixMilli(millis.Load()) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	request := storage.Request{Key: "first", Parameters: storage.Parameters{Kind: storage.FixedWindow, Limit: 1, Window: 1000}, Cost: 1}
	if result, err := store.Allow(ctx, request); err != nil || !result.Allowed {
		t.Fatalf("%+v %v", result, err)
	}
	request.Key = "second"
	if _, err := store.Allow(ctx, request); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}

	millis.Add(1000)
	deadline := time.Now().Add(time.Second)
	for store.count.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if count := store.count.Load(); count != 0 {
		t.Fatalf("cleanup left %d entries", count)
	}

	other := request
	other.Key = "third"
	if _, err := store.AllowBatch(ctx, []storage.Request{request, other}); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if count := store.count.Load(); count != 0 {
		t.Fatalf("rejected batch stored %d entries", count)
	}
	if result, err := store.Allow(ctx, request); err != nil || !result.Allowed {
		t.Fatalf("%+v %v", result, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Allow(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Allow(ctx, request); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
