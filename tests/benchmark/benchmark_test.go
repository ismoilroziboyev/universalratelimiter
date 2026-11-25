package benchmark

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	lim "github.com/ismoilroziboyev/universalratelimiter"
	"github.com/ismoilroziboyev/universalratelimiter/storage"
	"github.com/ismoilroziboyev/universalratelimiter/storage/memory"
	redisstore "github.com/ismoilroziboyev/universalratelimiter/storage/redis"
	"github.com/redis/go-redis/v9"
)

func benchmarkLimiter(b *testing.B, store storage.Store, rule lim.Option) *lim.RateLimiter {
	b.Helper()
	limiter, err := lim.New(lim.WithStorage(store), lim.WithNamespace(fmt.Sprintf("benchmark-%d", time.Now().UnixNano())), rule)
	if err != nil {
		b.Fatal(err)
	}
	return limiter
}

func memoryStore(b *testing.B, maxKeys int) *memory.Store {
	b.Helper()
	store, err := memory.NewWithConfig(memory.Config{MaxKeys: maxKeys})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	return store
}

func repeat(b *testing.B, store storage.Store, rule lim.Option, cost int64) {
	b.Helper()
	limiter := benchmarkLimiter(b, store, rule)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if result, err := limiter.Allow(ctx, "hot", cost); err != nil || !result.Allowed {
			b.Fatalf("%+v %v", result, err)
		}
	}
}

var token = lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: storage.MaxLimit, RefillRate: 1e9})

func BenchmarkMemoryFixedWindow(b *testing.B) {
	rule := lim.WithFixedWindow(lim.FixedWindowConfig{Limit: storage.MaxLimit, Window: time.Second})
	repeat(b, memoryStore(b, 1), rule, 1)
}

func BenchmarkMemoryTokenBucket(b *testing.B) { repeat(b, memoryStore(b, 1), token, 1) }

func BenchmarkMemoryHighCardinality(b *testing.B) {
	const keyCount = 32_768
	limiter := benchmarkLimiter(b, memoryStore(b, keyCount), token)
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("client-%d", i)
	}
	ctx := context.Background()
	b.ReportAllocs()
	index := 0
	for b.Loop() {
		if result, err := limiter.Allow(ctx, keys[index%keyCount], 1); err != nil || !result.Allowed {
			b.Fatalf("%+v %v", result, err)
		}
		index++
	}
}

func BenchmarkMemoryParallelHotKey(b *testing.B) {
	limiter := benchmarkLimiter(b, memoryStore(b, 1), token)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if result, err := limiter.Allow(ctx, "hot", 1); err != nil || !result.Allowed {
				b.Errorf("%+v %v", result, err)
				return
			}
		}
	})
}

func redisStore(b *testing.B) *redisstore.Store {
	b.Helper()
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		b.Skip("set REDIS_ADDR to benchmark against a live Redis instance")
	}
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1, ContextTimeoutEnabled: true})
	b.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		b.Fatal(err)
	}
	return redisstore.New(client)
}

func BenchmarkRedisTokenBucket(b *testing.B)         { repeat(b, redisStore(b), token, 1) }
func BenchmarkRedisWeightedTokenBucket(b *testing.B) { repeat(b, redisStore(b), token, 20) }
