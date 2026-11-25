package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"
	redisstore "github.com/ismoilroziboyev/ginuniversalratelimiter/storage/redis"
	"github.com/redis/go-redis/v9"
)

func main() {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		address = "127.0.0.1:6379"
	}
	client := redis.NewClient(&redis.Options{
		Addr: address, MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: 500 * time.Millisecond,
	})
	defer client.Close()
	limiter, err := ratelimiter.New(
		ratelimiter.WithStorage(redisstore.New(client)),
		ratelimiter.WithWeightedTokenBucket(ratelimiter.TokenBucketConfig{Capacity: 100, RefillRate: 10}),
	)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := limiter.Allow(ctx, "worker:export", 20)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("allowed=%v remaining=%d\n", result.Allowed, result.Remaining)
}
