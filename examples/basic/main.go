package main

import (
	"context"
	"fmt"
	"log"
	"time"

	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage/memory"
)

func main() {
	store := memory.New()
	defer store.Close()

	limiter, err := ratelimiter.New(
		ratelimiter.WithStorage(store),
		ratelimiter.WithFixedWindow(ratelimiter.FixedWindowConfig{Limit: 100, Window: time.Minute}),
	)
	if err != nil {
		log.Fatal(err)
	}
	result, err := limiter.Allow(context.Background(), "worker:export", 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("allowed=%v remaining=%d\n", result.Allowed, result.Remaining)
}
