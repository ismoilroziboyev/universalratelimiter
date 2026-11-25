package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"
	ginlimit "github.com/ismoilroziboyev/ginuniversalratelimiter/middleware/gin"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage/memory"
)

func main() {
	store := memory.New()
	defer store.Close()
	limiter, err := ratelimiter.New(
		ratelimiter.WithStorage(store),
		ratelimiter.WithTokenBucket(ratelimiter.TokenBucketConfig{Capacity: 100, RefillRate: 10}),
	)
	if err != nil {
		log.Fatal(err)
	}

	r := gin.New()
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Fatal(err)
	}
	ok := func(c *gin.Context) { c.Status(http.StatusNoContent) }
	r.GET("/regions", ginlimit.New(limiter, ginlimit.WithCost(1), ginlimit.WithHeaders(true)), ok)
	r.GET("/reports", ginlimit.New(limiter, ginlimit.WithCost(20), ginlimit.WithHeaders(true)), ok)
	log.Fatal(r.Run("127.0.0.1:8080"))
}
