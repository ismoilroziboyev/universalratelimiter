package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"
	ginlimit "github.com/ismoilroziboyev/ginuniversalratelimiter/middleware/gin"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage/memory"
)

func main() {
	store := memory.New()
	defer store.Close()
	global, err := ratelimiter.New(
		ratelimiter.WithStorage(store),
		ratelimiter.WithFixedWindow(ratelimiter.FixedWindowConfig{Limit: 1_000, Window: time.Minute}),
	)
	if err != nil {
		log.Fatal(err)
	}
	perIP, err := ratelimiter.New(
		ratelimiter.WithStorage(store),
		ratelimiter.WithTokenBucket(ratelimiter.TokenBucketConfig{Capacity: 25, RefillRate: 5}),
	)
	if err != nil {
		log.Fatal(err)
	}

	handler, err := ginlimit.NewMulti(ginlimit.Atomic,
		ginlimit.Policy{Name: "api", Config: ginlimit.Config{Limiter: global, KeyFunc: ginlimit.Global("api"), Headers: true}},
		ginlimit.Policy{Name: "ip", Config: ginlimit.Config{Limiter: perIP, KeyFunc: ginlimit.ByIP(), Headers: true}},
	)
	if err != nil {
		log.Fatal(err)
	}
	r := gin.New()
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Fatal(err)
	}
	r.Use(handler)
	r.GET("/regions", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	log.Fatal(r.Run("127.0.0.1:8080"))
}
