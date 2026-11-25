package main

import (
	"log"
	"net/http"
	"strconv"

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
	r.GET("/export", ginlimit.New(limiter, ginlimit.WithCostFunc(exportCost)), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	log.Fatal(r.Run("127.0.0.1:8080"))
}

func exportCost(c *gin.Context) (int64, error) {
	limit, err := strconv.ParseInt(c.DefaultQuery("limit", "20"), 10, 64)
	if err != nil || limit < 1 || limit > 1000 {
		return 0, ginlimit.ErrInvalidCost
	}
	return 1 + (limit+99)/100, nil
}
