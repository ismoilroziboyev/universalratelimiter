package gin

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"

	framework "github.com/gin-gonic/gin"
)

func retrySeconds(retry time.Duration) int64 {
	seconds := int64(retry / time.Second)
	if retry%time.Second > 0 {
		seconds++
	}
	return seconds
}

func addHeaders(c *framework.Context, result ratelimiter.Result) {
	c.Header("X-RateLimit-Limit", strconv.FormatInt(result.Limit, 10))
	c.Header("X-RateLimit-Remaining", strconv.FormatInt(result.Remaining, 10))
	if !result.Allowed && result.RetryAfter > 0 {
		c.Header("Retry-After", strconv.FormatInt(retrySeconds(result.RetryAfter), 10))
	}
}

func reject(c *framework.Context, result ratelimiter.Result) {
	c.JSON(http.StatusTooManyRequests, framework.H{
		"code": "RATE_LIMIT_EXCEEDED", "message": "Too many requests",
		"retry_after": retrySeconds(result.RetryAfter),
	})
}

func handleError(c *framework.Context, err error) {
	var resolver resolverError
	if errors.As(err, &resolver) || errors.Is(err, ratelimiter.ErrInvalidCost) || errors.Is(err, ratelimiter.ErrInvalidKey) {
		c.JSON(http.StatusBadRequest, framework.H{"code": "RATE_LIMIT_INVALID_REQUEST", "message": "Invalid rate limit input"})
		return
	}
	c.JSON(http.StatusServiceUnavailable, framework.H{"code": "RATE_LIMIT_UNAVAILABLE", "message": "Rate limiter unavailable"})
}
