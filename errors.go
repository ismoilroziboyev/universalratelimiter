package ratelimiter

import (
	"errors"

	"github.com/ismoilroziboyev/universalratelimiter/storage"
)

var (
	ErrInvalidConfig     = storage.ErrInvalidConfig
	ErrInvalidKey        = storage.ErrInvalidKey
	ErrInvalidCost       = storage.ErrInvalidCost
	ErrUnsupportedAtomic = storage.ErrUnsupportedAtomic
	ErrRateLimitExceeded = errors.New("ratelimiter: quota exceeded")
)
