// Package storage defines the atomic operation implemented by limiter backends.
package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ismoilroziboyev/ginuniversalratelimiter/internal/clock"
)

var (
	ErrInvalidConfig     = errors.New("ratelimiter: invalid configuration")
	ErrInvalidKey        = errors.New("ratelimiter: invalid key")
	ErrInvalidCost       = errors.New("ratelimiter: invalid cost")
	ErrUnsupportedAtomic = errors.New("ratelimiter: unsupported atomic operation")
	ErrClock             = errors.New("ratelimiter: clock outside supported range")
)

// Kind identifies the rate limiting algorithm.
type Kind string

const (
	FixedWindow          Kind = "fixed_window"
	SlidingWindowLog     Kind = "sliding_window_log"
	SlidingWindowCounter Kind = "sliding_window_counter"
	TokenBucket          Kind = "token_bucket"
	LeakyBucket          Kind = "leaky_bucket"
	GCRA                 Kind = "gcra"

	MaxLimit    = 1_000_000_000
	MaxLogLimit = 4096
	MaxBatch    = 32
	MaxInterval = clock.MaxInterval
	MaxKeyBytes = 4096
)

// Parameters is the normalized algorithm configuration passed to backends.
// Window is measured in milliseconds and Rate is measured in quota units per millisecond.
type Parameters struct {
	Kind   Kind
	Limit  int64
	Window int64
	Rate   float64
}

func (p Parameters) Validate() error {
	if p.Limit <= 0 || p.Limit > MaxLimit {
		return fmt.Errorf("%w: quota must be between 1 and %d", ErrInvalidConfig, MaxLimit)
	}
	switch p.Kind {
	case FixedWindow, SlidingWindowLog, SlidingWindowCounter:
		if p.Window <= 0 || p.Window > MaxInterval.Milliseconds() || p.Rate != 0 {
			return fmt.Errorf("%w: invalid window", ErrInvalidConfig)
		}
		if p.Kind == SlidingWindowLog && p.Limit > MaxLogLimit {
			return fmt.Errorf("%w: sliding window log supports up to %d units", ErrInvalidConfig, MaxLogLimit)
		}
		if p.Kind == SlidingWindowCounter && p.Limit > 100_000 {
			return fmt.Errorf("%w: sliding window counter supports up to 100000 units", ErrInvalidConfig)
		}
	case TokenBucket, LeakyBucket, GCRA:
		if p.Window != 0 || p.Rate <= 0 || math.IsNaN(p.Rate) || math.IsInf(p.Rate, 0) ||
			float64(p.Limit)/p.Rate > float64(MaxInterval.Milliseconds()) {
			return fmt.Errorf("%w: invalid rate or time to restore full quota exceeds %s", ErrInvalidConfig, MaxInterval)
		}
	default:
		return fmt.Errorf("%w: unknown algorithm", ErrInvalidConfig)
	}
	return nil
}

// Request describes one atomic evaluation. Reuse a key only with identical parameters.
type Request struct {
	Key string
	Parameters
	Cost int64
}

func (r Request) Validate() error {
	if len(r.Key) == 0 || len(r.Key) > MaxKeyBytes {
		return ErrInvalidKey
	}
	if r.Cost <= 0 {
		return ErrInvalidCost
	}
	return r.Parameters.Validate()
}

// Result reports quota available immediately after the operation. A denied
// request with zero RetryAfter exceeds the policy capacity permanently.
type Result struct {
	Allowed    bool
	Limit      int64
	Remaining  int64
	RetryAfter time.Duration
	ResetAt    time.Time
}

// Store evaluates a request and charges its cost only when allowed.
type Store interface {
	Allow(context.Context, Request) (Result, error)
}

// BatchStore evaluates distinct keys in order and charges all costs only if
// every request is allowed. Results describe committed state and are returned
// in request order. ValidateAtomic checks backend configuration at startup.
type BatchStore interface {
	Store
	ValidateAtomic() error
	AllowBatch(context.Context, []Request) ([]Result, error)
}

func ValidateBatch(requests []Request) error {
	if len(requests) > MaxBatch {
		return fmt.Errorf("%w: at most %d requests", ErrUnsupportedAtomic, MaxBatch)
	}
	keys := make(map[string]struct{}, len(requests))
	for _, req := range requests {
		if err := req.Validate(); err != nil {
			return err
		}
		if _, exists := keys[req.Key]; exists {
			return fmt.Errorf("%w: duplicate storage key", ErrUnsupportedAtomic)
		}
		keys[req.Key] = struct{}{}
	}
	return nil
}
