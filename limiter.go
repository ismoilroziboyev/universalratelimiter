// Package ratelimiter provides weighted, framework-independent rate limiting.
package ratelimiter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"time"

	"github.com/ismoilroziboyev/universalratelimiter/observability"
	"github.com/ismoilroziboyev/universalratelimiter/storage"
)

// Limiter allows or denies a request for the given key and quota cost.
type Limiter interface {
	Allow(ctx context.Context, key string, cost int64) (Result, error)
}

// RateLimiter is a limiter configured with a single algorithm and backend.
type RateLimiter struct {
	config
	prefix string
}

// New constructs a limiter. Exactly one algorithm and a storage backend are required.
func New(options ...Option) (*RateLimiter, error) {
	cfg := config{namespace: "default"}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil limiter option", ErrInvalidConfig)
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	if nilValue(cfg.store) {
		return nil, fmt.Errorf("%w: storage is required", ErrInvalidConfig)
	}
	if err := cfg.parameters.Validate(); err != nil {
		return nil, err
	}
	p := cfg.parameters
	digest := sha256.Sum256(fmt.Appendf(nil, "%d:%s:%s:%d:%d:%g", len(cfg.namespace), cfg.namespace, p.Kind, p.Limit, p.Window, p.Rate))
	return &RateLimiter{config: cfg, prefix: hex.EncodeToString(digest[:]) + ":"}, nil
}

func (l *RateLimiter) Allow(ctx context.Context, key string, cost int64) (Result, error) {
	return l.allow(ctx, "", key, cost)
}

func (l *RateLimiter) allow(ctx context.Context, policy, key string, cost int64) (Result, error) {
	start := time.Now()
	req, err := l.request(policy, key, cost)
	var result Result
	if err == nil {
		result, err = l.store.Allow(ctx, req)
	}
	l.emit(policy, cost, start, result, err == nil && result.Allowed, err)
	return result, err
}

func (l *RateLimiter) request(policy, key string, cost int64) (storage.Request, error) {
	if cost <= 0 {
		return storage.Request{}, ErrInvalidCost
	}
	if key == "" || len(key) > storage.MaxKeyBytes {
		return storage.Request{}, ErrInvalidKey
	}
	digest := sha256.Sum256([]byte(key))
	scope := "single:"
	if policy != "" {
		scope = "policy:"
		digest = sha256.Sum256(fmt.Appendf(nil, "%d:%s:%x", len(policy), policy, digest))
	}
	var encoded [sha256.Size * 2]byte
	hex.Encode(encoded[:], digest[:])
	return storage.Request{Key: l.prefix + scope + string(encoded[:]), Parameters: l.parameters, Cost: cost}, nil
}

func (l *RateLimiter) emit(policy string, cost int64, start time.Time, result Result, charged bool, err error) {
	if l.hooks.OnAllowed == nil && l.hooks.OnRejected == nil && l.hooks.OnError == nil {
		return
	}
	if policy == "" {
		policy = l.namespace
	}
	kind := observability.Allowed
	if err != nil {
		kind = observability.Failed
	} else if !result.Allowed {
		kind = observability.Rejected
	}
	l.hooks.Emit(kind, observability.Event{
		Policy: policy, Algorithm: l.parameters.Kind, Storage: fmt.Sprintf("%T", l.store),
		Cost: cost, Remaining: result.Remaining, Charged: charged, Duration: time.Since(start), Err: err,
	})
}

func nilValue(value any) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
