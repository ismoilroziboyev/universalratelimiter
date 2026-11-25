package ratelimiter

import (
	"fmt"
	"time"

	"github.com/ismoilroziboyev/ginuniversalratelimiter/observability"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"
)

type FixedWindowConfig struct {
	Limit  int64
	Window time.Duration
}

type SlidingWindowConfig struct {
	Limit  int64
	Window time.Duration
}

type SlidingWindowLogConfig = SlidingWindowConfig
type SlidingWindowCounterConfig = SlidingWindowConfig

type TokenBucketConfig struct {
	Capacity   int64
	RefillRate float64 // Quota units per second.
}

type LeakyBucketConfig struct {
	Capacity int64
	LeakRate float64 // Quota units per second.
}

type GCRAConfig struct {
	Rate  int64
	Per   time.Duration
	Burst int64 // Maximum quota units allowed at once in a fresh bucket.
}

type config struct {
	store      storage.Store
	parameters storage.Parameters
	namespace  string
	hooks      observability.Hooks
}

// Option configures a limiter before its first use.
type Option func(*config) error

func WithStorage(store storage.Store) Option {
	return func(c *config) error {
		c.store = store
		return nil
	}
}

func WithNamespace(namespace string) Option {
	return func(c *config) error {
		if namespace == "" || len(namespace) > storage.MaxKeyBytes {
			return fmt.Errorf("%w: invalid namespace", ErrInvalidConfig)
		}
		c.namespace = namespace
		return nil
	}
}

func WithHooks(hooks observability.Hooks) Option {
	return func(c *config) error {
		c.hooks = hooks
		return nil
	}
}

func WithFixedWindow(c FixedWindowConfig) Option {
	return withWindow(storage.FixedWindow, c.Limit, c.Window)
}

func WithSlidingWindowLog(c SlidingWindowLogConfig) Option {
	return withWindow(storage.SlidingWindowLog, c.Limit, c.Window)
}

func WithSlidingWindowCounter(c SlidingWindowCounterConfig) Option {
	return withWindow(storage.SlidingWindowCounter, c.Limit, c.Window)
}

func WithTokenBucket(c TokenBucketConfig) Option {
	return withParameters(storage.Parameters{Kind: storage.TokenBucket, Limit: c.Capacity, Rate: c.RefillRate / 1000})
}

func WithWeightedTokenBucket(c TokenBucketConfig) Option {
	return WithTokenBucket(c)
}

func WithLeakyBucket(c LeakyBucketConfig) Option {
	return withParameters(storage.Parameters{Kind: storage.LeakyBucket, Limit: c.Capacity, Rate: c.LeakRate / 1000})
}

func WithGCRA(c GCRAConfig) Option {
	return func(config *config) error {
		if c.Rate <= 0 || c.Rate > storage.MaxLimit || !validWindow(c.Per) {
			return fmt.Errorf("%w: GCRA requires a positive rate and a whole-millisecond period", ErrInvalidConfig)
		}
		return withParameters(storage.Parameters{Kind: storage.GCRA, Limit: c.Burst, Rate: float64(c.Rate) / float64(c.Per.Milliseconds())})(config)
	}
}

func withWindow(kind storage.Kind, limit int64, window time.Duration) Option {
	return func(c *config) error {
		if !validWindow(window) {
			return fmt.Errorf("%w: window must be a positive whole-millisecond duration up to %s", ErrInvalidConfig, storage.MaxInterval)
		}
		return withParameters(storage.Parameters{Kind: kind, Limit: limit, Window: window.Milliseconds()})(c)
	}
}

func validWindow(window time.Duration) bool {
	return window > 0 && window <= storage.MaxInterval && window%time.Millisecond == 0
}

func withParameters(p storage.Parameters) Option {
	return func(c *config) error {
		if c.parameters.Kind != "" {
			return fmt.Errorf("%w: select exactly one algorithm", ErrInvalidConfig)
		}
		if err := p.Validate(); err != nil {
			return err
		}
		c.parameters = p
		return nil
	}
}
