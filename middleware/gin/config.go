// Package gin provides Gin middleware for weighted and multi-policy limiting.
package gin

import (
	"fmt"
	"reflect"

	framework "github.com/gin-gonic/gin"
	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
)

type FailureMode = ratelimiter.FailureMode
type ExecutionMode = ratelimiter.ExecutionMode

const (
	FailOpen   = ratelimiter.FailOpen
	FailClosed = ratelimiter.FailClosed
	Sequential = ratelimiter.Sequential
	Atomic     = ratelimiter.Atomic
)

type Limiter = ratelimiter.Limiter
type KeyFunc func(*framework.Context) (string, error)
type CostFunc func(*framework.Context) (int64, error)
type SkipFunc func(*framework.Context) bool
type RejectionHandler func(*framework.Context, ratelimiter.Result)
type ErrorHandler func(*framework.Context, error)

type Config struct {
	Limiter     Limiter
	KeyFunc     KeyFunc
	Cost        int64
	CostFunc    CostFunc
	SkipFunc    SkipFunc
	OnRejected  RejectionHandler
	OnError     ErrorHandler
	FailureMode FailureMode
	Headers     bool
}

type Option func(*Config)

func WithKey(key KeyFunc) Option              { return func(c *Config) { c.KeyFunc = key } }
func WithSkip(skip SkipFunc) Option           { return func(c *Config) { c.SkipFunc = skip } }
func WithHeaders(enabled bool) Option         { return func(c *Config) { c.Headers = enabled } }
func WithFailureMode(mode FailureMode) Option { return func(c *Config) { c.FailureMode = mode } }
func WithRejectionHandler(handler RejectionHandler) Option {
	return func(c *Config) { c.OnRejected = handler }
}
func WithErrorHandler(handler ErrorHandler) Option { return func(c *Config) { c.OnError = handler } }

func (c Config) normalized() (Config, error) {
	value := reflect.ValueOf(c.Limiter)
	if c.Limiter == nil || (value.Kind() == reflect.Pointer && value.IsNil()) ||
		c.Cost < 0 || (c.FailureMode != FailOpen && c.FailureMode != FailClosed) {
		return Config{}, fmt.Errorf("%w: invalid Gin middleware configuration", ratelimiter.ErrInvalidConfig)
	}
	if c.KeyFunc == nil {
		c.KeyFunc = ByIP()
	}
	if c.Cost == 0 {
		c.Cost = 1
	}
	if c.OnRejected == nil {
		c.OnRejected = reject
	}
	if c.OnError == nil {
		c.OnError = handleError
	}
	return c, nil
}
