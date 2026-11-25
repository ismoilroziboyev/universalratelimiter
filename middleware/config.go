// Package middleware defines common configuration for HTTP rate limiter adapters.
package middleware

import ratelimiter "github.com/ismoilroziboyev/universalratelimiter"

// Config configures a limiter for a framework's request type.
type Config[C any] struct {
	Limiter     ratelimiter.Limiter
	KeyFunc     func(C) (string, error)
	Cost        int64
	CostFunc    func(C) (int64, error)
	SkipFunc    func(C) bool
	OnRejected  func(C, ratelimiter.Result) error
	OnError     func(C, error) error
	FailureMode ratelimiter.FailureMode
	Headers     bool
}

// Policy selects a named limit and its request and response behavior.
type Policy[C any] struct {
	Name string
	Config[C]
}
