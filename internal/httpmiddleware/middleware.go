// Package httpmiddleware implements the request lifecycle shared by HTTP adapters.
package httpmiddleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"time"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	"github.com/ismoilroziboyev/universalratelimiter/middleware"
	"github.com/ismoilroziboyev/universalratelimiter/storage"
)

type Handler[C any] func(C, func() error) error

// Transport supplies the framework-specific request context and response operations.
type Transport[C any] struct {
	Context func(C) context.Context
	Key     func(C) (string, error)
	Header  func(C, string, string)
	JSON    func(C, int, any) error
}

func New[C any](config middleware.Config[C], transport Transport[C]) (Handler[C], error) {
	config, err := normalize(config, transport)
	if err != nil {
		return nil, err
	}
	return func(request C, next func() error) error {
		input, err := resolve(config, request)
		if err != nil {
			return config.OnError(request, err)
		}
		if input.Skip {
			return next()
		}
		result, err := config.Limiter.Allow(transport.Context(request), input.Key, input.Cost)
		if err != nil {
			if config.FailureMode == ratelimiter.FailOpen && ratelimiter.CanFailOpen(err) {
				return next()
			}
			return config.OnError(request, err)
		}
		return respond(config, transport, request, result, next)
	}, nil
}

func NewMulti[C any](mode ratelimiter.ExecutionMode, policies []middleware.Policy[C], transport Transport[C]) (Handler[C], error) {
	configs := make(map[string]middleware.Config[C], len(policies))
	core := make([]ratelimiter.Policy, 0, len(policies))
	for _, policy := range policies {
		config, err := normalize(policy.Config, transport)
		if err != nil {
			return nil, fmt.Errorf("policy %q: %w", policy.Name, err)
		}
		configs[policy.Name] = config
		core = append(core, ratelimiter.Policy{Name: policy.Name, Limiter: config.Limiter, FailureMode: config.FailureMode})
	}
	limiter, err := ratelimiter.NewMulti(mode, core...)
	if err != nil {
		return nil, err
	}
	return func(request C, next func() error) error {
		decision, err := limiter.Evaluate(transport.Context(request), func(name string) (ratelimiter.PolicyInput, error) {
			return resolve(configs[name], request)
		})
		if err != nil {
			return configs[decision.Policy].OnError(request, err)
		}
		if !decision.Evaluated {
			return next()
		}
		return respond(configs[decision.Policy], transport, request, decision.Result, next)
	}, nil
}

func normalize[C any](config middleware.Config[C], transport Transport[C]) (middleware.Config[C], error) {
	limiter := reflect.ValueOf(config.Limiter)
	if isNil(limiter) || config.Cost < 0 ||
		(config.FailureMode != ratelimiter.FailClosed && config.FailureMode != ratelimiter.FailOpen) {
		return config, fmt.Errorf("%w: invalid HTTP middleware configuration", ratelimiter.ErrInvalidConfig)
	}
	if config.KeyFunc == nil {
		config.KeyFunc = transport.Key
	}
	if config.Cost == 0 {
		config.Cost = 1
	}
	if config.OnRejected == nil {
		config.OnRejected = func(request C, result ratelimiter.Result) error {
			retry := seconds(result.RetryAfter)
			return transport.JSON(request, http.StatusTooManyRequests, response{
				Code: "RATE_LIMIT_EXCEEDED", Message: "Too many requests", RetryAfter: &retry,
			})
		}
	}
	if config.OnError == nil {
		config.OnError = func(request C, err error) error {
			var resolver resolverError
			if errors.As(err, &resolver) || errors.Is(err, ratelimiter.ErrInvalidCost) || errors.Is(err, ratelimiter.ErrInvalidKey) {
				return transport.JSON(request, http.StatusBadRequest, response{
					Code: "RATE_LIMIT_INVALID_REQUEST", Message: "Invalid rate limit input",
				})
			}
			return transport.JSON(request, http.StatusServiceUnavailable, response{
				Code: "RATE_LIMIT_UNAVAILABLE", Message: "Rate limiter unavailable",
			})
		}
	}
	return config, nil
}

func isNil(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func resolve[C any](config middleware.Config[C], request C) (ratelimiter.PolicyInput, error) {
	if config.SkipFunc != nil && config.SkipFunc(request) {
		return ratelimiter.PolicyInput{Skip: true}, nil
	}
	key, err := config.KeyFunc(request)
	if err != nil {
		return ratelimiter.PolicyInput{}, resolverError{err}
	}
	if len(key) == 0 || len(key) > storage.MaxKeyBytes {
		return ratelimiter.PolicyInput{}, ratelimiter.ErrInvalidKey
	}
	cost := config.Cost
	if config.CostFunc != nil {
		cost, err = config.CostFunc(request)
		if err != nil {
			return ratelimiter.PolicyInput{}, resolverError{err}
		}
	}
	if cost <= 0 {
		return ratelimiter.PolicyInput{}, ratelimiter.ErrInvalidCost
	}
	return ratelimiter.PolicyInput{Key: key, Cost: cost}, nil
}

type resolverError struct{ error }

func (e resolverError) Unwrap() error { return e.error }

type response struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	RetryAfter *int64 `json:"retry_after,omitempty"`
}

func respond[C any](config middleware.Config[C], transport Transport[C], request C, result ratelimiter.Result, next func() error) error {
	if config.Headers {
		transport.Header(request, "X-RateLimit-Limit", strconv.FormatInt(result.Limit, 10))
		transport.Header(request, "X-RateLimit-Remaining", strconv.FormatInt(result.Remaining, 10))
		if !result.Allowed && result.RetryAfter > 0 {
			transport.Header(request, "Retry-After", strconv.FormatInt(seconds(result.RetryAfter), 10))
		}
	}
	if !result.Allowed {
		return config.OnRejected(request, result)
	}
	return next()
}

func seconds(retry time.Duration) int64 {
	whole := int64(retry / time.Second)
	if retry%time.Second > 0 {
		whole++
	}
	return whole
}
