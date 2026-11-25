package gin

import (
	"fmt"

	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"

	framework "github.com/gin-gonic/gin"
)

// New builds a Gin handler and panics on invalid startup configuration.
func New(limiter Limiter, options ...Option) framework.HandlerFunc {
	config := Config{Limiter: limiter}
	for _, option := range options {
		if option == nil {
			panic(fmt.Errorf("%w: nil Gin option", ratelimiter.ErrInvalidConfig))
		}
		option(&config)
	}
	handler, err := NewWithConfig(config)
	if err != nil {
		panic(err)
	}
	return handler
}

func NewWithConfig(config Config) (framework.HandlerFunc, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return func(c *framework.Context) {
		input, err := config.input(c)
		if err != nil {
			c.Abort()
			config.OnError(c, err)
			return
		}
		if input.Skip {
			c.Next()
			return
		}
		result, err := config.Limiter.Allow(c.Request.Context(), input.Key, input.Cost)
		if err != nil {
			if config.FailureMode == FailOpen && ratelimiter.CanFailOpen(err) {
				c.Next()
				return
			}
			c.Abort()
			config.OnError(c, err)
			return
		}
		if config.Headers {
			addHeaders(c, result)
		}
		if !result.Allowed {
			c.Abort()
			config.OnRejected(c, result)
			return
		}
		c.Next()
	}, nil
}

type Policy struct {
	Name string
	Config
}

// Multi creates a sequential middleware handler and panics on invalid configuration.
func Multi(policies ...Policy) framework.HandlerFunc {
	handler, err := NewMulti(Sequential, policies...)
	if err != nil {
		panic(err)
	}
	return handler
}

// NewMulti builds a handler with per-policy selectors and response configuration.
func NewMulti(mode ExecutionMode, policies ...Policy) (framework.HandlerFunc, error) {
	configs := make(map[string]Config, len(policies))
	corePolicies := make([]ratelimiter.Policy, 0, len(policies))
	for _, policy := range policies {
		config, err := policy.Config.normalized()
		if err != nil {
			return nil, fmt.Errorf("policy %q: %w", policy.Name, err)
		}
		configs[policy.Name] = config
		corePolicies = append(corePolicies, ratelimiter.Policy{
			Name: policy.Name, Limiter: config.Limiter, FailureMode: config.FailureMode,
		})
	}
	limiter, err := ratelimiter.NewMulti(mode, corePolicies...)
	if err != nil {
		return nil, err
	}
	return func(c *framework.Context) {
		result, err := limiter.Evaluate(c.Request.Context(), func(name string) (ratelimiter.PolicyInput, error) {
			return configs[name].input(c)
		})
		config := configs[result.Policy]
		if err != nil {
			c.Abort()
			config.OnError(c, err)
			return
		}
		if result.Evaluated && config.Headers {
			addHeaders(c, result.Result)
		}
		if !result.Allowed {
			c.Abort()
			config.OnRejected(c, result.Result)
			return
		}
		c.Next()
	}, nil
}
