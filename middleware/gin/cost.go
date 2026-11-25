package gin

import (
	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"

	framework "github.com/gin-gonic/gin"
)

var ErrInvalidCost = ratelimiter.ErrInvalidCost

func WithCost(cost int64) Option        { return func(c *Config) { c.Cost = cost } }
func WithCostFunc(cost CostFunc) Option { return func(c *Config) { c.CostFunc = cost } }

func (c Config) input(ctx *framework.Context) (ratelimiter.PolicyInput, error) {
	if c.SkipFunc != nil && c.SkipFunc(ctx) {
		return ratelimiter.PolicyInput{Skip: true}, nil
	}
	key, err := c.KeyFunc(ctx)
	if err != nil {
		return ratelimiter.PolicyInput{}, resolverError{err}
	}
	if key == "" || len(key) > storage.MaxKeyBytes {
		return ratelimiter.PolicyInput{}, ratelimiter.ErrInvalidKey
	}
	cost := c.Cost
	if c.CostFunc != nil {
		cost, err = c.CostFunc(ctx)
		if err != nil {
			return ratelimiter.PolicyInput{}, resolverError{err}
		}
	}
	if cost <= 0 {
		return ratelimiter.PolicyInput{}, ErrInvalidCost
	}
	return ratelimiter.PolicyInput{Key: key, Cost: cost}, nil
}

// resolverError preserves the cause for custom handlers while classifying
// resolver failures as request errors in the default response.
type resolverError struct{ error }

func (e resolverError) Unwrap() error { return e.error }
