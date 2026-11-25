// Package algorithm implements the in-memory quota transitions used by the limiter.
package algorithm

import (
	"math"
	"time"

	"github.com/ismoilroziboyev/ginuniversalratelimiter/internal/clock"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"
)

type entry struct {
	at   int64
	cost int64
}

// State holds the minimum history required by a policy.
type State struct {
	Last     int64
	Expires  int64
	start    int64
	count    int64
	previous int64
	value    float64
	log      []entry
}

// Evaluate calculates a possible update without changing the current state.
// For an allowed result, the second result describes quota before the charge.
func Evaluate(req storage.Request, actual int64, state State) (storage.Result, storage.Result, State) {
	now := max(actual, state.Last)
	if req.Cost > req.Limit {
		result := storage.Result{Limit: req.Limit, Remaining: req.Limit}
		if state.Expires > actual {
			probe := req
			probe.Cost = 1
			_, result, _ = Evaluate(probe, actual, state)
			result.Allowed = false
			result.RetryAfter = 0
		}
		return result, result, state
	}
	switch req.Kind {
	case storage.FixedWindow:
		return fixedWindow(req, actual, now, state)
	case storage.TokenBucket:
		return tokenBucket(req, actual, now, state)
	case storage.SlidingWindowLog:
		return slidingLog(req, actual, now, state)
	case storage.SlidingWindowCounter:
		return slidingCounter(req, actual, now, state)
	case storage.LeakyBucket, storage.GCRA:
		return leakyBucket(req, actual, now, state)
	default:
		panic("algorithm: unvalidated request")
	}
}

func result(req storage.Request, remaining, retry, reset int64) storage.Result {
	out := storage.Result{
		Allowed:    retry == 0,
		Limit:      req.Limit,
		Remaining:  remaining,
		RetryAfter: clock.Duration(retry),
	}
	if reset > 0 {
		out.ResetAt = time.UnixMilli(reset).UTC()
	}
	return out
}

func ceil(value float64) int64 {
	return int64(math.Ceil(value))
}
