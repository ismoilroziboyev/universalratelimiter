package algorithm

import (
	"math"

	"github.com/ismoilroziboyev/universalratelimiter/storage"
)

func tokenBucket(req storage.Request, actual, now int64, state State) (storage.Result, storage.Result, State) {
	capacity := float64(req.Limit)
	tokens := capacity
	if state.Expires > actual {
		refilled := float64(now-state.Last) * req.Rate
		tokens = math.Min(capacity, state.value+refilled)
	}
	cost := float64(req.Cost)
	beforeReset := now + ceil((capacity-tokens)/req.Rate)
	if tokens < cost {
		retry := now - actual + max(1, ceil((cost-tokens)/req.Rate))
		denied := result(req, int64(tokens), retry, beforeReset)
		return denied, denied, state
	}
	before := result(req, int64(tokens), 0, beforeReset)
	state.value = tokens - cost
	state.Last = now
	state.Expires = now + max(1, ceil((capacity-state.value)/req.Rate))
	return result(req, int64(state.value), 0, state.Expires), before, state
}
