package algorithm

import (
	"math"

	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"
)

// leakyBucket tracks queued work in quota units. For GCRA this is the
// theoretical arrival time relative to now, scaled by the emission rate.
func leakyBucket(req storage.Request, actual, now int64, state State) (storage.Result, storage.Result, State) {
	drained := float64(now-state.Last) * req.Rate
	level := math.Max(0, state.value-drained)
	capacity, cost := float64(req.Limit), float64(req.Cost)
	available := capacity - level
	var reset int64
	if level > 0 {
		reset = now + ceil(level/req.Rate)
	}
	if available < cost {
		retry := now - actual + max(1, ceil((cost-available)/req.Rate))
		denied := result(req, int64(available), retry, reset)
		return denied, denied, state
	}
	before := result(req, int64(available), 0, reset)
	state.value = level + cost
	state.Last = now
	state.Expires = now + max(1, ceil(state.value/req.Rate))
	return result(req, int64(capacity-state.value), 0, state.Expires), before, state
}
