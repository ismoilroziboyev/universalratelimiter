package algorithm

import "github.com/ismoilroziboyev/universalratelimiter/storage"

func slidingCounter(req storage.Request, actual, now int64, state State) (storage.Result, storage.Result, State) {
	start := now - now%req.Window
	switch {
	case start == state.start:
	case start-state.start == req.Window:
		state.previous, state.count = state.count, 0
	default:
		state.previous, state.count = 0, 0
	}
	offset := now - start
	weighted := float64(state.previous*(req.Window-offset)) / float64(req.Window)
	remaining := max(0, req.Limit-state.count-ceil(weighted))
	var reset int64
	if state.count > 0 {
		reset = start + 2*req.Window
	} else if state.previous > 0 {
		reset = start + req.Window
	}
	if remaining < req.Cost {
		budget := req.Limit - req.Cost
		var ready int64
		if state.count <= budget {
			ready = start + counterOffset(state.previous, budget-state.count, req.Window)
		} else {
			ready = start + req.Window + counterOffset(state.count, budget, req.Window)
		}
		denied := result(req, remaining, max(now+1, ready)-actual, reset)
		return denied, denied, state
	}
	before := result(req, remaining, 0, reset)
	state.start = start
	state.count += req.Cost
	state.Last, state.Expires = now, start+2*req.Window
	return result(req, remaining-req.Cost, 0, state.Expires), before, state
}

func counterOffset(count, budget, window int64) int64 {
	return ceil(float64((count-budget)*window) / float64(count))
}
