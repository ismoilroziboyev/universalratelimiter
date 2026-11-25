package algorithm

import "github.com/ismoilroziboyev/ginuniversalratelimiter/storage"

// slidingLog drops expired entries and only scans for a retry time on denial.
func slidingLog(req storage.Request, actual, now int64, state State) (storage.Result, storage.Result, State) {
	for len(state.log) > 0 && state.log[0].at <= now-req.Window {
		state.count -= state.log[0].cost
		state.log = state.log[1:]
	}
	var reset int64
	if len(state.log) > 0 {
		reset = state.log[len(state.log)-1].at + req.Window
	}
	remaining := req.Limit - state.count
	if remaining < req.Cost {
		for _, item := range state.log {
			remaining += item.cost
			if remaining >= req.Cost {
				denied := result(req, req.Limit-state.count, item.at+req.Window-actual, reset)
				return denied, denied, state
			}
		}
	}
	before := result(req, remaining, 0, reset)
	state.log = append(state.log, entry{at: now, cost: req.Cost})
	state.count += req.Cost
	state.Last, state.Expires = now, now+req.Window
	return result(req, remaining-req.Cost, 0, state.Expires), before, state
}
