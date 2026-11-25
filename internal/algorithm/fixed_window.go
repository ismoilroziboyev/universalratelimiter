package algorithm

import "github.com/ismoilroziboyev/ginuniversalratelimiter/storage"

func fixedWindow(req storage.Request, actual, now int64, state State) (storage.Result, storage.Result, State) {
	start := now - now%req.Window
	if state.start != start {
		state.count = 0
	}
	reset := start + req.Window
	remaining := req.Limit - state.count
	if remaining < req.Cost {
		denied := result(req, remaining, reset-actual, reset)
		return denied, denied, state
	}
	before := result(req, remaining, 0, reset)
	state.start = start
	state.count += req.Cost
	state.Last, state.Expires = now, reset
	return result(req, remaining-req.Cost, 0, reset), before, state
}
