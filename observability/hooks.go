// Package observability provides optional limiter lifecycle callbacks.
package observability

import (
	"time"

	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"
)

// Outcome identifies what happened to the operation.
type Outcome uint8

const (
	Allowed Outcome = iota
	Rejected
	Failed
)

// Event contains policy metadata without the raw request key.
type Event struct {
	Policy    string
	Algorithm storage.Kind
	Storage   string
	Cost      int64
	Remaining int64
	Charged   bool
	Duration  time.Duration
	Err       error
}

// Hooks are synchronous. Callbacks must support concurrent calls.
type Hooks struct {
	OnAllowed  func(Event)
	OnRejected func(Event)
	OnError    func(Event)
}

func (h Hooks) Emit(outcome Outcome, event Event) {
	switch outcome {
	case Allowed:
		if h.OnAllowed != nil {
			h.OnAllowed(event)
		}
	case Rejected:
		if h.OnRejected != nil {
			h.OnRejected(event)
		}
	case Failed:
		if h.OnError != nil {
			h.OnError(event)
		}
	}
}
