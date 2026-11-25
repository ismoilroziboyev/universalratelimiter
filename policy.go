package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"
)

type FailureMode uint8

const (
	FailClosed FailureMode = iota
	FailOpen
)

type ExecutionMode uint8

const (
	Sequential ExecutionMode = iota
	Atomic
)

// Policy binds a uniquely named quota to its failure behavior.
type Policy struct {
	Name        string
	Limiter     Limiter
	FailureMode FailureMode
}

// PolicyInput supplies request-specific values for a named policy.
type PolicyInput struct {
	Key  string
	Cost int64
	Skip bool
}

// MultiResult carries the first rejecting, first failing, or first allowed
// policy. Evaluated is false when all policies were skipped or failed open.
type MultiResult struct {
	Allowed   bool
	Policy    string
	Result    Result
	Evaluated bool
}

type MultiLimiter struct {
	mode     ExecutionMode
	policies []Policy
	batch    storage.BatchStore
}

// NewMulti constructs an ordered set of policies. Atomic mode requires all
// policies to use RateLimiters backed by the same batch-capable store instance.
func NewMulti(mode ExecutionMode, policies ...Policy) (*MultiLimiter, error) {
	if len(policies) == 0 || (mode != Sequential && mode != Atomic) {
		return nil, fmt.Errorf("%w: policy list or execution mode is invalid", ErrInvalidConfig)
	}
	if mode == Atomic && len(policies) > storage.MaxBatch {
		return nil, fmt.Errorf("%w: at most %d policies", ErrUnsupportedAtomic, storage.MaxBatch)
	}
	multi := &MultiLimiter{mode: mode, policies: append([]Policy(nil), policies...)}
	names := make(map[string]struct{}, len(policies))
	for _, policy := range policies {
		if policy.Name == "" || len(policy.Name) > storage.MaxKeyBytes || nilValue(policy.Limiter) ||
			(policy.FailureMode != FailClosed && policy.FailureMode != FailOpen) {
			return nil, fmt.Errorf("%w: invalid policy", ErrInvalidConfig)
		}
		if _, exists := names[policy.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate policy name %q", ErrInvalidConfig, policy.Name)
		}
		names[policy.Name] = struct{}{}
		if mode == Atomic {
			limiter, ok := policy.Limiter.(*RateLimiter)
			if !ok {
				return nil, fmt.Errorf("%w: policy %q requires a RateLimiter", ErrUnsupportedAtomic, policy.Name)
			}
			batch, ok := limiter.store.(storage.BatchStore)
			if !ok || !reflect.ValueOf(batch).Comparable() || (multi.batch != nil && multi.batch != batch) {
				return nil, fmt.Errorf("%w: policies must use the same batch store", ErrUnsupportedAtomic)
			}
			multi.batch = batch
		}
	}
	if mode == Atomic {
		if err := multi.batch.ValidateAtomic(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnsupportedAtomic, err)
		}
	}
	return multi, nil
}

// Evaluate resolves policies lazily in Sequential mode and before the batch
// in Atomic mode. Resolver and input errors are always returned.
func (m *MultiLimiter) Evaluate(ctx context.Context, resolve func(name string) (PolicyInput, error)) (MultiResult, error) {
	if resolve == nil {
		return MultiResult{}, fmt.Errorf("%w: missing policy resolver", ErrInvalidConfig)
	}
	if m.mode == Atomic {
		return m.evaluateAtomic(ctx, resolve)
	}
	out := MultiResult{Allowed: true}
	for _, policy := range m.policies {
		input, err := resolve(policy.Name)
		if err != nil {
			return MultiResult{Policy: policy.Name}, err
		}
		if input.Skip {
			continue
		}
		var result Result
		if limiter, ok := policy.Limiter.(*RateLimiter); ok {
			// Include the policy name in native quota identity and hook events.
			result, err = limiter.allow(ctx, policy.Name, input.Key, input.Cost)
		} else {
			if input.Cost <= 0 {
				return MultiResult{Policy: policy.Name}, ErrInvalidCost
			}
			if input.Key == "" || len(input.Key) > storage.MaxKeyBytes {
				return MultiResult{Policy: policy.Name}, ErrInvalidKey
			}
			result, err = policy.Limiter.Allow(ctx, input.Key, input.Cost)
		}
		if err != nil {
			if policy.FailureMode == FailOpen && CanFailOpen(err) {
				continue
			}
			return MultiResult{Policy: policy.Name}, err
		}
		decision := MultiResult{Allowed: result.Allowed, Policy: policy.Name, Result: result, Evaluated: true}
		if !result.Allowed {
			return decision, nil
		}
		if !out.Evaluated {
			out = decision
		}
	}
	return out, nil
}

func (m *MultiLimiter) evaluateAtomic(ctx context.Context, resolve func(name string) (PolicyInput, error)) (MultiResult, error) {
	requests := make([]storage.Request, 0, len(m.policies))
	active := make([]Policy, 0, len(m.policies))
	start := time.Now()
	for _, policy := range m.policies {
		input, err := resolve(policy.Name)
		if err != nil {
			return MultiResult{Policy: policy.Name}, err
		}
		if input.Skip {
			continue
		}
		limiter := policy.Limiter.(*RateLimiter)
		req, err := limiter.request(policy.Name, input.Key, input.Cost)
		if err != nil {
			limiter.emit(policy.Name, input.Cost, start, Result{}, false, err)
			return MultiResult{Policy: policy.Name}, err
		}
		requests, active = append(requests, req), append(active, policy)
	}
	if len(active) == 0 {
		return MultiResult{Allowed: true}, nil
	}
	results, err := m.batch.AllowBatch(ctx, requests)
	if err == nil && len(results) != len(active) {
		err = fmt.Errorf("ratelimiter: batch backend returned an unexpected result count")
	}
	if err != nil {
		for i, policy := range active {
			policy.Limiter.(*RateLimiter).emit(policy.Name, requests[i].Cost, start, Result{}, false, err)
		}
		for _, policy := range active {
			if policy.FailureMode == FailClosed || !CanFailOpen(err) {
				return MultiResult{Policy: policy.Name}, err
			}
		}
		return MultiResult{Allowed: true}, nil
	}
	out := MultiResult{Allowed: true, Policy: active[0].Name, Result: results[0], Evaluated: true}
	for i, result := range results {
		if !result.Allowed && out.Allowed {
			out = MultiResult{Policy: active[i].Name, Result: result, Evaluated: true}
		}
	}
	for i, result := range results {
		policy := active[i]
		// Policies that passed in a rejected batch were checked without a charge.
		policy.Limiter.(*RateLimiter).emit(policy.Name, requests[i].Cost, start, result, out.Allowed, nil)
	}
	return out, nil
}

// CanFailOpen reports whether a backend error is eligible for fail-open handling.
func CanFailOpen(err error) bool {
	return err != nil && !errors.Is(err, ErrInvalidCost) && !errors.Is(err, ErrInvalidKey) &&
		!errors.Is(err, ErrInvalidConfig) && !errors.Is(err, ErrUnsupportedAtomic) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}
