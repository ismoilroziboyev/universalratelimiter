// Package grpc provides server interceptors for grpc-go.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
	"github.com/ismoilroziboyev/universalratelimiter/storage"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	framework "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Config configures admission for unary RPCs and streams. The default key is
// the full RPC method and the default cost for each RPC is one unit.
type Config struct {
	Limiter     ratelimiter.Limiter
	KeyFunc     func(ctx context.Context, method string) (string, error)
	SkipFunc    func(ctx context.Context, method string) bool
	Cost        int64
	FailureMode ratelimiter.FailureMode

	// UnaryCostFunc overrides Cost for unary RPCs.
	UnaryCostFunc func(ctx context.Context, method string, request any) (int64, error)
	// StreamMessageCost enables charging for successfully decoded incoming
	// messages in addition to stream admission. Zero disables the static cost.
	StreamMessageCost int64
	// StreamMessageCostFunc enables per-message charging and overrides its static cost.
	StreamMessageCostFunc func(ctx context.Context, method string, message any) (int64, error)
}

// Interceptors can be shared by concurrent RPCs.
type Interceptors struct {
	config Config
}

// New validates the configuration and constructs both server interceptors.
func New(config Config) (*Interceptors, error) {
	limiter := reflect.ValueOf(config.Limiter)
	switch limiter.Kind() {
	case reflect.Invalid:
		return nil, fmt.Errorf("%w: a gRPC limiter is required", ratelimiter.ErrInvalidConfig)
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		if limiter.IsNil() {
			return nil, fmt.Errorf("%w: a gRPC limiter is required", ratelimiter.ErrInvalidConfig)
		}
	}
	if config.Cost < 0 || config.StreamMessageCost < 0 ||
		(config.FailureMode != ratelimiter.FailClosed && config.FailureMode != ratelimiter.FailOpen) {
		return nil, fmt.Errorf("%w: invalid gRPC cost or failure mode", ratelimiter.ErrInvalidConfig)
	}
	if config.Cost == 0 {
		config.Cost = 1
	}
	if config.KeyFunc == nil {
		config.KeyFunc = func(_ context.Context, method string) (string, error) {
			return method, nil
		}
	}
	return &Interceptors{config: config}, nil
}

// Unary checks quota before invoking the unary handler.
func (i *Interceptors) Unary(ctx context.Context, request any, info *framework.UnaryServerInfo, handler framework.UnaryHandler) (any, error) {
	method := info.FullMethod
	if i.skipped(ctx, method) {
		return handler(ctx, request)
	}
	key, err := i.key(ctx, method)
	if err != nil {
		return nil, err
	}
	cost := i.config.Cost
	if i.config.UnaryCostFunc != nil {
		cost, err = i.config.UnaryCostFunc(ctx, method, request)
		if err != nil {
			return nil, inputError(err)
		}
	}
	if err := i.allow(ctx, key, cost); err != nil {
		return nil, err
	}
	return handler(ctx, request)
}

// Stream checks quota before starting the handler and optionally before
// delivering each decoded incoming message to it.
func (i *Interceptors) Stream(service any, stream framework.ServerStream, info *framework.StreamServerInfo, handler framework.StreamHandler) error {
	ctx, method := stream.Context(), info.FullMethod
	if i.skipped(ctx, method) {
		return handler(service, stream)
	}
	key, err := i.key(ctx, method)
	if err != nil {
		return err
	}
	if err := i.allow(ctx, key, i.config.Cost); err != nil {
		return err
	}
	if i.config.StreamMessageCost == 0 && i.config.StreamMessageCostFunc == nil {
		return handler(service, stream)
	}
	limited := &limitedStream{ServerStream: stream, limiter: i, method: method, key: key}
	err = handler(service, limited)
	if limited.denied != nil {
		return limited.denied
	}
	return err
}

func (i *Interceptors) skipped(ctx context.Context, method string) bool {
	return i.config.SkipFunc != nil && i.config.SkipFunc(ctx, method)
}

func (i *Interceptors) key(ctx context.Context, method string) (string, error) {
	key, err := i.config.KeyFunc(ctx, method)
	if err != nil {
		return "", inputError(err)
	}
	if key == "" || len(key) > storage.MaxKeyBytes {
		return "", inputError(ratelimiter.ErrInvalidKey)
	}
	return key, nil
}

func (i *Interceptors) allow(ctx context.Context, key string, cost int64) error {
	if cost <= 0 {
		return inputError(ratelimiter.ErrInvalidCost)
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	result, err := i.config.Limiter.Allow(ctx, key, cost)
	if err != nil {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		switch {
		case isContextError(err):
			return status.FromContextError(err).Err()
		case errors.Is(err, ratelimiter.ErrInvalidCost), errors.Is(err, ratelimiter.ErrInvalidKey):
			return inputError(err)
		case i.config.FailureMode == ratelimiter.FailOpen && ratelimiter.CanFailOpen(err):
			return nil
		default:
			return status.Error(codes.Unavailable, "Rate limiter unavailable")
		}
	}
	if result.Allowed {
		return nil
	}
	denied := status.New(codes.ResourceExhausted, "Rate limit exceeded")
	if result.RetryAfter > 0 {
		if detailed, err := denied.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(result.RetryAfter)}); err == nil {
			denied = detailed
		}
	}
	return denied.Err()
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func inputError(err error) error {
	if isContextError(err) {
		return status.FromContextError(err).Err()
	}
	// Explicit statuses from application resolvers can carry, for example,
	// Unauthenticated when a verified identity is required.
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.InvalidArgument, "Invalid rate limit input")
}

type limitedStream struct {
	framework.ServerStream
	limiter *Interceptors
	method  string
	key     string
	denied  error
}

func (s *limitedStream) RecvMsg(message any) error {
	if s.denied != nil {
		return s.denied
	}
	if err := s.ServerStream.RecvMsg(message); err != nil {
		return err
	}
	cost := s.limiter.config.StreamMessageCost
	if resolve := s.limiter.config.StreamMessageCostFunc; resolve != nil {
		var err error
		cost, err = resolve(s.Context(), s.method, message)
		if err != nil {
			s.denied = inputError(err)
			return s.denied
		}
	}
	s.denied = s.limiter.allow(s.Context(), s.key, cost)
	return s.denied
}
