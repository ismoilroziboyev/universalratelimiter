// Package memory provides a process-local rate limiter store.
package memory

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ismoilroziboyev/ginuniversalratelimiter/internal/algorithm"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/internal/clock"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"
)

const shardCount = 64

var (
	ErrCapacity = errors.New("ratelimiter memory: key capacity reached")
	ErrClosed   = errors.New("ratelimiter memory: store closed")
)

// Config controls the key limit, TTL cleanup interval and time source.
// Zero fields select defaults. Clock must be safe for concurrent use.
type Config struct {
	MaxKeys         int
	CleanupInterval time.Duration
	Clock           func() time.Time
}

type shard struct {
	mu     sync.Mutex
	states map[string]algorithm.State
}

// Store must not be copied. Call Close when the store is no longer used.
type Store struct {
	shards  [shardCount]shard
	seed    maphash.Seed
	count   atomic.Int64
	maxKeys int64
	now     func() time.Time
	closed  atomic.Bool
	stop    chan struct{}
	done    chan struct{}
}

var _ storage.BatchStore = (*Store)(nil)

// New creates a store with default configuration.
func New() *Store {
	store, _ := NewWithConfig(Config{})
	return store
}

// NewWithConfig creates a store with a maximum of 10,000 keys and a one-minute
// cleanup interval by default.
func NewWithConfig(config Config) (*Store, error) {
	if config.MaxKeys < 0 || config.CleanupInterval < 0 {
		return nil, fmt.Errorf("%w: negative memory capacity or cleanup interval", storage.ErrInvalidConfig)
	}
	if config.MaxKeys == 0 {
		config.MaxKeys = 10_000
	}
	if config.CleanupInterval == 0 {
		config.CleanupInterval = time.Minute
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	s := &Store{
		seed: maphash.MakeSeed(), maxKeys: int64(config.MaxKeys), now: config.Clock,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go s.cleanup(config.CleanupInterval)
	return s, nil
}

func (s *Store) shardIndex(key string) int {
	return int(maphash.String(s.seed, key) % shardCount)
}

func (s *Store) Allow(ctx context.Context, req storage.Request) (storage.Result, error) {
	if err := req.Validate(); err != nil {
		return storage.Result{}, err
	}
	part := &s.shards[s.shardIndex(req.Key)]
	part.mu.Lock()
	defer part.mu.Unlock()
	now, err := s.operationTime(ctx)
	if err != nil {
		return storage.Result{}, err
	}
	state, exists := s.load(part, req.Key, now)
	result, _, next := algorithm.Evaluate(req, now, state)
	if result.Allowed {
		if !exists && !s.reserve(1) {
			return storage.Result{}, ErrCapacity
		}
		save(part, req.Key, next)
	}
	return result, nil
}

func (s *Store) ValidateAtomic() error {
	if s.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (s *Store) AllowBatch(ctx context.Context, requests []storage.Request) ([]storage.Result, error) {
	if err := storage.ValidateBatch(requests); err != nil {
		return nil, err
	}
	var locked [shardCount]bool
	var indices [storage.MaxBatch]int
	for i, req := range requests {
		index := s.shardIndex(req.Key)
		indices[i], locked[index] = index, true
	}
	for index, used := range locked {
		if used {
			s.shards[index].mu.Lock()
			defer s.shards[index].mu.Unlock()
		}
	}
	now, err := s.operationTime(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]storage.Result, len(requests))
	var previous [storage.MaxBatch]storage.Result
	var next [storage.MaxBatch]algorithm.State
	var missing int64
	allAllowed := true
	for i, req := range requests {
		part := &s.shards[indices[i]]
		state, exists := s.load(part, req.Key, now)
		if !exists {
			missing++
		}
		results[i], previous[i], next[i] = algorithm.Evaluate(req, now, state)
		allAllowed = allAllowed && results[i].Allowed
	}
	if !allAllowed {
		copy(results, previous[:len(requests)])
		return results, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.reserve(missing) {
		return nil, ErrCapacity
	}
	for i, req := range requests {
		save(&s.shards[indices[i]], req.Key, next[i])
	}
	return results, nil
}

func (s *Store) operationTime(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.closed.Load() {
		return 0, ErrClosed
	}
	return s.timestamp()
}

func (s *Store) timestamp() (int64, error) {
	t := s.now()
	if sec := t.Unix(); sec < 0 || sec > clock.MaxTimestamp/1000 {
		return 0, storage.ErrClock
	}
	now := t.UnixMilli()
	if now > clock.MaxTimestamp {
		return 0, storage.ErrClock
	}
	return now, nil
}

func (s *Store) load(part *shard, key string, now int64) (algorithm.State, bool) {
	state, exists := part.states[key]
	if exists && state.Expires <= now {
		delete(part.states, key)
		s.count.Add(-1)
		return algorithm.State{}, false
	}
	return state, exists
}

func save(part *shard, key string, state algorithm.State) {
	if part.states == nil {
		part.states = make(map[string]algorithm.State)
	}
	part.states[key] = state
}

func (s *Store) reserve(n int64) bool {
	for n > 0 {
		count := s.count.Load()
		if n > s.maxKeys-count {
			return false
		}
		if s.count.CompareAndSwap(count, count+n) {
			return true
		}
	}
	return true
}

// Close stops TTL cleanup and waits for operations holding shard locks.
func (s *Store) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		close(s.stop)
	}
	<-s.done
	for i := range s.shards {
		s.shards[i].mu.Lock()
		s.shards[i].states = nil
		s.shards[i].mu.Unlock()
	}
	return nil
}
