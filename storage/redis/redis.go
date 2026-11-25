// Package redis provides a distributed store backed by an injected Redis client.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ismoilroziboyev/universalratelimiter/internal/clock"
	"github.com/ismoilroziboyev/universalratelimiter/storage"
	goredis "github.com/redis/go-redis/v9"
)

// Config namespaces keys. HashTag co-locates this store's keys in one
// Redis Cluster slot, enabling atomic batches at the cost of distribution.
type Config struct {
	Prefix  string
	HashTag string
}

type Store struct {
	client  goredis.UniversalClient
	prefix  string
	cluster bool
	tagged  bool
}

var _ storage.BatchStore = (*Store)(nil)

// New uses the default key prefix. It returns nil for a nil client.
func New(client goredis.UniversalClient) *Store {
	s, _ := NewWithConfig(client, Config{})
	return s
}

// NewWithConfig configures a store. The caller owns the Redis client's lifecycle.
func NewWithConfig(client goredis.UniversalClient, config Config) (*Store, error) {
	if client == nil || (reflect.ValueOf(client).Kind() == reflect.Pointer && reflect.ValueOf(client).IsNil()) {
		return nil, fmt.Errorf("%w: Redis client is required", storage.ErrInvalidConfig)
	}
	if config.Prefix == "" {
		config.Prefix = "ratelimit:v1"
	}
	if !validKeyPart(config.Prefix) || !validKeyPart(config.HashTag) {
		return nil, fmt.Errorf("%w: Redis prefix and hash tag must contain at most 100 letters, digits, underscores, dashes or colons", storage.ErrInvalidConfig)
	}
	prefix := config.Prefix + ":"
	if config.HashTag != "" {
		prefix += "{" + config.HashTag + "}:"
	}
	_, cluster := client.(*goredis.ClusterClient)
	return &Store{client: client, prefix: prefix, cluster: cluster, tagged: config.HashTag != ""}, nil
}

func validKeyPart(part string) bool {
	if len(part) > 100 {
		return false
	}
	for _, c := range part {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == ':' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func (s *Store) Allow(ctx context.Context, request storage.Request) (storage.Result, error) {
	if err := request.Validate(); err != nil {
		return storage.Result{}, err
	}
	results, err := s.run(ctx, []storage.Request{request})
	if err != nil {
		return storage.Result{}, err
	}
	return results[0], nil
}

func (s *Store) ValidateAtomic() error {
	if s.cluster && !s.tagged {
		return fmt.Errorf("%w: Redis Cluster atomic batches require a configured hash tag", storage.ErrUnsupportedAtomic)
	}
	return nil
}

func (s *Store) AllowBatch(ctx context.Context, requests []storage.Request) ([]storage.Result, error) {
	if err := s.ValidateAtomic(); err != nil {
		return nil, err
	}
	if err := storage.ValidateBatch(requests); err != nil {
		return nil, err
	}
	return s.run(ctx, requests)
}

func (s *Store) run(ctx context.Context, requests []storage.Request) ([]storage.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return []storage.Result{}, nil
	}
	keys := make([]string, 0, len(requests))
	args := make([]any, 0, len(requests)*5)
	for _, req := range requests {
		digest := sha256.Sum256([]byte(req.Key))
		var encoded [sha256.Size * 2]byte
		hex.Encode(encoded[:], digest[:])
		keys = append(keys, s.prefix+string(encoded[:]))
		// Above-capacity requests are permanent denials. Clamp before Lua
		// parses the integer so large Go costs never lose meaningful precision.
		args = append(args, string(req.Kind), req.Limit, req.Window, req.Rate, min(req.Cost, req.Limit+1))
	}
	reply, err := evaluateScript.Run(ctx, s.client, keys, args...).Int64Slice()
	if err != nil {
		if strings.HasPrefix(err.Error(), "CROSSSLOT ") {
			return nil, fmt.Errorf("%w: Redis keys are in different cluster slots", storage.ErrUnsupportedAtomic)
		}
		if strings.HasPrefix(err.Error(), "RATELIMITER_CLOCK") {
			return nil, storage.ErrClock
		}
		return nil, fmt.Errorf("ratelimiter Redis evaluation: %w", err)
	}
	if len(reply) != len(requests)*4 {
		return nil, fmt.Errorf("ratelimiter Redis evaluation: unexpected number of result fields")
	}
	results := make([]storage.Result, len(requests))
	for i, req := range requests {
		fields := reply[i*4 : i*4+4]
		results[i] = storage.Result{
			Allowed: fields[0] == 1, Limit: req.Limit, Remaining: fields[1],
			RetryAfter: clock.Duration(fields[2]),
		}
		if fields[3] != 0 {
			results[i].ResetAt = time.UnixMilli(fields[3]).UTC()
		}
	}
	return results, nil
}
