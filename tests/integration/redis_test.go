package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lim "github.com/ismoilroziboyev/ginuniversalratelimiter"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"
	redisstore "github.com/ismoilroziboyev/ginuniversalratelimiter/storage/redis"
	"github.com/redis/go-redis/v9"
)

func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

type redisProcess struct {
	binary string
	port   string
	dir    string
	extra  []string
	cmd    *exec.Cmd
}

func fixture(t *testing.T, extra ...string) *redisProcess {
	t.Helper()
	binary := os.Getenv("REDIS_SERVER")
	if binary == "" {
		t.Skip("set REDIS_SERVER to run live Redis integration tests")
	}
	process := &redisProcess{binary: binary, port: freePort(t), dir: t.TempDir(), extra: extra}
	t.Cleanup(func() { process.stop() })
	process.start(t)
	return process
}

func (p *redisProcess) start(t *testing.T) {
	t.Helper()
	args := []string{"--bind", "127.0.0.1", "--port", p.port, "--dir", p.dir, "--save", "", "--appendonly", "no"}
	if len(p.extra) > 0 && p.extra[0] == "sentinel" {
		config := filepath.Join(p.dir, "sentinel.conf")
		body := fmt.Sprintf("bind 127.0.0.1\nport %s\ndir %q\nsentinel monitor ratelimiter 127.0.0.1 %s 1\n", p.port, p.dir, p.extra[1])
		if err := os.WriteFile(config, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		args = []string{config, "--sentinel"}
	} else {
		args = append(args, p.extra...)
	}
	logPath := filepath.Join(p.dir, "redis.log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	p.cmd = exec.Command(p.binary, args...)
	p.cmd.Stdout, p.cmd.Stderr = log, log
	err = p.cmd.Start()
	_ = log.Close()
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(p.options())
	defer client.Close()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if client.Ping(t.Context()).Err() == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	output, _ := os.ReadFile(logPath)
	t.Fatalf("Redis did not start at %s\n%s", p.addr(), output)
}

func (p *redisProcess) stop() {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
	p.cmd = nil
}

func (p *redisProcess) addr() string { return net.JoinHostPort("127.0.0.1", p.port) }

func (p *redisProcess) options() *redis.Options {
	return &redis.Options{
		Addr: p.addr(), MaxRetries: -1, ContextTimeoutEnabled: true,
		DialTimeout: 250 * time.Millisecond, ReadTimeout: 500 * time.Millisecond, WriteTimeout: 500 * time.Millisecond,
	}
}

func native(t *testing.T, store *redisstore.Store, algorithm lim.Option) *lim.RateLimiter {
	t.Helper()
	limiter, err := lim.New(lim.WithStorage(store), algorithm)
	if err != nil {
		t.Fatal(err)
	}
	return limiter
}

func allow(t *testing.T, limiter lim.Limiter, key string, cost int64, allowed bool, remaining int64) lim.Result {
	t.Helper()
	result, err := limiter.Allow(t.Context(), key, cost)
	if err != nil || result.Allowed != allowed || result.Remaining != remaining {
		t.Fatalf("%s cost=%d got %+v, %v; want allowed=%v remaining=%d", key, cost, result, err, allowed, remaining)
	}
	return result
}

func TestRedisAlgorithmsAndTTL(t *testing.T) {
	process := fixture(t)
	client := redis.NewClient(process.options())
	t.Cleanup(func() { _ = client.Close() })
	store := redisstore.New(client)
	for name, option := range map[string]lim.Option{
		"fixed":   lim.WithFixedWindow(lim.FixedWindowConfig{Limit: 5, Window: 24 * time.Hour}),
		"log":     lim.WithSlidingWindowLog(lim.SlidingWindowLogConfig{Limit: 5, Window: time.Minute}),
		"counter": lim.WithSlidingWindowCounter(lim.SlidingWindowCounterConfig{Limit: 5, Window: 24 * time.Hour}),
		"token":   lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 5, RefillRate: 0.1}),
		"leaky":   lim.WithLeakyBucket(lim.LeakyBucketConfig{Capacity: 5, LeakRate: 0.1}),
		"gcra":    lim.WithGCRA(lim.GCRAConfig{Rate: 1, Per: 10 * time.Second, Burst: 5}),
	} {
		t.Run(name, func(t *testing.T) {
			limiter := native(t, store, option)
			allow(t, limiter, "other", 2, true, 3)
			allow(t, limiter, "other", 3, true, 0)
			if result := allow(t, limiter, "other", 1, false, 0); result.RetryAfter <= 0 || result.ResetAt.IsZero() {
				t.Fatal(result)
			}
			if result := allow(t, limiter, "other", math.MaxInt64, false, 0); result.RetryAfter != 0 {
				t.Fatal(result)
			}
			allow(t, limiter, "new", 1, true, 4)
		})
	}
	expiring, err := redisstore.NewWithConfig(client, redisstore.Config{Prefix: "ttl-test"})
	if err != nil {
		t.Fatal(err)
	}
	bucket := native(t, expiring, lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 1, RefillRate: 4}))
	allow(t, bucket, "sensitive-key", 1, true, 0)
	keys, err := client.Keys(t.Context(), "ttl-test:*").Result()
	if err != nil || len(keys) != 1 || strings.Contains(keys[0], "sensitive-key") {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	ttl, err := client.PTTL(t.Context(), keys[0]).Result()
	if err != nil || ttl <= 0 || ttl > 250*time.Millisecond {
		t.Fatalf("ttl=%v err=%v", ttl, err)
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		exists, err := client.Exists(t.Context(), keys[0]).Result()
		if err != nil {
			t.Fatal(err)
		}
		if exists == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Redis key did not expire")
		}
		time.Sleep(15 * time.Millisecond)
	}
	allow(t, bucket, "sensitive-key", 1, true, 0)
}

func TestRedisSharedConcurrencyReloadAndReconnect(t *testing.T) {
	process := fixture(t)
	a, b := redis.NewClient(process.options()), redis.NewClient(process.options())
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	rule := lim.WithWeightedTokenBucket(lim.TokenBucketConfig{Capacity: 100, RefillRate: 0.1})
	first, second := native(t, redisstore.New(a), rule), native(t, redisstore.New(b), rule)
	var allowed atomic.Int64
	var requests sync.WaitGroup
	ctx := t.Context()
	for i := range 160 {
		requests.Go(func() {
			limiter := first
			if i%2 == 0 {
				limiter = second
			}
			result, err := limiter.Allow(ctx, "hot", 2)
			if err != nil {
				t.Error(err)
			}
			if result.Allowed {
				allowed.Add(1)
			}
		})
	}
	requests.Wait()
	if got := allowed.Load(); got != 50 {
		t.Fatalf("allowed=%d, want 50", got)
	}
	if err := a.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	allow(t, second, "hot", 2, false, 0)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := first.Allow(canceled, "hot", 2); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	process.stop()
	if _, err := first.Allow(ctx, "hot", 2); err == nil {
		t.Fatal("expected a storage error while Redis was offline")
	}
	process.start(t)
	allow(t, first, "after-reconnect", 2, true, 98)
}

func exerciseAtomic(t *testing.T, store *redisstore.Store) {
	t.Helper()
	global := native(t, store, lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 2, RefillRate: 0.1}))
	user := native(t, store, lim.WithSlidingWindowLog(lim.SlidingWindowLogConfig{Limit: 1, Window: time.Hour}))
	multi, err := lim.NewMulti(lim.Atomic, lim.Policy{Name: "global", Limiter: global}, lim.Policy{Name: "user", Limiter: user})
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{"alice", "alice", "bob", "charlie"} {
		result, err := multi.Evaluate(t.Context(), func(name string) (lim.PolicyInput, error) {
			if name == "global" {
				return lim.PolicyInput{Key: "shared", Cost: 1}, nil
			}
			return lim.PolicyInput{Key: key, Cost: 1}, nil
		})
		if err != nil || result.Allowed != (i == 0 || i == 2) || (i == 1 && result.Policy != "user") || (i == 3 && result.Policy != "global") {
			t.Fatalf("batch %d: %+v %v", i, result, err)
		}
	}
}

func TestRedisStandaloneAtomic(t *testing.T) {
	process := fixture(t)
	client := redis.NewClient(process.options())
	defer client.Close()
	exerciseAtomic(t, redisstore.New(client))
}

func TestRedisBatchWritePermissions(t *testing.T) {
	process := fixture(t)
	admin := redis.NewClient(process.options())
	defer admin.Close()

	firstKey := fmt.Sprintf("ratelimit:v1:%x", sha256.Sum256([]byte("first")))
	if err := admin.Do(t.Context(), "ACL", "SETUSER", "batch-test", "on", ">test-pass",
		"(~* +eval +evalsha +get +time +client +hello)", "(~"+firstKey+" +set)",
	).Err(); err != nil {
		t.Fatal(err)
	}
	options := process.options()
	options.Username, options.Password = "batch-test", "test-pass"
	client := redis.NewClient(options)
	defer client.Close()
	store := redisstore.New(client)

	params := storage.Parameters{Kind: storage.FixedWindow, Limit: 1, Window: 60_000}
	first := storage.Request{Key: "first", Parameters: params, Cost: 1}
	second := storage.Request{Key: "second", Parameters: params, Cost: 1}
	if _, err := store.AllowBatch(t.Context(), []storage.Request{first, second}); err == nil || !strings.Contains(err.Error(), "RATELIMITER_WRITE_DENIED") {
		t.Fatalf("expected a batch write authorization error, got %v", err)
	}
	if size, err := admin.DBSize(t.Context()).Result(); err != nil || size != 0 {
		t.Fatalf("failed batch changed state: size=%d error=%v", size, err)
	}
	if result, err := store.Allow(t.Context(), first); err != nil || !result.Allowed {
		t.Fatalf("single-key write should remain authorized: %+v, %v", result, err)
	}
}

func TestRedisSentinel(t *testing.T) {
	master := fixture(t)
	sentinel := fixture(t, "sentinel", master.port)
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "ratelimiter", SentinelAddrs: []string{sentinel.addr()}, MaxRetries: -1, ContextTimeoutEnabled: true,
	})
	defer client.Close()
	exerciseAtomic(t, redisstore.New(client))
}

func TestRedisCluster(t *testing.T) {
	if os.Getenv("REDIS_SERVER") == "" {
		t.Skip("set REDIS_SERVER to run live Redis integration tests")
	}
	ctx := t.Context()
	var nodes [3]*redis.Client
	var addresses [3]string
	var ports, busPorts [3]string
	for i := range nodes {
		busPorts[i] = freePort(t)
		node := fixture(t, "--cluster-enabled", "yes", "--cluster-port", busPorts[i],
			"--cluster-node-timeout", "1000", "--cluster-config-file", "nodes.conf")
		ports[i], addresses[i] = node.port, node.addr()
		nodes[i] = redis.NewClient(node.options())
		t.Cleanup(func() { _ = nodes[i].Close() })
	}
	for i, node := range nodes {
		lastSlot := (i+1)*5461 - 1
		if i == len(nodes)-1 {
			lastSlot = 16383
		}
		if err := node.ClusterAddSlotsRange(ctx, i*5461, lastSlot).Err(); err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			if err := nodes[0].Do(ctx, "CLUSTER", "MEET", "127.0.0.1", ports[i], busPorts[i]).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	for deadline := time.Now().Add(8 * time.Second); ; {
		ready := true
		for _, node := range nodes {
			info, err := node.ClusterInfo(ctx).Result()
			ready = ready && err == nil && strings.Contains(info, "cluster_state:ok")
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			for _, node := range nodes {
				info, err := node.ClusterInfo(ctx).Result()
				t.Log(info, err)
			}
			t.Fatal("Redis Cluster did not reach a healthy state")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cluster := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addresses[:], MaxRetries: -1, ContextTimeoutEnabled: true,
	})
	defer cluster.Close()
	untagged := redisstore.New(cluster)
	single := native(t, untagged, lim.WithTokenBucket(lim.TokenBucketConfig{Capacity: 2, RefillRate: 0.1}))
	for i := range 80 {
		allow(t, single, fmt.Sprintf("client-%d", i), 1, true, 1)
	}
	for _, node := range nodes {
		if size, err := node.DBSize(ctx).Result(); err != nil || size == 0 {
			t.Fatalf("cluster node size=%d, error=%v", size, err)
		}
	}
	if _, err := lim.NewMulti(lim.Atomic, lim.Policy{Name: "untagged", Limiter: single}); !errors.Is(err, lim.ErrUnsupportedAtomic) {
		t.Fatal(err)
	}
	tagged, err := redisstore.NewWithConfig(cluster, redisstore.Config{HashTag: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	exerciseAtomic(t, tagged)
}
