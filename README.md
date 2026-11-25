# Universal Rate Limiter

Weighted rate limiting for Go services, with middleware for net/http, Gin, Echo, and Fiber. Use the same `Limiter` interface from an HTTP handler, a gRPC interceptor, or a background worker. Choose an in-memory store for a single process or a Redis store for a shared quota across replicas.

## Install

Requires Go 1.26. Use Redis 8 or later. The Redis backend is tested with Redis 8.8.0.

```sh
go get github.com/ismoilroziboyev/universalratelimiter@v0.2.0
```

## Quick start

```go
import (
    "context"
    "time"

    ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
    "github.com/ismoilroziboyev/universalratelimiter/storage/memory"
)

func newExportLimiter(store *memory.Store) (*ratelimiter.RateLimiter, error) {
    return ratelimiter.New(
        ratelimiter.WithStorage(store),
        ratelimiter.WithFixedWindow(ratelimiter.FixedWindowConfig{
            Limit: 100, Window: time.Minute,
        }),
        ratelimiter.WithNamespace("exports"),
    )
}

func checkExport(ctx context.Context, limiter ratelimiter.Limiter) error {
    result, err := limiter.Allow(ctx, "worker:export", 20)
    if err != nil {
        return err
    }
    if !result.Allowed {
        return ratelimiter.ErrRateLimitExceeded
    }
    return nil
}
```

Create the store and limiter once at startup in an application. Reuse them across goroutines. The core `Allow(ctx, key, cost)` requires a positive cost. Pass `1` for a unit-cost request. Gin uses cost `1` by default.

## Algorithms

| Algorithm | Configuration | Behavior | State per key |
| --- | --- | --- | --- |
| Fixed Window | `Limit`, `Window` | Counts cost in UTC Unix-epoch-aligned windows. A new window restores the entire quota. | O(1) time and space |
| Sliding Window Log | `Limit`, `Window` | Tracks the cost and timestamp of each accepted request. Expires entries at the elapsed window boundary and computes an exact retry time at millisecond resolution. | O(n) space and worst-case time. In-memory appends are amortized O(1) and expiration or rejection may scan entries. Redis decodes active history and encodes updates in O(n). |
| Sliding Window Counter | `Limit`, `Window` | Weights the previous window by its remaining fraction. Usage is rounded up to a whole unit. | O(1) time and space |
| Token Bucket | `Capacity`, `RefillRate` | Starts full and refills fractionally. Unit-cost and weighted requests share one implementation. | O(1) time and space |
| Weighted Token Bucket | `Capacity`, `RefillRate` | Charges the caller-supplied weight per request using the same token bucket. | O(1) time and space |
| Leaky Bucket | `Capacity`, `LeakRate` | Starts empty and drains queued quota virtually. Admission is immediate. | O(1) time and space |
| GCRA | `Rate`, `Per`, `Burst` | Advances virtual arrival time at `Rate / Per`. `Burst` is the maximum quota a fresh key can admit at once. | O(1) time and space |

All algorithms accept weighted cost. `Limit` and `Remaining` report quota units, and `Remaining` is rounded down to a whole usable unit. Window and period durations must be whole milliseconds, up to 365 days. Refill and leak rates are quota units per second and must be finite and positive. A full restore must take at most 365 days. Quotas are capped at 1 billion units, except Sliding Window Counter at 100,000 and Sliding Window Log at 4,096. The log bound also limits Redis script work per key.

`RetryAfter` is the wait for a request of the same cost, assuming no intervening charges. A cost above capacity produces a permanent denial with a zero retry duration. `ResetAt` is the time the full quota becomes available in the absence of more charges, or zero if the algorithm has no reset to report. Quotas use millisecond time resolution. Rate-based algorithms use floating-point arithmetic for fractional quota.

## Storage

### In-memory

```go
store, err := memory.NewWithConfig(memory.Config{
    MaxKeys: 20_000, CleanupInterval: 30 * time.Second,
})
if err != nil {
    return err
}
defer store.Close()
```

`memory.New()` uses 10,000 keys and a one-minute cleanup interval. The sharded store expires inactive state and reports `memory.ErrCapacity` if a new key cannot fit. Expired keys are removed when accessed or on the next cleanup sweep. Reduce `MaxKeys` when many keys can hold large sliding logs. `Clock` can supply a concurrency-safe time source for tests. Separate processes have separate in-memory quotas.

### Redis

```go
client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
    MaxRetries: -1,
    ContextTimeoutEnabled: true,
    DialTimeout: 500 * time.Millisecond,
    ReadTimeout:  200 * time.Millisecond,
    WriteTimeout: 200 * time.Millisecond,
})
defer client.Close()

store, err := redisstore.NewWithConfig(client, redisstore.Config{
    Prefix: "myapp:ratelimit:v1",
})
if err != nil {
    return err
}

limiter, err := ratelimiter.New(
    ratelimiter.WithStorage(store),
    ratelimiter.WithWeightedTokenBucket(ratelimiter.TokenBucketConfig{
        Capacity: 100, RefillRate: 10,
    }),
)
if err != nil {
    return err
}
```

Use `*redis.Client` for standalone, `redis.NewFailoverClient` with `redis.FailoverOptions` for Sentinel, or `redis.NewClusterClient` with `redis.ClusterOptions` for Cluster. The store accepts any `redis.UniversalClient`. The client lifecycle, authentication, TLS, and timeouts belong to the application. Pass a deadline in the request context and enable `ContextTimeoutEnabled` on the client to apply it to Redis I/O.

A warmed check sends one Lua invocation via `EVALSHA`. A `NOSCRIPT` response triggers `EVAL` to restore the cache. Redis server time is authoritative. Charged state always has a TTL, and a token bucket remains in Redis until its quota would be fully restored. Retries after a timeout or connection loss can double-charge a request if Redis already applied it. The example disables automatic command retries. Redis restart, asynchronous failover, or eviction can discard quota state. Choose an eviction policy and persistence settings that fit the deployment.

Use matching namespaces, configuration, and Redis prefixes across instances that share quota. A separate `WithNamespace` or Redis prefix isolates independent applications or budget groups. Changing a policy's algorithm configuration selects a new bucket. The old one expires normally.

## HTTP middleware

| Server or router | Package | Registration |
| --- | --- | --- |
| Standard Go server, Chi, and other net/http routers | `middleware/nethttp` | Wrap a handler or pass the middleware to the router |
| Gin | `middleware/gin` | Router, group, or route middleware |
| Echo v5 | `middleware/echo` | Global, group, or route middleware |
| Fiber v3.7+ | `middleware/fiber` | Global, group, or route handler |

The net/http, Echo, and Fiber adapters expose `New(Config)` and `NewMulti(mode, policies...)`. Both return an error for invalid startup configuration. Each adapter supports static `Cost` and dynamic `CostFunc`, custom key and skip functions, response handlers, failure modes, and optional quota headers. The default responses and multi-policy rules are the same as for Gin. Custom Echo and Fiber response handlers can also return native framework errors. For net/http, write the response through `Request.Writer` and return `nil`.

### net/http and Chi

```go
import (
    "net/http"
    httpmw "github.com/ismoilroziboyev/universalratelimiter/middleware/nethttp"
)

limitReports, err := httpmw.New(httpmw.Config{
    Limiter: limiter, KeyFunc: httpmw.ByRoute(), Cost: 10, Headers: true,
})
if err != nil {
    return err
}

mux := http.NewServeMux()
mux.Handle("GET /reports/{id}", limitReports(http.HandlerFunc(getReport)))
```

`ByIP` is the default and uses the transport peer. For Go's ServeMux, install `ByRoute` on a matched route so `Request.Pattern` is populated. Chi can use the middleware directly with `r.Use(limiterMiddleware)` or `r.With(limiterMiddleware).Get(...)`. Use an explicit route key with `Global` or a custom resolver when the router supplies its own route metadata.

### Echo

```go
import (
    "github.com/labstack/echo/v5"
    echomw "github.com/ismoilroziboyev/universalratelimiter/middleware/echo"
)

limitReports, err := echomw.New(echomw.Config{
    Limiter: limiter, KeyFunc: echomw.ByRoute(), Cost: 10, Headers: true,
})
if err != nil {
    return err
}

app := echo.New()
app.GET("/reports/:id", getReport, limitReports)
```

Echo uses its configured IP extractor for the default `ByIP` resolver. Configure trusted proxies before relying on forwarding headers. The adapter passes `c.Request().Context()` to the limiter.

### Fiber

```go
import (
    "github.com/gofiber/fiber/v3"
    fibermw "github.com/ismoilroziboyev/universalratelimiter/middleware/fiber"
)

limitReports, err := fibermw.New(fibermw.Config{
    Limiter: limiter, KeyFunc: fibermw.ByRoute(), Cost: 10, Headers: true,
})
if err != nil {
    return err
}

app := fiber.New()
app.Get("/reports/:id", limitReports, getReport)
```

Fiber's default `ByIP` uses the transport peer. To use forwarding headers, configure Fiber's trusted proxies and use `ByClientIP`. `ByRoute` resolves the endpoint template even from global middleware. The adapter passes `c.Context()` to the limiter, so a deadline set upstream through `c.SetContext(...)` reaches the backend.

## Gin middleware

```go
import (
    "github.com/gin-gonic/gin"
    ginlimit "github.com/ismoilroziboyev/universalratelimiter/middleware/gin"
)

r := gin.New()
if err := r.SetTrustedProxies(nil); err != nil {
    return err
}
api := r.Group("/api")
api.GET("/regions", ginlimit.New(limiter,
    ginlimit.WithKey(ginlimit.ByIP()),
    ginlimit.WithCost(1),
    ginlimit.WithHeaders(true),
), getRegions)
api.GET("/reports", ginlimit.New(limiter,
    ginlimit.WithKey(ginlimit.ByIP()),
    ginlimit.WithCost(20),
    ginlimit.WithHeaders(true),
), getReports)
```

This example shares one token balance across both endpoints for each IP. Use `Composite(ByIP(), ByRoute())` for a separate budget per route and IP. A handler can also be attached with `r.Use` or `group.Use`. Set trusted proxy CIDRs explicitly when using forwarded client IPs. `ByRoute` uses Gin's matched route template and gives unmatched paths a shared bucket. `ByMethod` groups unknown methods in one bucket. `ByUserID`, `BySessionID`, `ByTenantID`, `ByAPIKey`, and `ByIdentifier` read values set by trusted application middleware. Run authentication before resolvers that depend on it. All core keys and built-in sensitive identifiers are hashed prior to storage.

Use `WithCostFunc` for validated dynamic cost. The callback's value overrides the static cost for that request.

```go
ginlimit.WithCostFunc(func(c *gin.Context) (int64, error) {
    limit, err := strconv.ParseInt(c.DefaultQuery("limit", "20"), 10, 64)
    if err != nil || limit < 1 || limit > 1_000 {
        return 0, ginlimit.ErrInvalidCost
    }
    return 1 + (limit+99)/100, nil
})
```

The middleware leaves the body untouched. A custom body-based resolver should preserve or restore the stream for downstream handlers. `WithSkip` bypasses a policy before key and cost resolution. Use `NewWithConfig` to receive a startup validation error or `New` for Gin-style registration that panics on invalid startup configuration.

With headers enabled, evaluated policies produce `X-RateLimit-Limit` and `X-RateLimit-Remaining`. Temporary denials also include `Retry-After`, rounded up to whole seconds. A default rejection returns HTTP 429.

```json
{"code":"RATE_LIMIT_EXCEEDED","message":"Too many requests","retry_after":5}
```

Permanent denials omit the HTTP retry header and report `retry_after: 0` in JSON. Override responses with `WithRejectionHandler` and `WithErrorHandler`.

### Multiple policies

```go
handler, err := ginlimit.NewMulti(ginlimit.Atomic,
    ginlimit.Policy{Name: "global", Config: ginlimit.Config{
        Limiter: globalLimiter, KeyFunc: ginlimit.Global("api"), Headers: true,
    }},
    ginlimit.Policy{Name: "ip", Config: ginlimit.Config{
        Limiter: ipLimiter, KeyFunc: ginlimit.ByIP(), Headers: true,
    }},
)
if err != nil {
    return err
}
r.Use(handler)
```

`ginlimit.Multi` evaluates sequentially and stops at the first rejection or fail-closed error. Earlier policies may already have charged the request. Atomic mode checks up to 32 uniquely named policies using the same batch-capable store instance and charges only when every active policy allows the request. Native limiters include each policy name in its quota identity. Custom `Limiter` implementations work in sequential mode. The framework-independent equivalent is `ratelimiter.NewMulti` and `Evaluate`.

Successful requests report headers from the first evaluated policy. Rejections report headers from the first denying policy. In a rejected atomic batch, remaining quota reflects the balance without a charge.

For Redis Cluster atomic batches, use one shared `*redisstore.Store` configured with a `HashTag`. It places all keys for that store in the same slot. Separate independent batches with separate tags and stores when the workload permits. A shared tag or a global limiter creates a hot slot or key. Ordinary untagged Redis checks distribute across slots. Unsupported atomic configuration returns `ErrUnsupportedAtomic`. Redis scripts are atomic with respect to other clients. An unexpected script runtime error after a write can leave a partial update. A network error can hide the outcome of a successfully applied script.

## Failure handling and observability

- Reaching a quota is a successful backend evaluation with `Result.Allowed == false`. The core returns a separate error for invalid input or backend failure. Use `errors.Is` for exported error values.
- Gin's default `FailClosed` responds with HTTP 503 on a storage failure. `FailOpen` permits the request and the native limiter still emits its error hook. Invalid input returns HTTP 400. Request cancellation, caller deadline expiry, and unsupported atomic configuration remain errors.
- A failed atomic backend operation follows the first active fail-closed policy. When every active policy is fail-open and the failure is eligible, the request proceeds.
- Supply `WithHooks(observability.Hooks{...})` to record per-policy outcomes in Prometheus, OpenTelemetry, or your logger. Callbacks run inline and must be concurrency-safe. Events include policy name, algorithm, storage type, charged state, cost, remaining quota, elapsed time and error. Raw request keys are excluded. Use low-cardinality metric labels, such as policy and algorithm.

Rate limiting complements application authentication and resource limits. Use verified identities for tenant and user budgets, and keep namespaces and policy names free of sensitive data. Configure Redis memory limits and monitor key counts, especially when callers can cause new IP or identity buckets. Large weights and logs should match the actual service budget.

## Tests and benchmarks

```sh
go test ./...
go test -race ./...
go vet ./...
go test -bench=. -benchmem ./...
```

Run live standalone, reconnect, Sentinel and three-node Cluster tests with Redis installed locally. The tests create isolated processes and temporary directories.

```sh
make integration
REDIS_SERVER=/path/to/redis-server go test -race ./tests/integration
REDIS_ADDR=127.0.0.1:6379 go test -run '^$' -bench=. -benchmem ./tests/benchmark
```

Set `REDIS_SERVER` to enable live integration tests and `REDIS_ADDR` to enable Redis benchmarks. A local run on Apple M4 with Go 1.26.3 and Redis 8.8.0 measured:

| Benchmark | Time/op | Bytes/op | Allocations/op |
| --- | ---: | ---: | ---: |
| In-memory fixed window | 277 ns | 208 | 2 |
| In-memory token bucket | 282 ns | 208 | 2 |
| In-memory high cardinality | 313 ns | 211 | 2 |
| In-memory parallel hot key | 372 ns | 208 | 2 |
| Redis token bucket | 31.8 µs | 1,184 | 24 |
| Redis weighted token bucket | 31.4 µs | 1,184 | 24 |

Redis timing includes loopback round trips and depends on deployment latency.

## Examples

Use `go run ./examples/basic`, `./examples/redis`, `./examples/weighted`, `./examples/dynamic_cost`, or `./examples/multi_policy`. HTTP examples listen on localhost port 8080.

Licensed under [MIT](LICENSE).
