package gin

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/netip"
	"strconv"
	"strings"

	ratelimiter "github.com/ismoilroziboyev/ginuniversalratelimiter"
	"github.com/ismoilroziboyev/ginuniversalratelimiter/storage"

	framework "github.com/gin-gonic/gin"
)

func Global(name string) KeyFunc {
	return func(*framework.Context) (string, error) {
		if name == "" {
			return "", ratelimiter.ErrInvalidKey
		}
		return "global:" + name, nil
	}
}

// ByIP follows Gin's trusted proxy configuration.
func ByIP() KeyFunc {
	return func(c *framework.Context) (string, error) {
		ip, err := netip.ParseAddr(c.ClientIP())
		if err != nil || ip.Zone() != "" {
			return "", ratelimiter.ErrInvalidKey
		}
		return "ip:" + ip.Unmap().String(), nil
	}
}

// ByUserID reads a value already verified by application middleware.
// An optional argument overrides the Gin context key.
func ByUserID(contextKey ...string) KeyFunc    { return identity("user", "user_id", contextKey) }
func BySessionID(contextKey ...string) KeyFunc { return identity("session", "session_id", contextKey) }
func ByAPIKey(contextKey ...string) KeyFunc    { return identity("api_key", "api_key", contextKey) }
func ByTenantID(contextKey ...string) KeyFunc  { return identity("tenant", "tenant_id", contextKey) }
func ByIdentifier(contextKey string) KeyFunc   { return identity("identifier", contextKey, nil) }

func identity(scope, contextKey string, override []string) KeyFunc {
	if len(override) == 1 {
		contextKey = override[0]
	}
	return func(c *framework.Context) (string, error) {
		if len(override) > 1 || contextKey == "" {
			return "", ratelimiter.ErrInvalidKey
		}
		value, _ := c.Get(contextKey)
		var id string
		switch v := value.(type) {
		case string:
			id = v
		case int:
			id = strconv.Itoa(v)
		case int64:
			id = strconv.FormatInt(v, 10)
		case uint64:
			id = strconv.FormatUint(v, 10)
		}
		if id == "" || len(id) > storage.MaxKeyBytes {
			return "", ratelimiter.ErrInvalidKey
		}
		digest := sha256.Sum256([]byte(id))
		return scope + ":" + hex.EncodeToString(digest[:]), nil
	}
}

func ByRoute() KeyFunc {
	return func(c *framework.Context) (string, error) {
		path := c.FullPath()
		if path == "" {
			path = "unmatched"
		}
		return "route:" + method(c.Request.Method) + ":" + path, nil
	}
}

func ByMethod() KeyFunc {
	return func(c *framework.Context) (string, error) {
		return "method:" + method(c.Request.Method), nil
	}
}

func method(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

// Composite combines multiple resolvers without ambiguous concatenation.
func Composite(resolvers ...KeyFunc) KeyFunc {
	parts := append([]KeyFunc(nil), resolvers...)
	return func(c *framework.Context) (string, error) {
		if len(parts) == 0 {
			return "", ratelimiter.ErrInvalidKey
		}
		var key strings.Builder
		key.WriteString("composite:")
		for _, resolve := range parts {
			if resolve == nil {
				return "", ratelimiter.ErrInvalidKey
			}
			part, err := resolve(c)
			if err != nil {
				return "", err
			}
			size := strconv.Itoa(len(part))
			if part == "" || key.Len()+len(size)+1+len(part) > storage.MaxKeyBytes {
				return "", ratelimiter.ErrInvalidKey
			}
			key.WriteString(size)
			key.WriteByte(':')
			key.WriteString(part)
		}
		return key.String(), nil
	}
}
