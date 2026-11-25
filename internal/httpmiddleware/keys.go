package httpmiddleware

import (
	"net/http"
	"net/netip"

	ratelimiter "github.com/ismoilroziboyev/universalratelimiter"
)

func Global[C any](name string) func(C) (string, error) {
	return func(C) (string, error) {
		if name == "" {
			return "", ratelimiter.ErrInvalidKey
		}
		return "global:" + name, nil
	}
}

// IP accepts a socket address or an IP extracted by a trusted framework.
func IP(raw string) (string, error) {
	var ip netip.Addr
	if address, err := netip.ParseAddrPort(raw); err == nil {
		ip = address.Addr()
	} else {
		ip, err = netip.ParseAddr(raw)
		if err != nil {
			return "", ratelimiter.ErrInvalidKey
		}
	}
	return "ip:" + ip.WithZone("").Unmap().String(), nil
}

func Route(method, pattern string) (string, error) {
	if pattern == "" {
		pattern = "unmatched"
	}
	return "route:" + canonicalMethod(method) + ":" + pattern, nil
}

func Method(method string) (string, error) {
	return "method:" + canonicalMethod(method), nil
}

func canonicalMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}
