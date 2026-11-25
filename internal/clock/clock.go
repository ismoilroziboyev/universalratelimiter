// Package clock defines the shared millisecond time representation.
package clock

import (
	"math"
	"time"
)

const (
	MaxInterval = 365 * 24 * time.Hour
	// Reserve room for two maximum-length windows in Redis Lua's
	// exact integer range.
	MaxTimestamp = int64(1<<53) - 2*int64(MaxInterval/time.Millisecond) - 1
)

func Duration(milliseconds int64) time.Duration {
	if milliseconds > math.MaxInt64/int64(time.Millisecond) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(milliseconds) * time.Millisecond
}
