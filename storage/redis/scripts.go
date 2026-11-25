package redis

import (
	"embed"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

//go:embed scripts/*.lua
var scripts embed.FS

var evaluateScript = goredis.NewScript(scriptSource())

func scriptSource() string {
	// A shared entry point supports both a single check and an atomic batch.
	parts := []string{
		"common", "fixed_window", "token_bucket", "sliding_window_log",
		"sliding_window_counter", "leaky_bucket", "gcra", "evaluate",
	}
	var source strings.Builder
	for _, part := range parts {
		body, err := scripts.ReadFile("scripts/" + part + ".lua")
		if err != nil {
			panic(err)
		}
		source.Write(body)
		source.WriteByte('\n')
	}
	return source.String()
}
