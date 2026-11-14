package logger

import (
	"log/slog"
	"os"
)

var L *slog.Logger

func init() {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelDebug}

	if os.Getenv("LOG_FORMAT") == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	L = slog.New(handler)
	slog.SetDefault(L)
}
