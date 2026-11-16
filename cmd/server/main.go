package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yoohooshantanu/tenantgate/internal/limiter"
	"github.com/yoohooshantanu/tenantgate/internal/logger"
	"github.com/yoohooshantanu/tenantgate/internal/server"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8082")
	redisAddr := os.Getenv("REDIS_ADDR")

	logger.L.Info("starting_tenantgate", "port", port)

	var rdb *redis.Client
	if redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.L.Warn("redis_ping_failed_falling_back_to_l1", "error", err)
			rdb = nil
		} else {
			logger.L.Info("redis_connected", "addr", redisAddr)
		}
		cancel()
	}

	engine := limiter.NewEngine(rdb)
	keyRegistry := server.LoadProductionKeyRegistry()
	mux := server.SetupRouter(engine, keyRegistry)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.L.Info("tenantgate_listening", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Error("server_listen_failed", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.L.Info("shutting_down_tenantgate")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = srv.Shutdown(ctx)
	engine.Close()
	logger.L.Info("tenantgate_terminated_cleanly")
}
