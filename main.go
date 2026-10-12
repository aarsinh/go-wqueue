package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	q := &Queue{rdb: rdb}
	mode := env("MODE", "all") // api | worker | all
	workers, _ := strconv.Atoi(env("WORKERS", "8"))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := rdb.Ping(r.Context()).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	if mode == "api" || mode == "all" {
		registerAPI(mux, q)
	}

	srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
			stop()
		}
	}()

	var wg sync.WaitGroup
	if mode == "worker" || mode == "all" {
		if err := q.RecoverProcessing(ctx); err != nil {
			slog.Error("recover", "err", err)
		}
		startWorkers(ctx, &wg, q, workers)
	}

	<-ctx.Done()
	slog.Info("shutting down: draining in-flight jobs")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		slog.Info("drained cleanly")
	case <-shutdownCtx.Done():
		slog.Warn("drain timed out")
	}
}
