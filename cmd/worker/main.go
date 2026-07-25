package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DarkTheme404/distributed-task-scheduler/internal/config"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/metrics"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/queue"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/storage"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/worker"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage
	pgStore, err := storage.NewPostgresStore(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Fatal("Failed to connect to PostgreSQL", zap.Error(err))
	}
	defer pgStore.Close()

	// Initialize Redis queue
	redisQueue, err := queue.NewRedisQueue(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
	}
	defer redisQueue.Close()

	// Initialize metrics
	m := metrics.NewMetrics()

	// Initialize worker
	w := worker.New(worker.Config{
		Concurrency: cfg.WorkerConcurrency,
		Queue:       redisQueue,
		Storage:     pgStore,
		Metrics:     m,
		Logger:      logger,
	})

	// HTTP server for health checks
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := pgStore.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", m.Handler())

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.MetricsPort),
		Handler: mux,
	}

	go func() {
		logger.Info("Worker metrics server starting", zap.String("port", cfg.MetricsPort))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Failed to serve HTTP", zap.Error(err))
		}
	}()

	// Start worker
	go func() {
		logger.Info("Worker starting", zap.Int("concurrency", cfg.WorkerConcurrency))
		if err := w.Start(ctx); err != nil {
			logger.Fatal("Worker failed", zap.Error(err))
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down worker...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	w.Stop()
	cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("Failed to shutdown HTTP server", zap.Error(err))
	}

	logger.Info("Worker stopped")
}
