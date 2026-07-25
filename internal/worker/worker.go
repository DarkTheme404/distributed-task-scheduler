package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DarkTheme404/distributed-task-scheduler/internal/metrics"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/queue"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/storage"
	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxRetryAttempts  = 3
	retryBaseDelay    = 5 * time.Second
	retryMaxDelay     = 5 * time.Minute
	taskTimeout       = 5 * time.Minute
	pollInterval      = 1 * time.Second
)

// TaskHandler processes a task and returns an error if it fails.
type TaskHandler func(ctx context.Context, task *pb.Task) error

// Config holds worker configuration.
type Config struct {
	Concurrency int
	Queue       queue.Queue
	Storage     storage.Store
	Metrics     *metrics.Metrics
	Logger      *zap.Logger
	Handler     TaskHandler
}

// Worker processes tasks from the queue with concurrency control.
type Worker struct {
	config    Config
	semaphore chan struct{}
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	stopped   bool
	mu        sync.Mutex
}

// New creates a new Worker with the given configuration.
func New(config Config) *Worker {
	if config.Concurrency <= 0 {
		config.Concurrency = 5
	}
	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}
	if config.Handler == nil {
		config.Handler = defaultHandler
	}

	return &Worker{
		config:    config,
		semaphore: make(chan struct{}, config.Concurrency),
	}
}

// Start begins processing tasks from the queue.
func (w *Worker) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	w.config.Logger.Info("Worker started",
		zap.Int("concurrency", w.config.Concurrency),
	)

	// Start scheduled task processor
	go w.processScheduledTasks(ctx)

	// Start main processing loop
	for {
		select {
		case <-ctx.Done():
			w.config.Logger.Info("Worker shutting down")
			w.wg.Wait()
			return nil
		default:
			if err := w.processNext(ctx); err != nil {
				w.config.Logger.Error("Error processing task", zap.Error(err))
				time.Sleep(pollInterval)
			}
		}
	}
}

// Stop gracefully stops the worker.
func (w *Worker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopped {
		return
	}
	w.stopped = true

	if w.cancel != nil {
		w.cancel()
	}

	w.config.Logger.Info("Worker stopping, waiting for active tasks...")
	w.wg.Wait()
	w.config.Logger.Info("Worker stopped")
}

// processNext attempts to dequeue and process one task.
func (w *Worker) processNext(ctx context.Context) error {
	// Check if we can accept more tasks
	select {
	case w.semaphore <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	default:
		// At capacity, wait
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	task, err := w.config.Queue.Dequeue(ctx)
	if err != nil {
		<-w.semaphore
		return fmt.Errorf("dequeue failed: %w", err)
	}

	if task == nil {
		<-w.semaphore
		time.Sleep(pollInterval)
		return nil
	}

	w.wg.Add(1)
	go w.executeTask(ctx, task)

	return nil
}

// executeTask runs a task with retry logic and semaphore control.
func (w *Worker) executeTask(ctx context.Context, task *pb.Task) {
	defer w.wg.Done()
	defer func() { <-w.semaphore }()

	w.config.Logger.Info("Processing task",
		zap.String("task_id", task.Id),
		zap.String("name", task.Name),
		zap.String("type", task.Type),
		zap.Int32("retry_count", task.RetryCount),
	)

	// Update task status to running
	task.Status = pb.TaskStatus_TASK_STATUS_RUNNING
	task.UpdatedAt = timestamppb.New(time.Now())
	w.config.Storage.UpdateTask(ctx, task)
	w.config.Metrics.WorkerActive.Inc()
	w.config.Metrics.WorkerIdle.Dec()

	startTime := time.Now()

	// Create timeout context
	taskCtx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	// Execute the task
	err := w.config.Handler(taskCtx, task)

	duration := time.Since(startTime).Seconds()
	w.config.Metrics.TaskDuration.Observe(duration)
	w.config.Metrics.WorkerActive.Dec()
	w.config.Metrics.WorkerIdle.Inc()

	if err != nil {
		w.handleTaskError(ctx, task, err)
	} else {
		w.handleTaskSuccess(ctx, task)
	}
}

// handleTaskSuccess processes a successfully completed task.
func (w *Worker) handleTaskSuccess(ctx context.Context, task *pb.Task) {
	task.Status = pb.TaskStatus_TASK_STATUS_COMPLETED
	task.UpdatedAt = timestamppb.New(time.Now())
	task.CompletedAt = timestamppb.New(time.Now())

	if err := w.config.Storage.UpdateTask(ctx, task); err != nil {
		w.config.Logger.Error("Failed to update completed task", zap.Error(err), zap.String("task_id", task.Id))
	}

	if err := w.config.Queue.Ack(ctx, task.Id); err != nil {
		w.config.Logger.Error("Failed to ack task", zap.Error(err), zap.String("task_id", task.Id))
	}

	w.config.Metrics.TasksCompleted.Inc()
	w.config.Logger.Info("Task completed",
		zap.String("task_id", task.Id),
		zap.String("name", task.Name),
	)
}

// handleTaskError processes a failed task with retry logic.
func (w *Worker) handleTaskError(ctx context.Context, task *pb.Task, err error) {
	task.RetryCount++
	task.ErrorMessage = err.Error()
	task.UpdatedAt = timestamppb.New(time.Now())

	w.config.Logger.Error("Task failed",
		zap.String("task_id", task.Id),
		zap.String("name", task.Name),
		zap.Int32("retry_count", task.RetryCount),
		zap.Error(err),
	)

	if task.RetryCount >= task.MaxRetries {
		// Move to dead letter queue
		w.handleDeadLetter(ctx, task)
		return
	}

	// Calculate exponential backoff with jitter
	delay := w.calculateBackoff(task.RetryCount)

	// Re-enqueue with delay
	w.config.Logger.Info("Retrying task",
		zap.String("task_id", task.Id),
		zap.Int32("retry_count", task.RetryCount),
		zap.Duration("delay", delay),
	)

	task.Status = pb.TaskStatus_TASK_STATUS_RETRYING
	if err := w.config.Storage.UpdateTask(ctx, task); err != nil {
		w.config.Logger.Error("Failed to update retrying task", zap.Error(err), zap.String("task_id", task.Id))
	}

	scheduledAt := time.Now().Add(delay)
	if err := w.config.Queue.EnqueueScheduled(ctx, task, scheduledAt); err != nil {
		w.config.Logger.Error("Failed to enqueue scheduled retry", zap.Error(err), zap.String("task_id", task.Id))
		// Fallback to dead letter
		w.handleDeadLetter(ctx, task)
	}

	w.config.Metrics.RetryAttempts.Inc()

	// Ack the original dequeue
	if err := w.config.Queue.Ack(ctx, task.Id); err != nil {
		w.config.Logger.Error("Failed to ack retried task", zap.Error(err), zap.String("task_id", task.Id))
	}
}

// handleDeadLetter moves a task to the dead letter queue.
func (w *Worker) handleDeadLetter(ctx context.Context, task *pb.Task) {
	task.Status = pb.TaskStatus_TASK_STATUS_FAILED
	task.UpdatedAt = timestamppb.New(time.Now())

	if err := w.config.Storage.UpdateTask(ctx, task); err != nil {
		w.config.Logger.Error("Failed to update dead-lettered task", zap.Error(err), zap.String("task_id", task.Id))
	}

	if err := w.config.Queue.EnqueueDeadLetter(ctx, task); err != nil {
		w.config.Logger.Error("Failed to enqueue dead letter", zap.Error(err), zap.String("task_id", task.Id))
	}

	w.config.Metrics.DeadLetterCount.Inc()
	w.config.Logger.Warn("Task moved to dead letter",
		zap.String("task_id", task.Id),
		zap.String("error", task.ErrorMessage),
	)
}

// calculateBackoff computes exponential backoff with jitter.
func (w *Worker) calculateBackoff(retryCount int32) time.Duration {
	delay := retryBaseDelay
	for i := int32(0); i < retryCount; i++ {
		delay *= 2
	}

	if delay > retryMaxDelay {
		delay = retryMaxDelay
	}

	return delay
}

// processScheduledTasks periodically checks for scheduled tasks ready to run.
func (w *Worker) processScheduledTasks(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.config.Queue.ProcessScheduled(ctx); err != nil {
				w.config.Logger.Error("Failed to process scheduled tasks", zap.Error(err))
			}
		}
	}
}

// defaultHandler is the default task handler that does nothing.
func defaultHandler(ctx context.Context, task *pb.Task) error {
	return nil
}
