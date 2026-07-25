package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DarkTheme404/distributed-task-scheduler/internal/metrics"
	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

const (
	defaultQueueKey     = "scheduler:queue"
	deadLetterKey       = "scheduler:deadletter"
	scheduledKey        = "scheduler:scheduled"
	priorityQueueKey    = "scheduler:priority"
	lockKeyPrefix       = "scheduler:lock:"
	defaultLockTTL      = 30 * time.Second
	defaultRetryBackoff = 5 * time.Second
)

// Queue defines the interface for task queue operations.
type Queue interface {
	Enqueue(ctx context.Context, task *pb.Task) error
	Dequeue(ctx context.Context) (*pb.Task, error)
	Ack(ctx context.Context, taskID string) error
	Nack(ctx context.Context, taskID string) error
	Remove(ctx context.Context, taskID string) error
	EnqueueDeadLetter(ctx context.Context, task *pb.Task) error
	EnqueueScheduled(ctx context.Context, task *pb.Task, scheduledAt time.Time) error
	ProcessScheduled(ctx context.Context) error
	Size(ctx context.Context) (int64, error)
	DeadLetterSize(ctx context.Context) (int64, error)
}

// RedisQueue implements Queue using Redis sorted sets.
type RedisQueue struct {
	client  *redis.Client
	metrics *metrics.Metrics
	logger  *zap.Logger
}

// NewRedisQueue creates a new Redis-backed queue.
func NewRedisQueue(addr, password string, db int) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     20,
		MinIdleConns: 5,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisQueue{
		client:  client,
		metrics: nil,
		logger:  zap.NewNop(),
	}, nil
}

// SetMetrics sets the metrics collector for the queue.
func (q *RedisQueue) SetMetrics(m *metrics.Metrics) {
	q.metrics = m
}

// SetLogger sets the logger for the queue.
func (q *RedisQueue) SetLogger(logger *zap.Logger) {
	q.logger = logger
}

// Enqueue adds a task to the priority queue sorted by priority and timestamp.
func (q *RedisQueue) Enqueue(ctx context.Context, task *pb.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	score := q.calculateScore(task)
	pipe := q.client.Pipeline()
	pipe.ZAdd(ctx, priorityQueueKey, &redis.Z{
		Score:  score,
		Member: string(data),
	})
	pipe.ZAdd(ctx, defaultQueueKey, &redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: task.Id,
	})

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	if q.metrics != nil {
		q.metrics.QueueDepth.Inc()
	}

	q.logger.Debug("Task enqueued",
		zap.String("task_id", task.Id),
		zap.Float64("score", score),
	)

	return nil
}

// Dequeue retrieves and locks the highest-priority task.
func (q *RedisQueue) Dequeue(ctx context.Context) (*pb.Task, error) {
	// Try priority queue first
	result, err := q.client.ZPopMax(ctx, priorityQueueKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to dequeue task: %w", err)
	}

	if len(result) == 0 {
		return nil, nil
	}

	var task pb.Task
	if err := json.Unmarshal([]byte(result[0].Member.(string)), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	// Try to acquire lock
	lockKey := lockKeyPrefix + task.Id
	locked, err := q.client.SetNX(ctx, lockKey, "1", defaultLockTTL).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	if !locked {
		// Task is being processed, try next
		return q.Dequeue(ctx)
	}

	if q.metrics != nil {
		q.metrics.QueueDepth.Dec()
	}

	return &task, nil
}

// Ack acknowledges successful task completion and removes the lock.
func (q *RedisQueue) Ack(ctx context.Context, taskID string) error {
	lockKey := lockKeyPrefix + taskID
	pipe := q.client.Pipeline()
	pipe.Del(ctx, lockKey)
	pipe.ZRem(ctx, defaultQueueKey, taskID)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to ack task: %w", err)
	}

	return nil
}

// Nack negatively acknowledges a task, re-enqueueing it with backoff.
func (q *RedisQueue) Nack(ctx context.Context, taskID string) error {
	lockKey := lockKeyPrefix + taskID
	pipe := q.client.Pipeline()
	pipe.Del(ctx, lockKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to nack task: %w", err)
	}

	return nil
}

// Remove removes a task from all queues.
func (q *RedisQueue) Remove(ctx context.Context, taskID string) error {
	lockKey := lockKeyPrefix + taskID
	pipe := q.client.Pipeline()
	pipe.Del(ctx, lockKey)
	pipe.ZRem(ctx, defaultQueueKey, taskID)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to remove task: %w", err)
	}

	return nil
}

// EnqueueDeadLetter moves a task to the dead letter queue.
func (q *RedisQueue) EnqueueDeadLetter(ctx context.Context, task *pb.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	pipe := q.client.Pipeline()
	pipe.ZAdd(ctx, deadLetterKey, &redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: string(data),
	})
	pipe.Del(ctx, lockKeyPrefix+task.Id)
	pipe.ZRem(ctx, defaultQueueKey, task.Id)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to enqueue dead letter: %w", err)
	}

	if q.metrics != nil {
		q.metrics.DeadLetterCount.Inc()
	}

	q.logger.Warn("Task moved to dead letter",
		zap.String("task_id", task.Id),
		zap.String("error", task.ErrorMessage),
	)

	return nil
}

// EnqueueScheduled adds a task to the scheduled queue for future execution.
func (q *RedisQueue) EnqueueScheduled(ctx context.Context, task *pb.Task, scheduledAt time.Time) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	err = q.client.ZAdd(ctx, scheduledKey, &redis.Z{
		Score:  float64(scheduledAt.Unix()),
		Member: string(data),
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to enqueue scheduled task: %w", err)
	}

	return nil
}

// ProcessScheduled checks for scheduled tasks that are ready to run.
func (q *RedisQueue) ProcessScheduled(ctx context.Context) error {
	now := time.Now().Unix()

	results, err := q.client.ZRangeByScore(ctx, scheduledKey, &redis.ZRangeBy{
		Min:   "0",
		Max:   fmt.Sprintf("%d", now),
		Count: 100,
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to get scheduled tasks: %w", err)
	}

	for _, data := range results {
		var task pb.Task
		if err := json.Unmarshal([]byte(data), &task); err != nil {
			q.logger.Error("Failed to unmarshal scheduled task", zap.Error(err))
			continue
		}

		// Remove from scheduled queue
		q.client.ZRem(ctx, scheduledKey, data)

		// Enqueue for immediate execution
		if err := q.Enqueue(ctx, &task); err != nil {
			q.logger.Error("Failed to enqueue scheduled task", zap.Error(err), zap.String("task_id", task.Id))
		}
	}

	return nil
}

// Size returns the number of tasks in the queue.
func (q *RedisQueue) Size(ctx context.Context) (int64, error) {
	return q.client.ZCard(ctx, defaultQueueKey).Result()
}

// DeadLetterSize returns the number of tasks in the dead letter queue.
func (q *RedisQueue) DeadLetterSize(ctx context.Context) (int64, error) {
	return q.client.ZCard(ctx, deadLetterKey).Result()
}

// calculateScore computes the priority score for a task.
// Higher score = higher priority.
func (q *RedisQueue) calculateScore(task *pb.Task) float64 {
	priorityScore := float64(task.Priority) * 1000000
	timestampScore := float64(time.Now().UnixMilli()) / 1000000.0
	return priorityScore + timestampScore
}

// Close closes the Redis client connection.
func (q *RedisQueue) Close() error {
	return q.client.Close()
}
