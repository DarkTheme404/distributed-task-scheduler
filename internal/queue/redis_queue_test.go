package queue

import (
	"context"
	"testing"
	"time"

	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

func setupTestRedis(t *testing.T) (*RedisQueue, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	queue := &RedisQueue{
		client: client,
		logger: nil,
	}

	return queue, mr
}

func TestEnqueue(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()
	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Type:     "email",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	err := queue.Enqueue(ctx, task)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	size, err := queue.Size(ctx)
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}

	if size != 1 {
		t.Errorf("Expected queue size 1, got %d", size)
	}
}

func TestDequeue(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()
	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Type:     "email",
		Priority: pb.TaskPriority_TASK_PRIORITY_HIGH,
	}

	queue.Enqueue(ctx, task)

	dequeued, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if dequeued == nil {
		t.Fatal("Expected task, got nil")
	}

	if dequeued.Id != task.Id {
		t.Errorf("Expected task ID %s, got %s", task.Id, dequeued.Id)
	}
}

func TestDequeueEmpty(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()

	dequeued, err := queue.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if dequeued != nil {
		t.Errorf("Expected nil, got task %s", dequeued.Id)
	}
}

func TestAck(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()
	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Type:     "email",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	queue.Enqueue(ctx, task)
	queue.Dequeue(ctx)

	err := queue.Ack(ctx, task.Id)
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	size, _ := queue.Size(ctx)
	if size != 0 {
		t.Errorf("Expected queue size 0 after ack, got %d", size)
	}
}

func TestNack(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()
	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Type:     "email",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	queue.Enqueue(ctx, task)
	queue.Dequeue(ctx)

	err := queue.Nack(ctx, task.Id)
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}
}

func TestRemove(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()
	task := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "test-task",
		Type:     "email",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	}

	queue.Enqueue(ctx, task)

	err := queue.Remove(ctx, task.Id)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	size, _ := queue.Size(ctx)
	if size != 0 {
		t.Errorf("Expected queue size 0 after remove, got %d", size)
	}
}

func TestEnqueueDeadLetter(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()
	task := &pb.Task{
		Id:           uuid.New().String(),
		Name:         "test-task",
		Type:         "email",
		ErrorMessage: "test error",
	}

	err := queue.EnqueueDeadLetter(ctx, task)
	if err != nil {
		t.Fatalf("EnqueueDeadLetter failed: %v", err)
	}

	dlSize, err := queue.DeadLetterSize(ctx)
	if err != nil {
		t.Fatalf("DeadLetterSize failed: %v", err)
	}

	if dlSize != 1 {
		t.Errorf("Expected dead letter size 1, got %d", dlSize)
	}
}

func TestEnqueueScheduled(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()
	task := &pb.Task{
		Id:   uuid.New().String(),
		Name: "scheduled-task",
		Type: "email",
	}

	scheduledAt := time.Now().Add(1 * time.Hour)
	err := queue.EnqueueScheduled(ctx, task, scheduledAt)
	if err != nil {
		t.Fatalf("EnqueueScheduled failed: %v", err)
	}
}

func TestProcessScheduled(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()
	task := &pb.Task{
		Id:   uuid.New().String(),
		Name: "scheduled-task",
		Type: "email",
	}

	// Schedule for 1 hour ago
	scheduledAt := time.Now().Add(-1 * time.Hour)
	queue.EnqueueScheduled(ctx, task, scheduledAt)

	err := queue.ProcessScheduled(ctx)
	if err != nil {
		t.Fatalf("ProcessScheduled failed: %v", err)
	}

	// Task should have been moved to main queue
	size, _ := queue.Size(ctx)
	if size != 1 {
		t.Errorf("Expected queue size 1 after processing scheduled, got %d", size)
	}
}

func TestPriorityOrdering(t *testing.T) {
	queue, mr := setupTestRedis(t)
	defer mr.Close()
	defer queue.Close()

	ctx := context.Background()

	lowTask := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "low-priority",
		Type:     "email",
		Priority: pb.TaskPriority_TASK_PRIORITY_LOW,
	}

	highTask := &pb.Task{
		Id:       uuid.New().String(),
		Name:     "high-priority",
		Type:     "email",
		Priority: pb.TaskPriority_TASK_PRIORITY_HIGH,
	}

	// Enqueue low first
	queue.Enqueue(ctx, lowTask)
	time.Sleep(10 * time.Millisecond)
	queue.Enqueue(ctx, highTask)

	// High priority should come first
	dequeued, _ := queue.Dequeue(ctx)
	if dequeued.Name != "high-priority" {
		t.Errorf("Expected high-priority task first, got %s", dequeued.Name)
	}
}
