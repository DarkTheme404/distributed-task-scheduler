package storage

import (
	"context"
	"testing"

	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupTestDB(t *testing.T) (*PostgresStore, func()) {
	t.Helper()

	// Use a test database URL or skip if not available
	dsn := "postgres://postgres:postgres@localhost:5432/scheduler_test?sslmode=disable"

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Skip("PostgreSQL not available, skipping test")
	}

	// Clean up test database
	pool.Exec(context.Background(), "DROP TABLE IF EXISTS tasks")
	pool.Close()

	store, err := NewPostgresStore(context.Background(), dsn)
	if err != nil {
		t.Skip("PostgreSQL not available, skipping test")
	}

	cleanup := func() {
		store.Close()
	}

	return store, cleanup
}

func TestCreateTask(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	task := &pb.Task{
		Id:          uuid.New().String(),
		Name:        "test-task",
		Type:        "email",
		Priority:    pb.TaskPriority_TASK_PRIORITY_NORMAL,
		Status:      pb.TaskStatus_TASK_STATUS_PENDING,
		MaxRetries:  3,
		CreatedAt:   timestamppb.Now(),
		UpdatedAt:   timestamppb.Now(),
		ScheduledAt: timestamppb.Now(),
	}

	err := store.CreateTask(ctx, task)
	if err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	retrieved, err := store.GetTask(ctx, task.Id)
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}

	if retrieved.Id != task.Id {
		t.Errorf("Expected ID %s, got %s", task.Id, retrieved.Id)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	_, err := store.GetTask(ctx, uuid.New().String())
	if err != ErrTaskNotFound {
		t.Errorf("Expected ErrTaskNotFound, got %v", err)
	}
}

func TestUpdateTask(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	task := &pb.Task{
		Id:          uuid.New().String(),
		Name:        "test-task",
		Type:        "email",
		Priority:    pb.TaskPriority_TASK_PRIORITY_NORMAL,
		Status:      pb.TaskStatus_TASK_STATUS_PENDING,
		MaxRetries:  3,
		CreatedAt:   timestamppb.Now(),
		UpdatedAt:   timestamppb.Now(),
		ScheduledAt: timestamppb.Now(),
	}

	store.CreateTask(ctx, task)

	task.Status = pb.TaskStatus_TASK_STATUS_RUNNING
	task.UpdatedAt = timestamppb.Now()

	err := store.UpdateTask(ctx, task)
	if err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}

	retrieved, _ := store.GetTask(ctx, task.Id)
	if retrieved.Status != pb.TaskStatus_TASK_STATUS_RUNNING {
		t.Errorf("Expected status RUNNING, got %s", retrieved.Status)
	}
}

func TestDeleteTask(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	task := &pb.Task{
		Id:          uuid.New().String(),
		Name:        "test-task",
		Type:        "email",
		Priority:    pb.TaskPriority_TASK_PRIORITY_NORMAL,
		Status:      pb.TaskStatus_TASK_STATUS_PENDING,
		MaxRetries:  3,
		CreatedAt:   timestamppb.Now(),
		UpdatedAt:   timestamppb.Now(),
		ScheduledAt: timestamppb.Now(),
	}

	store.CreateTask(ctx, task)

	err := store.DeleteTask(ctx, task.Id)
	if err != nil {
		t.Fatalf("DeleteTask failed: %v", err)
	}

	_, err = store.GetTask(ctx, task.Id)
	if err != ErrTaskNotFound {
		t.Errorf("Expected ErrTaskNotFound after delete, got %v", err)
	}
}

func TestDeleteTaskNotFound(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	err := store.DeleteTask(ctx, uuid.New().String())
	if err != ErrTaskNotFound {
		t.Errorf("Expected ErrTaskNotFound, got %v", err)
	}
}

func TestListTasks(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		task := &pb.Task{
			Id:          uuid.New().String(),
			Name:        "test-task",
			Type:        "email",
			Priority:    pb.TaskPriority_TASK_PRIORITY_NORMAL,
			Status:      pb.TaskStatus_TASK_STATUS_PENDING,
			MaxRetries:  3,
			CreatedAt:   timestamppb.Now(),
			UpdatedAt:   timestamppb.Now(),
			ScheduledAt: timestamppb.Now(),
		}
		store.CreateTask(ctx, task)
	}

	tasks, _, err := store.ListTasks(ctx, pb.TaskStatus_TASK_STATUS_UNSPECIFIED, 10, "")
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}

	if len(tasks) != 5 {
		t.Errorf("Expected 5 tasks, got %d", len(tasks))
	}
}

func TestListTasksWithStatusFilter(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		task := &pb.Task{
			Id:          uuid.New().String(),
			Name:        "pending-task",
			Type:        "email",
			Priority:    pb.TaskPriority_TASK_PRIORITY_NORMAL,
			Status:      pb.TaskStatus_TASK_STATUS_PENDING,
			MaxRetries:  3,
			CreatedAt:   timestamppb.Now(),
			UpdatedAt:   timestamppb.Now(),
			ScheduledAt: timestamppb.Now(),
		}
		store.CreateTask(ctx, task)
	}

	for i := 0; i < 2; i++ {
		task := &pb.Task{
			Id:          uuid.New().String(),
			Name:        "completed-task",
			Type:        "email",
			Priority:    pb.TaskPriority_TASK_PRIORITY_NORMAL,
			Status:      pb.TaskStatus_TASK_STATUS_COMPLETED,
			MaxRetries:  3,
			CreatedAt:   timestamppb.Now(),
			UpdatedAt:   timestamppb.Now(),
			ScheduledAt: timestamppb.Now(),
		}
		store.CreateTask(ctx, task)
	}

	tasks, _, err := store.ListTasks(ctx, pb.TaskStatus_TASK_STATUS_PENDING, 10, "")
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}

	if len(tasks) != 3 {
		t.Errorf("Expected 3 pending tasks, got %d", len(tasks))
	}
}

func TestPing(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	err := store.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}
