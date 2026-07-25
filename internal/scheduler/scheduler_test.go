package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/DarkTheme404/distributed-task-scheduler/internal/metrics"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/storage"
	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockStore struct {
	tasks map[string]*pb.Task
}

func newMockStore() *mockStore {
	return &mockStore{tasks: make(map[string]*pb.Task)}
}

func (m *mockStore) CreateTask(ctx context.Context, task *pb.Task) error {
	m.tasks[task.Id] = task
	return nil
}

func (m *mockStore) GetTask(ctx context.Context, id string) (*pb.Task, error) {
	task, ok := m.tasks[id]
	if !ok {
		return nil, storage.ErrTaskNotFound
	}
	return task, nil
}

func (m *mockStore) UpdateTask(ctx context.Context, task *pb.Task) error {
	m.tasks[task.Id] = task
	return nil
}

func (m *mockStore) DeleteTask(ctx context.Context, id string) error {
	delete(m.tasks, id)
	return nil
}

func (m *mockStore) ListTasks(ctx context.Context, status pb.TaskStatus, limit int, offset string) ([]*pb.Task, string, error) {
	var result []*pb.Task
	for _, task := range m.tasks {
		if status == pb.TaskStatus_TASK_STATUS_UNSPECIFIED || task.Status == status {
			result = append(result, task)
		}
	}
	return result, "", nil
}

func (m *mockStore) Ping(ctx context.Context) error {
	return nil
}

func (m *mockStore) Close() error {
	return nil
}

func newTestMetrics() *metrics.Metrics {
	return &metrics.Metrics{
		TasksSubmitted:   prometheus.NewCounter(prometheus.CounterOpts{}),
		TasksCompleted:   prometheus.NewCounter(prometheus.CounterOpts{}),
		TasksFailed:      prometheus.NewCounter(prometheus.CounterOpts{}),
		TasksCancelled:   prometheus.NewCounter(prometheus.CounterOpts{}),
		TasksByStatus:    prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"status"}),
		TasksByType:      prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"type"}),
		TasksByPriority:  prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"priority"}),
		DAGsSubmitted:    prometheus.NewCounter(prometheus.CounterOpts{}),
		DAGsCompleted:    prometheus.NewCounter(prometheus.CounterOpts{}),
		QueueDepth:       prometheus.NewGauge(prometheus.GaugeOpts{}),
		WorkerActive:     prometheus.NewGauge(prometheus.GaugeOpts{}),
		WorkerIdle:       prometheus.NewGauge(prometheus.GaugeOpts{}),
		TaskDuration:     prometheus.NewHistogram(prometheus.HistogramOpts{}),
		RetryAttempts:    prometheus.NewCounter(prometheus.CounterOpts{}),
		DeadLetterCount:  prometheus.NewCounter(prometheus.CounterOpts{}),
	}
}

func TestSubmitTask(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	task, err := s.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		Name:     "test-task",
		Type:     "email",
		Priority: pb.TaskPriority_TASK_PRIORITY_NORMAL,
	})

	if err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}

	if task.Id == "" {
		t.Fatal("Expected task ID to be set")
	}
	if task.Name != "test-task" {
		t.Errorf("Expected name 'test-task', got %s", task.Name)
	}
	if task.Status != pb.TaskStatus_TASK_STATUS_PENDING {
		t.Errorf("Expected status PENDING, got %s", task.Status)
	}
}

func TestSubmitTaskInvalidName(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	_, err := s.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		Type: "email",
	})

	if err == nil {
		t.Fatal("Expected error for empty name")
	}
}

func TestGetTask(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	task, _ := s.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		Name: "test-task",
		Type: "email",
	})

	retrieved, err := s.GetTask(context.Background(), task.Id)
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}

	if retrieved.Id != task.Id {
		t.Errorf("Expected ID %s, got %s", task.Id, retrieved.Id)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	_, err := s.GetTask(context.Background(), uuid.New().String())
	if err == nil {
		t.Fatal("Expected error for non-existent task")
	}
}

func TestCancelTask(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	task, _ := s.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
		Name: "test-task",
		Type: "email",
	})

	err := s.CancelTask(context.Background(), task.Id)
	if err != nil {
		t.Fatalf("CancelTask failed: %v", err)
	}

	retrieved, _ := s.GetTask(context.Background(), task.Id)
	if retrieved.Status != pb.TaskStatus_TASK_STATUS_CANCELLED {
		t.Errorf("Expected status CANCELLED, got %s", retrieved.Status)
	}
}

func TestSubmitDAG(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	dag, err := s.SubmitDAG(context.Background(), &pb.SubmitDAGRequest{
		Name: "test-dag",
		Nodes: []*pb.DAGDefinitionNode{
			{Name: "task1", Type: "email"},
			{Name: "task2", Type: "sms"},
			{Name: "task3", Type: "push"},
		},
		Edges: []*pb.DAGDefinitionEdge{
			{From: "task1", To: "task2"},
			{From: "task2", To: "task3"},
		},
	})

	if err != nil {
		t.Fatalf("SubmitDAG failed: %v", err)
	}

	if len(dag.Tasks) != 3 {
		t.Errorf("Expected 3 tasks, got %d", len(dag.Tasks))
	}
}

func TestValidateDAGCycleDetection(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	dag := &pb.DAG{
		Tasks: map[string]*pb.Task{
			"a": {Name: "a", Dependencies: []string{"c"}},
			"b": {Name: "b", Dependencies: []string{"a"}},
			"c": {Name: "c", Dependencies: []string{"b"}},
		},
	}

	err := s.validateDAG(dag)
	if err == nil {
		t.Fatal("Expected cycle detection error")
	}
}

func TestListTasks(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	for i := 0; i < 5; i++ {
		s.SubmitTask(context.Background(), &pb.SubmitTaskRequest{
			Name: "task",
			Type: "email",
		})
	}

	tasks, _, err := s.ListTasks(context.Background(), pb.TaskStatus_TASK_STATUS_UNSPECIFIED, 10, "")
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}

	if len(tasks) != 5 {
		t.Errorf("Expected 5 tasks, got %d", len(tasks))
	}
}

func TestHandleTaskCompletion(t *testing.T) {
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	s := New(store, m, logger)

	task := &pb.Task{
		Id:     uuid.New().String(),
		Name:   "test",
		Type:   "email",
		Status: pb.TaskStatus_TASK_STATUS_RUNNING,
	}

	store.CreateTask(context.Background(), task)

	completedTask := &pb.Task{
		Id:        task.Id,
		Name:      task.Name,
		Type:      task.Type,
		Status:    pb.TaskStatus_TASK_STATUS_COMPLETED,
		UpdatedAt: timestamppb.New(time.Now()),
	}

	err := s.HandleTaskCompletion(context.Background(), completedTask)
	if err != nil {
		t.Fatalf("HandleTaskCompletion failed: %v", err)
	}
}
