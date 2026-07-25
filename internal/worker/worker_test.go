package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DarkTheme404/distributed-task-scheduler/internal/metrics"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/storage"
	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type mockQueue struct {
	tasks   []*pb.Task
	mu      sync.Mutex
	ackeds  map[string]bool
	nacked  map[string]bool
	removed map[string]bool
}

func newMockQueue() *mockQueue {
	return &mockQueue{
		tasks:   make([]*pb.Task, 0),
		ackeds:  make(map[string]bool),
		nacked:  make(map[string]bool),
		removed: make(map[string]bool),
	}
}

func (q *mockQueue) Enqueue(ctx context.Context, task *pb.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
	return nil
}

func (q *mockQueue) Dequeue(ctx context.Context) (*pb.Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return nil, nil
	}
	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	return task, nil
}

func (q *mockQueue) Ack(ctx context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ackeds[taskID] = true
	return nil
}

func (q *mockQueue) Nack(ctx context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nacked[taskID] = true
	return nil
}

func (q *mockQueue) Remove(ctx context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.removed[taskID] = true
	return nil
}

func (q *mockQueue) EnqueueDeadLetter(ctx context.Context, task *pb.Task) error {
	return nil
}

func (q *mockQueue) EnqueueScheduled(ctx context.Context, task *pb.Task, scheduledAt time.Time) error {
	return nil
}

func (q *mockQueue) ProcessScheduled(ctx context.Context) error {
	return nil
}

func (q *mockQueue) Size(ctx context.Context) (int64, error) {
	return int64(len(q.tasks)), nil
}

func (q *mockQueue) DeadLetterSize(ctx context.Context) (int64, error) {
	return 0, nil
}

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

func TestWorkerProcessTask(t *testing.T) {
	queue := newMockQueue()
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	var processed int32
	handler := func(ctx context.Context, task *pb.Task) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	w := New(Config{
		Concurrency: 2,
		Queue:       queue,
		Storage:     store,
		Metrics:     m,
		Logger:      logger,
		Handler:     handler,
	})

	ctx, cancel := context.WithCancel(context.Background())

	task := &pb.Task{
		Id:         uuid.New().String(),
		Name:       "test-task",
		Type:       "email",
		MaxRetries: 3,
		Status:     pb.TaskStatus_TASK_STATUS_QUEUED,
	}

	queue.Enqueue(ctx, task)

	go w.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	cancel()
	w.Stop()

	if atomic.LoadInt32(&processed) != 1 {
		t.Errorf("Expected 1 task processed, got %d", processed)
	}
}

func TestWorkerRetryOnFailure(t *testing.T) {
	queue := newMockQueue()
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	var attempts int32
	handler := func(ctx context.Context, task *pb.Task) error {
		count := atomic.AddInt32(&attempts, 1)
		if count < 3 {
			return storage.ErrTaskNotFound
		}
		return nil
	}

	w := New(Config{
		Concurrency: 1,
		Queue:       queue,
		Storage:     store,
		Metrics:     m,
		Logger:      logger,
		Handler:     handler,
	})

	ctx, cancel := context.WithCancel(context.Background())

	task := &pb.Task{
		Id:         uuid.New().String(),
		Name:       "test-task",
		Type:       "email",
		MaxRetries: 3,
		Status:     pb.TaskStatus_TASK_STATUS_QUEUED,
	}

	queue.Enqueue(ctx, task)

	go w.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	cancel()
	w.Stop()

	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestWorkerDeadLetter(t *testing.T) {
	queue := newMockQueue()
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	handler := func(ctx context.Context, task *pb.Task) error {
		return storage.ErrTaskNotFound
	}

	w := New(Config{
		Concurrency: 1,
		Queue:       queue,
		Storage:     store,
		Metrics:     m,
		Logger:      logger,
		Handler:     handler,
	})

	ctx, cancel := context.WithCancel(context.Background())

	task := &pb.Task{
		Id:         uuid.New().String(),
		Name:       "test-task",
		Type:       "email",
		MaxRetries: 1,
		Status:     pb.TaskStatus_TASK_STATUS_QUEUED,
	}

	queue.Enqueue(ctx, task)

	go w.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	cancel()
	w.Stop()

	if m.DeadLetterCount.(prometheus.Counter).Desc() == nil {
		t.Error("Expected dead letter count to be incremented")
	}
}

func TestWorkerConcurrencyLimit(t *testing.T) {
	queue := newMockQueue()
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	var active int32
	var maxActive int32

	handler := func(ctx context.Context, task *pb.Task) error {
		current := atomic.AddInt32(&active, 1)
		for {
			old := atomic.LoadInt32(&maxActive)
			if current <= old || atomic.CompareAndSwapInt32(&maxActive, old, current) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return nil
	}

	w := New(Config{
		Concurrency: 2,
		Queue:       queue,
		Storage:     store,
		Metrics:     m,
		Logger:      logger,
		Handler:     handler,
	})

	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < 5; i++ {
		queue.Enqueue(ctx, &pb.Task{
			Id:         uuid.New().String(),
			Name:       "test-task",
			Type:       "email",
			MaxRetries: 3,
			Status:     pb.TaskStatus_TASK_STATUS_QUEUED,
		})
	}

	go w.Start(ctx)
	time.Sleep(500 * time.Millisecond)

	cancel()
	w.Stop()

	if atomic.LoadInt32(&maxActive) > 2 {
		t.Errorf("Expected max active <= 2, got %d", atomic.LoadInt32(&maxActive))
	}
}

func TestWorkerGracefulShutdown(t *testing.T) {
	queue := newMockQueue()
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	handler := func(ctx context.Context, task *pb.Task) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	w := New(Config{
		Concurrency: 2,
		Queue:       queue,
		Storage:     store,
		Metrics:     m,
		Logger:      logger,
		Handler:     handler,
	})

	ctx, cancel := context.WithCancel(context.Background())

	task := &pb.Task{
		Id:         uuid.New().String(),
		Name:       "test-task",
		Type:       "email",
		MaxRetries: 3,
		Status:     pb.TaskStatus_TASK_STATUS_QUEUED,
	}

	queue.Enqueue(ctx, task)

	go w.Start(ctx)
	time.Sleep(50 * time.Millisecond)

	w.Stop()
	cancel()
}

func TestWorkerEmptyQueue(t *testing.T) {
	queue := newMockQueue()
	store := newMockStore()
	m := newTestMetrics()
	logger := zap.NewNop()

	w := New(Config{
		Concurrency: 2,
		Queue:       queue,
		Storage:     store,
		Metrics:     m,
		Logger:      logger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	go w.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	cancel()
	w.Stop()
}
