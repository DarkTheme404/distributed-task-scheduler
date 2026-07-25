package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DarkTheme404/distributed-task-scheduler/internal/metrics"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/queue"
	"github.com/DarkTheme404/distributed-task-scheduler/internal/storage"
	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Scheduler manages task submission, DAG resolution, and execution orchestration.
type Scheduler struct {
	storage  storage.Store
	queue    queue.Queue
	metrics  *metrics.Metrics
	logger   *zap.Logger
	eventCh  chan *pb.TaskEvent
	mu       sync.RWMutex
	dags     map[string]*pb.DAG
	tasks    map[string]*pb.Task
}

// New creates a new Scheduler instance.
func New(storage storage.Store, m *metrics.Metrics, logger *zap.Logger) *Scheduler {
	return &Scheduler{
		storage: storage,
		queue:   nil,
		metrics: m,
		logger:  logger,
		eventCh: make(chan *pb.TaskEvent, 1000),
		dags:    make(map[string]*pb.DAG),
		tasks:   make(map[string]*pb.Task),
	}
}

// SetQueue sets the queue backend for the scheduler.
func (s *Scheduler) SetQueue(q queue.Queue) {
	s.queue = q
}

// SubmitTask creates and queues a new task.
func (s *Scheduler) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.Task, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "task name is required")
	}
	if req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "task type is required")
	}

	maxRetries := req.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	now := timestamppb.New(time.Now())
	task := &pb.Task{
		Id:          uuid.New().String(),
		Name:        req.Name,
		Type:        req.Type,
		Payload:     req.Payload,
		Priority:    req.Priority,
		Status:      pb.TaskStatus_TASK_STATUS_PENDING,
		MaxRetries:  maxRetries,
		RetryCount:  0,
		CreatedAt:   now,
		UpdatedAt:   now,
		ScheduledAt: req.ScheduledAt,
	}

	if task.ScheduledAt == nil {
		task.ScheduledAt = now
	}

	// Store in database
	if err := s.storage.CreateTask(ctx, task); err != nil {
		s.logger.Error("Failed to store task", zap.Error(err), zap.String("task_id", task.Id))
		return nil, status.Error(codes.Internal, "failed to create task")
	}

	// Queue for execution
	if s.queue != nil {
		if err := s.queue.Enqueue(ctx, task); err != nil {
			s.logger.Error("Failed to enqueue task", zap.Error(err), zap.String("task_id", task.Id))
			return nil, status.Error(codes.Internal, "failed to queue task")
		}
	}

	s.metrics.TasksSubmitted.Inc()
	s.metrics.TasksByStatus.WithLabelValues("pending").Inc()
	s.metrics.TasksByType.WithLabelValues(task.Type).Inc()
	s.metrics.TasksByPriority.WithLabelValues(task.Priority.String()).Inc()

	s.logger.Info("Task submitted",
		zap.String("task_id", task.Id),
		zap.String("name", task.Name),
		zap.String("type", task.Type),
	)

	s.emitEvent(&pb.TaskEvent{
		TaskId:    task.Id,
		Status:    task.Status,
		Timestamp: now,
	})

	return task, nil
}

// SubmitDAG creates a DAG with dependency resolution.
func (s *Scheduler) SubmitDAG(ctx context.Context, req *pb.SubmitDAGRequest) (*pb.DAG, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "DAG name is required")
	}
	if len(req.Nodes) == 0 {
		return nil, status.Error(codes.InvalidArgument, "DAG must have at least one node")
	}

	now := timestamppb.New(time.Now())
	dag := &pb.DAG{
		Id:        uuid.New().String(),
		Name:      req.Name,
		Tasks:     make(map[string]*pb.Task),
		Edges:     make(map[string]*pb.DAG_EdgesList),
		Status:    pb.TaskStatus_TASK_STATUS_PENDING,
		CreatedAt: now,
	}

	// Create tasks from nodes
	for _, node := range req.Nodes {
		maxRetries := node.MaxRetries
		if maxRetries == 0 {
			maxRetries = 3
		}

		task := &pb.Task{
			Id:          uuid.New().String(),
			Name:        node.Name,
			Type:        node.Type,
			Payload:     node.Payload,
			Priority:    node.Priority,
			Status:      pb.TaskStatus_TASK_STATUS_PENDING,
			MaxRetries:  maxRetries,
			CreatedAt:   now,
			UpdatedAt:   now,
			ParentDagId: dag.Id,
		}

		dag.Tasks[node.Name] = task

		if err := s.storage.CreateTask(ctx, task); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create task %s", node.Name))
		}
	}

	// Validate edges and create dependencies
	for _, edge := range req.Edges {
		if _, ok := dag.Tasks[edge.From]; !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown source node: %s", edge.From)
		}
		if _, ok := dag.Tasks[edge.To]; !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown target node: %s", edge.To)
		}

		dag.Edges[edge.From] = &pb.DAG_EdgesList{
			Targets: append(dag.Edges[edge.From].GetTargets(), edge.To),
		}

		dag.Tasks[edge.To].Dependencies = append(dag.Tasks[edge.To].Dependencies, edge.From)
	}

	// Validate no cycles
	if err := s.validateDAG(dag); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	s.mu.Lock()
	s.dags[dag.Id] = dag
	s.mu.Unlock()

	// Enqueue root tasks (no dependencies)
	for _, task := range dag.Tasks {
		if len(task.Dependencies) == 0 && s.queue != nil {
			if err := s.queue.Enqueue(ctx, task); err != nil {
				s.logger.Error("Failed to enqueue DAG root task", zap.Error(err), zap.String("task_id", task.Id))
			}
		}
	}

	s.metrics.DAGsSubmitted.Inc()

	s.logger.Info("DAG submitted",
		zap.String("dag_id", dag.Id),
		zap.String("name", dag.Name),
		zap.Int("tasks", len(dag.Tasks)),
	)

	return dag, nil
}

// validateDAG checks for cycles in the DAG using topological sort.
func (s *Scheduler) validateDAG(dag *pb.DAG) error {
	inDegree := make(map[string]int)
	for name := range dag.Tasks {
		inDegree[name] = 0
	}

	for _, task := range dag.Tasks {
		for _, dep := range task.Dependencies {
			inDegree[task.Name]++
			_ = dep
		}
	}

	queue := make([]string, 0)
	for name, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, name)
		}
	}

	visited := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		visited++

		if edges, ok := dag.Edges[current]; ok {
			for _, target := range edges.Targets {
				inDegree[target]--
				if inDegree[target] == 0 {
					queue = append(queue, target)
				}
			}
		}
	}

	if visited != len(dag.Tasks) {
		return fmt.Errorf("DAG contains a cycle")
	}

	return nil
}

// GetTask retrieves a task by ID.
func (s *Scheduler) GetTask(ctx context.Context, taskID string) (*pb.Task, error) {
	task, err := s.storage.GetTask(ctx, taskID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}
	return task, nil
}

// GetDAG retrieves a DAG by ID.
func (s *Scheduler) GetDAG(ctx context.Context, dagID string) (*pb.DAG, error) {
	s.mu.RLock()
	dag, ok := s.dags[dagID]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Error(codes.NotFound, "DAG not found")
	}

	return dag, nil
}

// CancelTask cancels a pending or running task.
func (s *Scheduler) CancelTask(ctx context.Context, taskID string) error {
	task, err := s.storage.GetTask(ctx, taskID)
	if err != nil {
		return status.Error(codes.NotFound, "task not found")
	}

	if task.Status != pb.TaskStatus_TASK_STATUS_PENDING &&
		task.Status != pb.TaskStatus_TASK_STATUS_QUEUED &&
		task.Status != pb.TaskStatus_TASK_STATUS_RUNNING {
		return status.Error(codes.FailedPrecondition, "task cannot be cancelled in current state")
	}

	task.Status = pb.TaskStatus_TASK_STATUS_CANCELLED
	task.UpdatedAt = timestamppb.New(time.Now())

	if err := s.storage.UpdateTask(ctx, task); err != nil {
		return status.Error(codes.Internal, "failed to cancel task")
	}

	if s.queue != nil {
		if err := s.queue.Remove(ctx, taskID); err != nil {
			s.logger.Warn("Failed to remove task from queue", zap.Error(err), zap.String("task_id", taskID))
		}
	}

	s.metrics.TasksCancelled.Inc()
	s.metrics.TasksByStatus.WithLabelValues("cancelled").Inc()

	s.emitEvent(&pb.TaskEvent{
		TaskId:    taskID,
		Status:    pb.TaskStatus_TASK_STATUS_CANCELLED,
		Timestamp: timestamppb.New(time.Now()),
	})

	return nil
}

// ListTasks lists tasks with optional filters and pagination.
func (s *Scheduler) ListTasks(ctx context.Context, statusFilter pb.TaskStatus, pageSize int32, pageToken string) ([]*pb.Task, string, error) {
	if pageSize <= 0 {
		pageSize = 50
	}

	tasks, nextToken, err := s.storage.ListTasks(ctx, statusFilter, int(pageSize), pageToken)
	if err != nil {
		return nil, "", status.Error(codes.Internal, "failed to list tasks")
	}

	return tasks, nextToken, nil
}

// HandleTaskCompletion processes a completed or failed task and triggers dependents.
func (s *Scheduler) HandleTaskCompletion(ctx context.Context, task *pb.Task) error {
	s.logger.Info("Task completed",
		zap.String("task_id", task.Id),
		zap.String("status", task.Status.String()),
	)

	if task.Status == pb.TaskStatus_TASK_STATUS_COMPLETED {
		s.metrics.TasksCompleted.Inc()
		s.metrics.TasksByStatus.WithLabelValues("completed").Inc()
	} else if task.Status == pb.TaskStatus_TASK_STATUS_FAILED {
		s.metrics.TasksFailed.Inc()
		s.metrics.TasksByStatus.WithLabelValues("failed").Inc()
	}

	s.emitEvent(&pb.TaskEvent{
		TaskId:    task.Id,
		Status:    task.Status,
		Timestamp: timestamppb.New(time.Now()),
	})

	// If task belongs to a DAG, check for dependent tasks
	if task.ParentDagId != "" {
		if err := s.resolveDependents(ctx, task); err != nil {
			s.logger.Error("Failed to resolve dependents", zap.Error(err), zap.String("task_id", task.Id))
		}
	}

	return nil
}

// resolveDependents enqueues tasks whose dependencies are now satisfied.
func (s *Scheduler) resolveDependents(ctx context.Context, completedTask *pb.Task) error {
	s.mu.RLock()
	dag, ok := s.dags[completedTask.ParentDagId]
	s.mu.RUnlock()

	if !ok {
		return nil
	}

	for _, task := range dag.Tasks {
		if task.Status != pb.TaskStatus_TASK_STATUS_PENDING {
			continue
		}

		allDepsCompleted := true
		for _, dep := range task.Dependencies {
			depTask, exists := dag.Tasks[dep]
			if !exists || depTask.Status != pb.TaskStatus_TASK_STATUS_COMPLETED {
				allDepsCompleted = false
				break
			}
		}

		if allDepsCompleted && len(task.Dependencies) > 0 {
			if s.queue != nil {
				if err := s.queue.Enqueue(ctx, task); err != nil {
					s.logger.Error("Failed to enqueue dependent task",
						zap.Error(err),
						zap.String("task_id", task.Id),
					)
					continue
				}
			}
			task.Status = pb.TaskStatus_TASK_STATUS_QUEUED
			task.UpdatedAt = timestamppb.New(time.Now())
			s.storage.UpdateTask(ctx, task)
		}
	}

	return nil
}

// emitEvent sends a task event to the event channel.
func (s *Scheduler) emitEvent(event *pb.TaskEvent) {
	select {
	case s.eventCh <- event:
	default:
		s.logger.Warn("Event channel full, dropping event", zap.String("task_id", event.TaskId))
	}
}

// SubscribeEvents returns a read-only channel for task events.
func (s *Scheduler) SubscribeEvents() <-chan *pb.TaskEvent {
	return s.eventCh
}
