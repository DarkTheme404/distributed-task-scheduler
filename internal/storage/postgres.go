package storage

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/DarkTheme404/distributed-task-scheduler/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrTaskNotFound = errors.New("task not found")

// Store defines the interface for task storage operations.
type Store interface {
	CreateTask(ctx context.Context, task *pb.Task) error
	GetTask(ctx context.Context, id string) (*pb.Task, error)
	UpdateTask(ctx context.Context, task *pb.Task) error
	DeleteTask(ctx context.Context, id string) error
	ListTasks(ctx context.Context, status pb.TaskStatus, limit int, offset string) ([]*pb.Task, string, error)
	Ping(ctx context.Context) error
	Close() error
}

// PostgresStore implements Store using PostgreSQL.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore creates a new PostgreSQL-backed store.
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	config.MaxConns = 20
	config.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if err := migrate(ctx, pool); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

// migrate creates the necessary database tables.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS tasks (
			id UUID PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			type VARCHAR(100) NOT NULL,
			payload JSONB DEFAULT '{}',
			priority INTEGER DEFAULT 1,
			status VARCHAR(50) DEFAULT 'pending',
			max_retries INTEGER DEFAULT 3,
			retry_count INTEGER DEFAULT 0,
			error_message TEXT DEFAULT '',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			scheduled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			completed_at TIMESTAMP WITH TIME ZONE,
			dependencies JSONB DEFAULT '[]',
			parent_dag_id UUID
		);

		CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
		CREATE INDEX IF NOT EXISTS idx_tasks_scheduled_at ON tasks(scheduled_at);
		CREATE INDEX IF NOT EXISTS idx_tasks_parent_dag_id ON tasks(parent_dag_id);
		CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority DESC);
	`)
	return err
}

// CreateTask inserts a new task into the database.
func (s *PostgresStore) CreateTask(ctx context.Context, task *pb.Task) error {
	payload, err := protojson.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO tasks (id, name, type, payload, priority, status, max_retries, retry_count,
			error_message, created_at, updated_at, scheduled_at, completed_at, dependencies, parent_dag_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			type = EXCLUDED.type,
			payload = EXCLUDED.payload,
			priority = EXCLUDED.priority,
			status = EXCLUDED.status,
			max_retries = EXCLUDED.max_retries,
			retry_count = EXCLUDED.retry_count,
			error_message = EXCLUDED.error_message,
			updated_at = EXCLUDED.updated_at,
			scheduled_at = EXCLUDED.scheduled_at,
			completed_at = EXCLUDED.completed_at,
			dependencies = EXCLUDED.dependencies,
			parent_dag_id = EXCLUDED.parent_dag_id
	`,
		task.Id,
		task.Name,
		task.Type,
		string(payload),
		int32(task.Priority),
		task.Status.String(),
		task.MaxRetries,
		task.RetryCount,
		task.ErrorMessage,
		task.CreatedAt.AsTime(),
		task.UpdatedAt.AsTime(),
		task.ScheduledAt.AsTime(),
		nil,
		task.Dependencies,
		task.ParentDagId,
	)

	return err
}

// GetTask retrieves a task by ID using FOR UPDATE SKIP LOCKED.
func (s *PostgresStore) GetTask(ctx context.Context, id string) (*pb.Task, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, type, payload, priority, status, max_retries, retry_count,
			error_message, created_at, updated_at, scheduled_at, completed_at,
			dependencies, parent_dag_id
		FROM tasks
		WHERE id = $1
		FOR UPDATE SKIP LOCKED
	`, id)

	task, err := scanTask(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	return task, nil
}

// UpdateTask updates an existing task.
func (s *PostgresStore) UpdateTask(ctx context.Context, task *pb.Task) error {
	payload, err := protojson.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	result, err := s.pool.Exec(ctx, `
		UPDATE tasks SET
			name = $2,
			type = $3,
			payload = $4,
			priority = $5,
			status = $6,
			max_retries = $7,
			retry_count = $8,
			error_message = $9,
			updated_at = $10,
			scheduled_at = $11,
			completed_at = $12,
			dependencies = $13,
			parent_dag_id = $14
		WHERE id = $1
	`,
		task.Id,
		task.Name,
		task.Type,
		string(payload),
		int32(task.Priority),
		task.Status.String(),
		task.MaxRetries,
		task.RetryCount,
		task.ErrorMessage,
		task.UpdatedAt.AsTime(),
		task.ScheduledAt.AsTime(),
		nil,
		task.Dependencies,
		task.ParentDagId,
	)

	if err != nil {
		return fmt.Errorf("failed to update task: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrTaskNotFound
	}

	return nil
}

// DeleteTask removes a task by ID.
func (s *PostgresStore) DeleteTask(ctx context.Context, id string) error {
	result, err := s.pool.Exec(ctx, `DELETE FROM tasks WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete task: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrTaskNotFound
	}

	return nil
}

// ListTasks lists tasks with optional status filter and pagination.
func (s *PostgresStore) ListTasks(ctx context.Context, status pb.TaskStatus, limit int, offset string) ([]*pb.Task, string, error) {
	query := `
		SELECT id, name, type, payload, priority, status, max_retries, retry_count,
			error_message, created_at, updated_at, scheduled_at, completed_at,
			dependencies, parent_dag_id
		FROM tasks
		WHERE ($1::text = '' OR status = $1)
		ORDER BY priority DESC, created_at ASC
		LIMIT $2
	`

	statusStr := ""
	if status != pb.TaskStatus_TASK_STATUS_UNSPECIFIED {
		statusStr = status.String()
	}

	rows, err := s.pool.Query(ctx, query, statusStr, limit)
	if err != nil {
		return nil, "", fmt.Errorf("failed to list tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*pb.Task
	var lastID string
	for rows.Next() {
		task, err := scanTaskRows(rows)
		if err != nil {
			return nil, "", fmt.Errorf("failed to scan task: %w", err)
		}
		tasks = append(tasks, task)
		lastID = task.Id
	}

	return tasks, lastID, nil
}

// Ping checks the database connection.
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes the connection pool.
func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

// scanTask scans a single row into a Task proto.
func scanTask(row pgx.Row) (*pb.Task, error) {
	var task pb.Task
	var payload string
	var status string
	var dependencies []string
	var createdAt, updatedAt, scheduledAt interface{}

	err := row.Scan(
		&task.Id,
		&task.Name,
		&task.Type,
		&payload,
		&task.Priority,
		&status,
		&task.MaxRetries,
		&task.RetryCount,
		&task.ErrorMessage,
		&createdAt,
		&updatedAt,
		&scheduledAt,
		&task.CompletedAt,
		&dependencies,
		&task.ParentDagId,
	)
	if err != nil {
		return nil, err
	}

	task.Status = pb.TaskStatus(pb.TaskStatus_value[status])
	task.Dependencies = dependencies

	if t, ok := createdAt.(*timestamppb.Timestamp); ok {
		task.CreatedAt = t
	}
	if t, ok := updatedAt.(*timestamppb.Timestamp); ok {
		task.UpdatedAt = t
	}
	if t, ok := scheduledAt.(*timestamppb.Timestamp); ok {
		task.ScheduledAt = t
	}

	return &task, nil
}

// scanTaskRows scans a row from rows result into a Task proto.
func scanTaskRows(rows pgx.Rows) (*pb.Task, error) {
	var task pb.Task
	var payload string
	var status string
	var dependencies []string
	var createdAt, updatedAt, scheduledAt interface{}

	err := rows.Scan(
		&task.Id,
		&task.Name,
		&task.Type,
		&payload,
		&task.Priority,
		&status,
		&task.MaxRetries,
		&task.RetryCount,
		&task.ErrorMessage,
		&createdAt,
		&updatedAt,
		&scheduledAt,
		&task.CompletedAt,
		&dependencies,
		&task.ParentDagId,
	)
	if err != nil {
		return nil, err
	}

	task.Status = pb.TaskStatus(pb.TaskStatus_value[status])
	task.Dependencies = dependencies

	if t, ok := createdAt.(*timestamppb.Timestamp); ok {
		task.CreatedAt = t
	}
	if t, ok := updatedAt.(*timestamppb.Timestamp); ok {
		task.UpdatedAt = t
	}
	if t, ok := scheduledAt.(*timestamppb.Timestamp); ok {
		task.ScheduledAt = t
	}

	return &task, nil
}
