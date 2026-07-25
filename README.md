# Distributed Task Scheduler

A production-ready distributed task scheduler built in Go, demonstrating senior-level engineering skills with gRPC, PostgreSQL, Redis, and Kubernetes.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client                                  │
│                    (gRPC Protocol Buffers)                      │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Scheduler Server                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   gRPC API   │  │   Scheduler  │  │   Prometheus Metrics │  │
│  │   (Health    │  │   (DAG       │  │   (15 Metrics)       │  │
│  │    Checks)   │  │    Resolver) │  │                      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────┬───────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          │                               │
          ▼                               ▼
┌─────────────────────┐       ┌─────────────────────┐
│     PostgreSQL       │       │       Redis          │
│  (Task Storage)      │       │  (Queue Backend)     │
│  FOR UPDATE SKIP     │       │  Sorted Sets         │
│  LOCKED              │       │  Dead Letter Queue   │
└─────────────────────┘       └─────────────────────┘
          │                               │
          └───────────────┬───────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Worker Pool                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Concurrency │  │   Retry      │  │   Dead Letter        │  │
│  │  Semaphore   │  │   Backoff    │  │   Handler            │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Features

- **gRPC API** with Protocol Buffers and streaming support
- **DAG Resolution** for complex task dependencies
- **Retry Logic** with exponential backoff and jitter
- **Dead Letter Queue** for failed tasks
- **Priority Queues** using Redis sorted sets
- **Semaphore-based Concurrency** control
- **PostgreSQL** with `FOR UPDATE SKIP LOCKED` for safe concurrent access
- **Prometheus Metrics** (15 metrics)
- **Structured Logging** with Uber's zap
- **Health Checks** (liveness, readiness, gRPC health)
- **Graceful Shutdown** handling
- **Kubernetes Deployment** with HPA
- **Terraform** for GKE + Cloud SQL + Memorystore Redis
- **CI/CD Pipeline** with GitHub Actions

## Quick Start

### Local Development

```bash
# Start services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f server
docker-compose logs -f worker

# Stop services
docker-compose down
```

### gRPC API Usage

```bash
# Install grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# List services
grpcurl -plaintext localhost:50051 list

# Health check
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check

# Submit a task
grpcurl -plaintext -d '{"name":"send-email","type":"email","priority":"TASK_PRIORITY_HIGH"}' \
  localhost:50051 scheduler.SchedulerService/SubmitTask

# Get task status
grpcurl -plaintext -d '{"task_id":"<task-id>"}' \
  localhost:50051 scheduler.SchedulerService/GetTask

# Stream task events
grpcurl -plaintext -d '{}' \
  localhost:50051 scheduler.SchedulerService/StreamTaskEvents
```

### Prometheus Metrics

Access metrics at `http://localhost:9090/metrics`

Available metrics:
- `scheduler_tasks_submitted_total`
- `scheduler_tasks_completed_total`
- `scheduler_tasks_failed_total`
- `scheduler_tasks_cancelled_total`
- `scheduler_tasks_by_status`
- `scheduler_tasks_by_type`
- `scheduler_tasks_by_priority`
- `scheduler_dags_submitted_total`
- `scheduler_dags_completed_total`
- `scheduler_queue_depth`
- `scheduler_worker_active`
- `scheduler_worker_idle`
- `scheduler_task_duration_seconds`
- `scheduler_retry_attempts_total`
- `scheduler_dead_letter_count`

## Configuration

All configuration is environment-based:

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `50051` | gRPC server port |
| `METRICS_PORT` | `9090` | HTTP metrics port |
| `POSTGRES_DSN` | `postgres://postgres:postgres@localhost:5432/scheduler?sslmode=disable` | PostgreSQL DSN |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `REDIS_PASSWORD` | `` | Redis password |
| `REDIS_DB` | `0` | Redis database |
| `WORKER_CONCURRENCY` | `5` | Number of concurrent workers |
| `LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |

## Deployment

### Kubernetes

```bash
# Apply configurations
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/hpa.yaml

# Check status
kubectl get pods -n scheduler
kubectl get hpa -n scheduler

# View logs
kubectl logs -f deployment/scheduler-server -n scheduler
kubectl logs -f deployment/scheduler-worker -n scheduler
```

### Terraform (GCP)

```bash
cd deploy/terraform

# Initialize
terraform init

# Plan
terraform plan -var="project_id=your-project" -var="db_password=your-password"

# Apply
terraform apply -var="project_id=your-project" -var="db_password=your-password"

# Get credentials
gcloud container clusters get-credentials distributed-task-scheduler-cluster --region us-central1
```

## Development

### Prerequisites

- Go 1.22+
- Docker & Docker Compose
- Protocol Buffers compiler (protoc)
- golangci-lint

### Building

```bash
# Build server
go build -o bin/server ./cmd/server

# Build worker
go build -o bin/worker ./cmd/worker

# Run tests
go test -v -race ./...

# Run lint
golangci-lint run
```

### Proto Generation

```bash
# Install protoc plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate proto files
protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/scheduler.proto
```

## Project Structure

```
distributed-task-scheduler/
├── cmd/
│   ├── server/         # gRPC server entry point
│   └── worker/         # Worker process entry point
├── internal/
│   ├── config/         # Configuration management
│   ├── metrics/        # Prometheus metrics
│   ├── queue/          # Queue interfaces and implementations
│   │   ├── redis_queue.go      # Redis sorted-set queue
│   │   └── priority.go         # Heap-based priority queue
│   ├── scheduler/      # Core scheduler logic
│   ├── storage/        # PostgreSQL storage
│   └── worker/         # Worker with semaphore concurrency
├── proto/              # Protocol Buffer definitions
├── deploy/
│   ├── k8s/            # Kubernetes manifests
│   ├── terraform/      # GCP infrastructure
│   └── prometheus/     # Prometheus configuration
├── .github/
│   └── workflows/      # CI/CD pipelines
├── Dockerfile          # Multi-stage build
├── docker-compose.yml  # Local development
└── README.md
```

## Testing

```bash
# Run all tests
go test -v -race ./...

# Run specific tests
go test -v ./internal/scheduler/...
go test -v ./internal/queue/...
go test -v ./internal/worker/...

# Run tests with coverage
go test -v -race -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## License

MIT
