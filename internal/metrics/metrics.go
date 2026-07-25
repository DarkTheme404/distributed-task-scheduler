package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the scheduler.
type Metrics struct {
	TasksSubmitted   prometheus.Counter
	TasksCompleted   prometheus.Counter
	TasksFailed      prometheus.Counter
	TasksCancelled   prometheus.Counter
	TasksByStatus    *prometheus.CounterVec
	TasksByType      *prometheus.CounterVec
	TasksByPriority  *prometheus.CounterVec
	DAGsSubmitted    prometheus.Counter
	DAGsCompleted    prometheus.Counter
	QueueDepth       prometheus.Gauge
	WorkerActive     prometheus.Gauge
	WorkerIdle       prometheus.Gauge
	TaskDuration     prometheus.Histogram
	RetryAttempts    prometheus.Counter
	DeadLetterCount  prometheus.Counter
}

// NewMetrics creates and registers all Prometheus metrics.
func NewMetrics() *Metrics {
	m := &Metrics{
		TasksSubmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_submitted_total",
			Help: "Total number of tasks submitted",
		}),
		TasksCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_completed_total",
			Help: "Total number of tasks completed successfully",
		}),
		TasksFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_failed_total",
			Help: "Total number of tasks that failed",
		}),
		TasksCancelled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_cancelled_total",
			Help: "Total number of tasks cancelled",
		}),
		TasksByStatus: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_tasks_by_status",
			Help: "Tasks grouped by status",
		}, []string{"status"}),
		TasksByType: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_tasks_by_type",
			Help: "Tasks grouped by type",
		}, []string{"type"}),
		TasksByPriority: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_tasks_by_priority",
			Help: "Tasks grouped by priority",
		}, []string{"priority"}),
		DAGsSubmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_dags_submitted_total",
			Help: "Total number of DAGs submitted",
		}),
		DAGsCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_dags_completed_total",
			Help: "Total number of DAGs completed",
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_queue_depth",
			Help: "Current number of tasks in queue",
		}),
		WorkerActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_worker_active",
			Help: "Number of workers currently processing tasks",
		}),
		WorkerIdle: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_worker_idle",
			Help: "Number of workers currently idle",
		}),
		TaskDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "scheduler_task_duration_seconds",
			Help:    "Task execution duration in seconds",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 15),
		}),
		RetryAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_retry_attempts_total",
			Help: "Total number of retry attempts",
		}),
		DeadLetterCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_dead_letter_count",
			Help: "Total number of tasks moved to dead letter queue",
		}),
	}

	// Register all metrics
	prometheus.MustRegister(
		m.TasksSubmitted,
		m.TasksCompleted,
		m.TasksFailed,
		m.TasksCancelled,
		m.TasksByStatus,
		m.TasksByType,
		m.TasksByPriority,
		m.DAGsSubmitted,
		m.DAGsCompleted,
		m.QueueDepth,
		m.WorkerActive,
		m.WorkerIdle,
		m.TaskDuration,
		m.RetryAttempts,
		m.DeadLetterCount,
	)

	return m
}

// Handler returns an HTTP handler for Prometheus metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.Handler()
}
