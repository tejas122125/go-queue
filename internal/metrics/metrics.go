package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Registry is the dedicated Prometheus registry matching TS prom-client register.
	Registry = prometheus.NewRegistry()

	JobsCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "queue_forge_jobs_created_total",
		Help: "Total jobs created",
	})

	JobsCompleted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "queue_forge_jobs_completed_total",
		Help: "Total jobs completed",
	})

	JobsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "queue_forge_jobs_failed_total",
		Help: "Total jobs failed",
	})

	ExecutionLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "queue_forge_job_execution_seconds",
		Help:    "Job execution duration",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10},
	})

	OutboxLagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "queue_forge_outbox_lag_seconds",
		Help: "Age in seconds of the oldest unprocessed outbox event",
	})

	OutboxPendingEvents = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "queue_forge_outbox_pending_events",
		Help: "Number of unprocessed outbox events",
	})

	OutboxEventsPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "queue_forge_outbox_events_published_total",
		Help: "Total outbox events published to Redis",
	})

	OutboxEventsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "queue_forge_outbox_events_failed_total",
		Help: "Total outbox event publish failures",
	})

	OutboxPublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "queue_forge_outbox_publish_duration_seconds",
		Help:    "Outbox event publish duration",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	})

	DelayedJobsPromoted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "queue_forge_delayed_jobs_promoted_total",
		Help: "Total delayed jobs promoted to ready queues",
	})
)

func init() {
	Registry.MustRegister(
		JobsCreated,
		JobsCompleted,
		JobsFailed,
		ExecutionLatency,
		OutboxLagSeconds,
		OutboxPendingEvents,
		OutboxEventsPublished,
		OutboxEventsFailed,
		OutboxPublishDuration,
		DelayedJobsPromoted,
	)
}

// Handler returns the HTTP handler for exposing Prometheus metrics.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}

