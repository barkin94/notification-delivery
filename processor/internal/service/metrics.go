package service

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	apipub "github.com/barkin94/notification-system/api/public"
)

// Metrics records delivery outcomes for the processor service.
type metrics struct {
	sentCounter   metric.Int64Counter
	failedCounter metric.Int64Counter
	latencyHist   metric.Int64Histogram
}

// Metrics is the interface for recording delivery outcomes.
type Metrics interface {
	RecordNotificationSent(ctx context.Context, latencyMS int64)
	RecordNotificationFailed(ctx context.Context, latencyMS int64)
}

var _ Metrics = (*metrics)(nil)

// NewMetrics registers all processor metric instruments against the global MeterProvider.
// rdb is used to poll Redis stream lengths for the queue depth gauge.
func NewMetrics(rdb *goredis.Client) (Metrics, error) {
	meter := otel.Meter("processor")

	sent, err := newSentCounter(meter)
	if err != nil {
		return nil, err
	}

	failed, err := newFailedCounter(meter)
	if err != nil {
		return nil, err
	}

	latency, err := newLatencyHistogram(meter)
	if err != nil {
		return nil, err
	}

	if err := newQueueDepthGauge(meter, rdb); err != nil {
		return nil, err
	}

	return &metrics{
		sentCounter:   sent,
		failedCounter: failed,
		latencyHist:   latency,
	}, nil
}

func (m *metrics) RecordNotificationSent(ctx context.Context, latencyMS int64) {
	m.sentCounter.Add(ctx, 1)
	m.latencyHist.Record(ctx, latencyMS)
}

func (m *metrics) RecordNotificationFailed(ctx context.Context, latencyMS int64) {
	m.failedCounter.Add(ctx, 1)
	m.latencyHist.Record(ctx, latencyMS)
}

func newSentCounter(meter metric.Meter) (metric.Int64Counter, error) {
	return meter.Int64Counter("notification.sent")
}

func newFailedCounter(meter metric.Meter) (metric.Int64Counter, error) {
	return meter.Int64Counter("notification.failed")
}

func newLatencyHistogram(meter metric.Meter) (metric.Int64Histogram, error) {
	return meter.Int64Histogram("notification.delivery.latency.ms",
		metric.WithExplicitBucketBoundaries(50, 100, 250, 500, 1000, 2500, 5000),
	)
}

func newQueueDepthGauge(meter metric.Meter, rdb *goredis.Client) error {
	_, err := meter.Int64ObservableGauge("notification.queue.depth",
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			if rdb == nil {
				return nil
			}
			for priority, topic := range map[string]string{
				"high":   string(apipub.TopicHigh),
				"normal": string(apipub.TopicNormal),
				"low":    string(apipub.TopicLow),
			} {
				n, err := rdb.XLen(ctx, topic).Result()
				if err == nil {
					o.Observe(n, metric.WithAttributes(attribute.String("priority", priority)))
				}
			}
			return nil
		}),
	)
	return err
}
