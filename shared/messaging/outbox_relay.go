package messaging

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/barkin94/notification-system/shared/otel"
)

// OutboxRelay publishes events from the outbox to the message stream
type OutboxRelay struct {
	repo         OutboxReader
	publisher    BatchPublisher
	pollInterval time.Duration
	batchSize    int
	maxRetries   int
	logger       *slog.Logger
}

// NewOutboxRelay creates a new outbox relay
func NewOutboxRelay(repo OutboxReader, publisher BatchPublisher, pollInterval time.Duration, batchSize int, maxRetries int) *OutboxRelay {
	if batchSize <= 0 {
		batchSize = 100
	}
	if maxRetries <= 0 {
		maxRetries = 10
	}
	return &OutboxRelay{
		repo:         repo,
		publisher:    publisher,
		pollInterval: pollInterval,
		batchSize:    batchSize,
		maxRetries:   maxRetries,
		logger:       slog.Default().With("component", "outbox-relay"),
	}
}

// Run starts the outbox relay loop
func (r *OutboxRelay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	r.logger.InfoContext(ctx, "outbox relay started", "poll_interval", r.pollInterval, "batch_size", r.batchSize, "max_retries", r.maxRetries)

	for {
		select {
		case <-ctx.Done():
			r.logger.InfoContext(ctx, "outbox relay stopping")
			return
		case <-ticker.C:
			r.flush(ctx)
		}
	}
}

// flush processes a batch of unpublished events
func (r *OutboxRelay) flush(ctx context.Context) {
	events, err := r.repo.GetUnpublished(ctx, r.batchSize)
	if err != nil {
		r.logger.ErrorContext(ctx, "failed to get unpublished events", "error", err)
		return
	}
	if len(events) == 0 {
		return
	}

	r.logger.DebugContext(ctx, "publishing outbox events", "count", len(events))

	// Group events by topic for batch publishing
	eventsByTopic := make(map[string][]OutboxEvent)
	for _, e := range events {
		eventsByTopic[e.Topic] = append(eventsByTopic[e.Topic], e)
	}

	var publishedIDs []string
	var failedIDs []string
	for topic, topicEvents := range eventsByTopic {
		msgs := make([]BatchMessage, len(topicEvents))
		for i, e := range topicEvents {
			// Parse trace headers if present
			var traceCtx = ctx
			if len(e.TraceHeaders) > 0 {
				var headers map[string]string
				if err := json.Unmarshal(e.TraceHeaders, &headers); err == nil {
					traceCtx = otel.ContextWithTraceHeaders(ctx, headers)
				}
			}

			// The payload is already a serialized CloudEvent
			var ce cloudevents.Event
			if err := json.Unmarshal(e.Payload, &ce); err != nil {
				r.logger.ErrorContext(ctx, "failed to unmarshal CloudEvent from outbox", "event_id", e.ID.String(), "error", err)
				// Invalid event - increment retry count and skip
				failedIDs = append(failedIDs, e.ID.String())
				continue
			}

			msgs[i] = BatchMessage{
				Ctx:     traceCtx,
				Payload: &ce,
			}
			publishedIDs = append(publishedIDs, e.ID.String())
		}

		if len(msgs) > 0 {
			if err := r.publisher.PublishBatch(ctx, topic, msgs); err != nil {
				r.logger.ErrorContext(ctx, "failed to publish batch", "topic", topic, "count", len(msgs), "error", err)
				// All events in this batch failed - add to failed list
				for _, e := range topicEvents {
					failedIDs = append(failedIDs, e.ID.String())
				}
			}
		}
	}

	// Mark all successfully published events
	if len(publishedIDs) > 0 {
		if err := r.repo.MarkPublished(ctx, publishedIDs, time.Now().UTC()); err != nil {
			r.logger.ErrorContext(ctx, "failed to mark events as published", "error", err)
		} else {
			r.logger.DebugContext(ctx, "marked events as published", "count", len(publishedIDs))
		}
	}

	// Handle failed events - increment retry count
	if len(failedIDs) > 0 {
		if err := r.repo.IncrementRetryCount(ctx, failedIDs); err != nil {
			r.logger.ErrorContext(ctx, "failed to increment retry count", "error", err)
		}

		// Check for dead letters (events that exceeded max retries)
		// Note: We'd need to fetch the events again to check retry_count, but for now
		// we'll just log. A more complete implementation would query retry_count.
		r.logger.WarnContext(ctx, "some events failed to publish, will retry", "failed_count", len(failedIDs), "max_retries", r.maxRetries)
	}
}
