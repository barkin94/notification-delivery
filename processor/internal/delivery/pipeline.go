package delivery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/barkin94/notification-system/processor/internal/service"
	"github.com/barkin94/notification-system/shared/events"
	"github.com/barkin94/notification-system/shared/lock"
	"github.com/barkin94/notification-system/shared/messaging"
)

// ErrRetryAfter is returned by Run when the message should be redelivered via
// NakWithDelay after the given duration. It is not a fatal error.
type ErrRetryAfter struct {
	Delay time.Duration
}

func (e ErrRetryAfter) Error() string {
	return fmt.Sprintf("retry after %s", e.Delay)
}

// NotificationReadyEvent interface defines the methods needed by the pipeline
type NotificationReadyEvent interface {
	GetNotificationID() string
	GetChannel() string
	GetRecipient() string
	GetContent() string
	GetPriority() string
	GetMaxAttempts() int
}

// Ensure events.NotificationReadyData implements NotificationReadyEvent
var _ NotificationReadyEvent = (*events.NotificationReadyData)(nil)

// NotificationDeliveryPipeline runs a single notification event through
// each gate in sequence: locking, rate limiting, delivery, and outcome recording.
type NotificationDeliveryPipeline struct {
	outboxRepo     messaging.OutboxRepository
	deliveryClient service.NtfnDeliveryClient
	limiter        service.Limiter
	locker         lock.Locker
	metrics        service.Metrics
}

func NewNotificationDeliveryPipeline(
	outboxRepo messaging.OutboxRepository,
	deliveryClient service.NtfnDeliveryClient,
	limiter service.Limiter,
	locker lock.Locker,
	m service.Metrics,
) *NotificationDeliveryPipeline {
	return &NotificationDeliveryPipeline{
		outboxRepo:     outboxRepo,
		deliveryClient: deliveryClient,
		limiter:        limiter,
		locker:         locker,
		metrics:        m,
	}
}

// Run processes one notification event. attemptNumber is the 1-indexed NATS delivery
// count for this message (1 = first attempt, 2 = first retry, etc.).
// Returns ErrRetryAfter when the caller should NakWithDelay, nil on success or
// terminal failure (caller should Ack), or another error on infrastructure failure
// (caller should Nack).
func (p *NotificationDeliveryPipeline) Run(ctx context.Context, evt NotificationReadyEvent, attemptNumber int) error {
	lockAcquired, err := p.locker.TryLock(ctx, evt.GetNotificationID())
	if err != nil {
		slog.ErrorContext(ctx, "lock error", "notification_id", evt.GetNotificationID(), "error", err)
		return err
	}

	// If lock is not acquired, another worker is already processing this notification.
	if !lockAcquired {
		slog.InfoContext(ctx, "lock miss, skipping", "notification_id", evt.GetNotificationID())
		return nil
	}
	defer p.locker.Unlock(ctx, evt.GetNotificationID()) //nolint:errcheck

	if err := p.applyRateLimit(ctx, evt); err != nil {
		return err // ErrRetryAfter or infra error — both propagate to caller
	}

	dr := p.deliveryClient.Send(ctx, evt.GetRecipient(), evt.GetChannel(), evt.GetContent())

	return p.handleDeliveryResult(ctx, evt, dr, attemptNumber)
}

// applyRateLimit checks the channel's token bucket. Returns ErrRetryAfter when
// the bucket is exhausted so the caller can NakWithDelay without counting the
// attempt against MaxAttempts. Returns nil when the request is allowed through.
func (p *NotificationDeliveryPipeline) applyRateLimit(ctx context.Context, evt NotificationReadyEvent) error {
	allowed, retryAfter, err := p.limiter.IsAllowed(ctx, evt.GetChannel())
	if err != nil {
		slog.ErrorContext(ctx, "rate limit error", "notification_id", evt.GetNotificationID(), "error", err)
		return err
	}
	if allowed {
		return nil
	}
	if retryAfter <= 0 {
		retryAfter = time.Second
	}
	slog.InfoContext(ctx, "rate limited, naking with delay", "notification_id", evt.GetNotificationID(), "channel", evt.GetChannel(), "delay", retryAfter)
	return ErrRetryAfter{Delay: retryAfter}
}

// handleDeliveryResult returns ErrRetryAfter for retryable failures with remaining
// attempts, nil after publishing a terminal status event, or an error if the status
// event could not be published.
func (p *NotificationDeliveryPipeline) handleDeliveryResult(ctx context.Context, evt NotificationReadyEvent, dr service.DeliveryResult, attemptNumber int) error {
	currentAttempt := attemptNumber
	maxAttempts := maxAttemptsFor(evt)
	switch {
	case dr.Success:
		p.metrics.RecordNotificationSent(ctx, dr.LatencyMS)
		data := events.NotificationDeliveryResultData{
			NotificationID:    evt.GetNotificationID(),
			Status:            events.DeliveryStatusDelivered,
			AttemptNumber:     currentAttempt,
			HTTPStatusCode:    dr.StatusCode,
			ProviderMessageID: dr.ProviderMsgID,
			LatencyMS:         int(dr.LatencyMS),
		}
		return p.publishStatus(ctx, data)

	case dr.Retryable && currentAttempt < maxAttempts:
		delay := service.RetryDelay(currentAttempt + 1)
		slog.InfoContext(ctx, "retryable failure, naking with delay", "notification_id", evt.GetNotificationID(), "attempt", currentAttempt, "delay", delay)
		return ErrRetryAfter{Delay: delay}

	default:
		p.metrics.RecordNotificationFailed(ctx, dr.LatencyMS)
		data := events.NotificationDeliveryResultData{
			NotificationID:  evt.GetNotificationID(),
			Status:          events.DeliveryStatusFailed,
			AttemptNumber:   currentAttempt,
			HTTPStatusCode:  dr.StatusCode,
			ErrorMessage:    dr.ErrorMessage,
			LatencyMS:       int(dr.LatencyMS),
		}
		return p.publishStatus(ctx, data)
	}
}

func (p *NotificationDeliveryPipeline) publishStatus(ctx context.Context, data events.NotificationDeliveryResultData) error {
	outboxEvent, err := BuildNotificationDeliveryResultEvent(ctx, data, "notify.status")
	if err != nil {
		return err
	}

	if err := p.outboxRepo.Save(ctx, outboxEvent); err != nil {
		slog.ErrorContext(ctx, "write status to outbox failed", "notification_id", data.NotificationID, "error", err)
		return err
	}
	return nil
}

// BuildNotificationDeliveryResultEvent creates an OutboxEvent for NotificationDeliveryResult.
func BuildNotificationDeliveryResultEvent(ctx context.Context, data events.NotificationDeliveryResultData, topic string) (messaging.OutboxEvent, error) {
	return messaging.BuildOutboxEvent(ctx, data, events.TypeNotificationDeliveryResult, events.SourceProcessor, data.NotificationID, "notification_delivery", topic)
}

func maxAttemptsFor(evt NotificationReadyEvent) int {
	if evt.GetMaxAttempts() > 0 {
		return evt.GetMaxAttempts()
	}
	return service.MaxAttempts
}