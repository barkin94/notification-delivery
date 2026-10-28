package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"

	"github.com/barkin94/notification-system/processor/internal/delivery"
	"github.com/barkin94/notification-system/shared/events"
	sharedotel "github.com/barkin94/notification-system/shared/otel"
	natsmsg "github.com/barkin94/notification-system/shared/messaging/nats"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func (c *NotificationReadyConsumer) processOne(msg natsmsg.Result[cloudevents.Event]) {
	defer msg.EndSpan()

	// Extract notification ready data from CloudEvent
	var readyData events.NotificationReadyData
	if err := json.Unmarshal(msg.Event.Data(), &readyData); err != nil {
		slog.ErrorContext(msg.Ctx, "notification ready consumer: parse CloudEvent data", "error", err)
		sharedotel.RecordError(msg.Ctx, err)
		_ = msg.Msg.Nak()
		return
	}

	err := c.pipeline.Run(msg.Ctx, &readyData, msg.AttemptNumber)
	var retryAfter delivery.ErrRetryAfter
	switch {
	case errors.As(err, &retryAfter):
		_ = msg.Msg.NakWithDelay(retryAfter.Delay)
		slog.InfoContext(msg.Ctx, "message nacked with delay", "id", readyData.NotificationID, "delay", retryAfter.Delay)
	case err != nil:
		_ = msg.Msg.Nak()
		slog.ErrorContext(msg.Ctx, "pipeline error", "id", readyData.NotificationID, "error", err)
		sharedotel.RecordError(msg.Ctx, err)
	default:
		_ = msg.Msg.Ack()
	}
}

// NotificationReadyConsumer subscribes to the three NATS JetStream priority subjects
// and fans work out to a fixed-size worker pool. On a retryable result it calls
// NakWithDelay so NATS re-delivers the same message after the backoff period,
// eliminating the need for a separate retry scheduler service.
type NotificationReadyConsumer struct {
	router      *delivery.PriorityRouter[natsmsg.Result[cloudevents.Event]]
	pipeline    *delivery.NotificationDeliveryPipeline
	concurrency int
}

// NewNotificationReadyConsumer subscribes to high/normal/low NATS subjects and
// returns a consumer ready to call StartMessageProcessing.
// maxDeliver is passed to NATS as the per-consumer MaxDeliver limit (safety net
// above the application-level MaxAttempts).
func NewNotificationReadyConsumer(
	ctx context.Context,
	h *natsmsg.Handle,
	serviceName string,
	highWeight, normalWeight, lowWeight int,
	pipeline *delivery.NotificationDeliveryPipeline,
	concurrency int,
	maxDeliver int,
) *NotificationReadyConsumer {
	highMsgs := natsmsg.Subscribe[cloudevents.Event](
		ctx, h, events.SubjectHigh, events.ConsumerGroupHigh, serviceName, maxDeliver,
	)
	normalMsgs := natsmsg.Subscribe[cloudevents.Event](
		ctx, h, events.SubjectNormal, events.ConsumerGroupNormal, serviceName, maxDeliver,
	)
	lowMsgs := natsmsg.Subscribe[cloudevents.Event](
		ctx, h, events.SubjectLow, events.ConsumerGroupLow, serviceName, maxDeliver,
	)

	router := delivery.NewPriorityRouter([]delivery.WeightedSource[natsmsg.Result[cloudevents.Event]]{
		{Ch: highMsgs, Weight: highWeight},
		{Ch: normalMsgs, Weight: normalWeight},
		{Ch: lowMsgs, Weight: lowWeight},
	})

	return &NotificationReadyConsumer{router: router, pipeline: pipeline, concurrency: concurrency}
}

func (c *NotificationReadyConsumer) StartMessageProcessing(ctx context.Context) {
	var wg sync.WaitGroup
	for range c.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				msg, ok := c.router.Next(ctx)
				if !ok {
					if ctx.Err() != nil {
						return
					}
					continue
				}
				c.processOne(msg)
			}
		}()
	}
	wg.Wait()
}