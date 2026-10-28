package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	natsio "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"

	messaging "github.com/barkin94/notification-system/shared/messaging"
)

// NewPublisher constructs a BatchPublisher that sends JSON-encoded events to NATS
// subjects via JetStream. Single publishes wait for server ack; batch publishes
// pipeline all messages before collecting acks.
func NewPublisher(h *Handle) messaging.BatchPublisher {
	return &publisher{h: h}
}

type publisher struct{ h *Handle }

func (p *publisher) Publish(ctx context.Context, subject string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("nats marshal payload: %w", err)
	}
	msg := &natsio.Msg{
		Subject: subject,
		Data:    b,
		Header:  make(natsio.Header),
	}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{msg.Header})
	if _, err := p.h.js.PublishMsg(msg); err != nil {
		slog.ErrorContext(ctx, "nats publish failed", "subject", subject, "error", err)
		return err
	}
	slog.InfoContext(ctx, "nats message published", "subject", subject)
	return nil
}

func (p *publisher) PublishBatch(ctx context.Context, subject string, messages []messaging.BatchMessage) error {
	if len(messages) == 0 {
		return nil
	}
	slog.DebugContext(ctx, "nats publishing batch", "subject", subject, "count", len(messages))
	futures := make([]natsio.PubAckFuture, 0, len(messages))
	for _, m := range messages {
		b, err := json.Marshal(m.Payload)
		if err != nil {
			return fmt.Errorf("nats marshal payload: %w", err)
		}
		msg := &natsio.Msg{
			Subject: subject,
			Data:    b,
			Header:  make(natsio.Header),
		}
		otel.GetTextMapPropagator().Inject(m.Ctx, headerCarrier{msg.Header})
		f, err := p.h.js.PublishMsgAsync(msg)
		if err != nil {
			return fmt.Errorf("nats async publish: %w", err)
		}
		futures = append(futures, f)
	}
	select {
	case <-p.h.js.PublishAsyncComplete():
	case <-ctx.Done():
		slog.WarnContext(ctx, "nats batch publish aborted, context cancelled", "subject", subject)
		return ctx.Err()
	}
	var errs []error
	for _, f := range futures {
		select {
		case <-f.Ok():
			slog.InfoContext(ctx, "nats message published", "subject", subject)
		case err := <-f.Err():
			slog.ErrorContext(ctx, "nats batch ack failed", "subject", subject, "error", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
