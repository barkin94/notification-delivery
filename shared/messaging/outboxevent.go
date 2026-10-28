package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/barkin94/insider-notification/shared/otel"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// BuildOutboxEvent is the low-level helper that creates an OutboxEvent from raw data.
// Producers should call this with their domain data.
func BuildOutboxEvent(ctx context.Context, data any, eventType, source, aggregateID, aggregateType, topic string) (OutboxEvent, error) {
	// Create CloudEvent with trace context at publish time
	ce := cloudevents.NewEvent()
	ce.SetSpecVersion(cloudevents.VersionV1)
	ce.SetID(aggregateID)
	ce.SetSource(source)
	ce.SetType(eventType)
	ce.SetTime(time.Now().UTC())
	ce.SetData(cloudevents.ApplicationJSON, data)

	// Marshal CloudEvent to JSON for outbox payload
	payload, err := json.Marshal(ce)
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("marshal CloudEvent: %w", err)
	}

	// Extract trace headers from context for propagation
	traceHeaders := otel.ExtractTraceHeaders(ctx)
	traceHeadersJSON, _ := json.Marshal(traceHeaders)

	// Parse aggregate ID for outbox
	aggID, err := uuid.Parse(aggregateID)
	if err != nil {
		aggID = uuid.New()
	}

	return OutboxEvent{
		AggregateType: aggregateType,
		AggregateID:   aggID,
		EventType:     eventType,
		Payload:       payload,
		Topic:         topic,
		TraceHeaders:  traceHeadersJSON,
	}, nil
}