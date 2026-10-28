package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/barkin94/notification-system/shared/httpclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// DeliveryResult holds the outcome of a single delivery attempt.
type DeliveryResult struct {
	Success       bool
	Retryable     bool
	StatusCode    int
	LatencyMS     int64
	ProviderMsgID string
	ErrorMessage  string
}

// DeliveryClient delivers a notification to the webhook provider.
type NtfnDeliveryClient interface {
	Send(ctx context.Context, to, channel, content string) DeliveryResult
}

type ntfnDeliveryClient struct {
	http *httpclient.Client
}

// NewDeliveryClient returns a DeliveryClient that POSTs to notificationProviderURL with the given timeout.
func NewNtfnDeliveryClient(notificationProviderURL string, timeout time.Duration) NtfnDeliveryClient {
	return &ntfnDeliveryClient{
		http: httpclient.New(notificationProviderURL, httpclient.WithTimeout(timeout)),
	}
}

type ntfnDeliveryRequestBody struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

func (c *ntfnDeliveryClient) Send(ctx context.Context, to, channel, content string) DeliveryResult {
	ctx, span := otel.Tracer("ntfndeliveryclient").Start(ctx, "ntfndeliveryclient.Send", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	span.SetAttributes(
		attribute.String("delivery.to", to),
		attribute.String("delivery.channel", channel),
	)

	start := time.Now()
	resp, err := c.http.Request(ctx, http.MethodPost, "", ntfnDeliveryRequestBody{
		To:      to,
		Channel: channel,
		Content: content,
	})
	latency := time.Since(start).Milliseconds()

	if err != nil {
		return DeliveryResult{Retryable: true, LatencyMS: latency, ErrorMessage: err.Error()}
	}
	defer resp.Body.Close() //nolint:errcheck

	code := resp.StatusCode
	result := DeliveryResult{StatusCode: code, LatencyMS: latency}
	span.SetAttributes(attribute.Int("http.status_code", code))

	switch code {
	case http.StatusAccepted:
		result.Success = true
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		result.ErrorMessage = fmt.Sprintf("non-retryable provider error: %d", code)
	default:
		result.Retryable = true
		result.ErrorMessage = fmt.Sprintf("retryable provider error: %d", code)
	}

	span.SetAttributes(
		attribute.Bool("delivery.success", result.Success),
		attribute.Bool("delivery.retryable", result.Retryable),
	)

	slog.InfoContext(ctx, "delivery response",
		"to", to,
		"channel", channel,
		"status", code,
		"latency_ms", latency,
		"success", result.Success,
		"retryable", result.Retryable,
	)
	return result
}
