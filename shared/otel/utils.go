package otel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// ExtractTraceMetadata serializes the current span context from ctx into a
// map suitable for storage (e.g. as jsonb), using the global text map propagator.
func ExtractTraceMetadata(ctx context.Context) map[string]any {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	m := make(map[string]any, len(carrier))
	for k, v := range carrier {
		m[k] = v
	}
	return m
}

// ExtractTraceHeaders serializes the current span context from ctx into a
// map[string]string suitable for storage (e.g. as jsonb), using the global text map propagator.
func ExtractTraceHeaders(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

// ContextWithTraceMetadata restores a span context from a previously stored
// metadata map into a new child context, using the global text map propagator.
func ContextWithTraceMetadata(ctx context.Context, metadata map[string]any) context.Context {
	carrier := make(propagation.MapCarrier, len(metadata))
	for k, v := range metadata {
		if s, ok := v.(string); ok {
			carrier[k] = s
		}
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// ContextWithTraceHeaders restores a span context from a previously stored
// map[string]string into a new child context, using the global text map propagator.
func ContextWithTraceHeaders(ctx context.Context, headers map[string]string) context.Context {
	carrier := make(propagation.MapCarrier, len(headers))
	for k, v := range headers {
		carrier[k] = v
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
