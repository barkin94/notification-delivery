package otel

import (
	"context"
	"errors"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	sharedlogger "github.com/barkin94/notification-system/shared/logger"
)

// Shutdown cleanly flushes and stops the OTel SDK.
type Shutdown func(context.Context) error

// Init initializes the global TracerProvider, MeterProvider, and LoggerProvider.
// It returns an error instead of exiting to allow graceful degradation or custom handling at startup.
func Init(ctx context.Context, serviceName, collectorEndpoint, logLevel string) (Shutdown, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
	)
	if err != nil {
		return nil, errors.Join(errors.New("failed to create otel resource"), err)
	}

	// 1. Traces Setup
	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(collectorEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, errors.Join(errors.New("failed to create otlp trace exporter"), err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// Use standard composite propagator (TraceContext + Baggage)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// 2. Metrics Setup
	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(collectorEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, errors.Join(errors.New("failed to create otlp metric exporter"), err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	// 3. Logs Setup (OTel Logs Bridge)
	logExp, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(collectorEndpoint),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		return nil, errors.Join(errors.New("failed to create otlp log exporter"), err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
		sdklog.WithResource(res),
	)
	global.SetLoggerProvider(lp)

	// Handle internal OTel errors cleanly without crashing
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Error("otel internal error", "error", err)
	}))

	// Configure structured slog to pipe directly into OpenTelemetry
	slog.SetDefault(slog.New(&levelHandler{
		min:  sharedlogger.ParseLevel(logLevel),
		next: otelslog.NewHandler("app"),
	}))

	// Return a unified shutdown hook that guarantees execution of all flushes
	return func(ctx context.Context) error {
		var errs []error
		if err := tp.Shutdown(ctx); err != nil {
			errs = append(errs, errors.Join(errors.New("failed shutting down tracer provider"), err))
		}
		if err := mp.Shutdown(ctx); err != nil {
			errs = append(errs, errors.Join(errors.New("failed shutting down meter provider"), err))
		}
		if err := lp.Shutdown(ctx); err != nil {
			errs = append(errs, errors.Join(errors.New("failed shutting down logger provider"), err))
		}
		return errors.Join(errs...)
	}, nil
}

// levelHandler enforces a minimum slog level cleanly.
type levelHandler struct {
	min  slog.Level
	next slog.Handler
}

func (h *levelHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.min
}

func (h *levelHandler) Handle(ctx context.Context, r slog.Record) error {
	if !h.Enabled(ctx, r.Level) {
		return nil
	}
	return h.next.Handle(ctx, r)
}

func (h *levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelHandler{h.min, h.next.WithAttrs(attrs)}
}

func (h *levelHandler) WithGroup(name string) slog.Handler {
	return &levelHandler{h.min, h.next.WithGroup(name)}
}
