// Package observability provides OpenTelemetry tracing initialisation helpers.
package observability

import (
	"context"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"
)

// InitTracer configures an OTLP gRPC trace exporter and sets it as the global
// TracerProvider. The returned function must be called on shutdown to flush
// pending spans. If the exporter cannot be created the function logs a warning
// and returns a no-op shutdown function so the caller can proceed without
// tracing.
func InitTracer(serviceName string, logger *zap.Logger) func() {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		logger.Info("OpenTelemetry disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)")
		return func() {}
	}

	ctx := context.Background()
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		logger.Warn("OpenTelemetry init failed (tracing disabled)", zap.Error(err))
		return func() {}
	}

	res, _ := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	logger.Info("OpenTelemetry tracing initialized",
		zap.String("endpoint", endpoint),
		zap.String("service", serviceName),
	)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
	}
}
