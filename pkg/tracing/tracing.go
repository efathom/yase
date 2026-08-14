// Package tracing provides OpenTelemetry distributed tracing for YASE.
// Supports stdout and OTLP/HTTP exporters.
package tracing

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Config configures distributed tracing.
type Config struct {
	Enabled    bool    `mapstructure:"enabled"`
	Exporter   string  `mapstructure:"exporter"`    // "stdout", "otlp" (default "stdout")
	Endpoint   string  `mapstructure:"endpoint"`    // OTLP endpoint (e.g., "localhost:4318")
	SampleRate float64 `mapstructure:"sample_rate"` // 0.0-1.0 (default 0.1 = 10%)
}

// Tracer is the YASE application tracer.
// Initialized with a safe no-op default to prevent nil-pointer panics
// if StartSpan is called before Setup.
var Tracer trace.Tracer = otel.Tracer("yase")

// Setup initializes the OpenTelemetry tracer provider.
// Returns a shutdown function that should be called on service exit.
func Setup(ctx context.Context, serviceName string, cfg Config) (func(context.Context) error, error) {
	if !cfg.Enabled {
		Tracer = otel.Tracer(serviceName)
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	switch cfg.Exporter {
	case "otlp":
		endpoint := cfg.Endpoint
		if endpoint == "" {
			endpoint = "localhost:4318"
		}
		exporter, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
	case "", "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdout exporter: %w", err)
		}
	default:
		slog.Warn("tracing: unknown exporter, falling back to stdout", "exporter", cfg.Exporter)
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdout exporter: %w", err)
		}
	}

	sampleRate := cfg.SampleRate
	if sampleRate <= 0 {
		sampleRate = 0.1
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRate))),
	)

	otel.SetTracerProvider(tp)
	Tracer = tp.Tracer(serviceName)

	return tp.Shutdown, nil
}

// StartSpan creates a new span from the YASE tracer.
func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name)
}
