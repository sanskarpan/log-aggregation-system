package tracing

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	ServiceName string
	Enabled     bool
	Exporter    string
	Endpoint    string
	Insecure    bool
}

func Setup(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := buildExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}

func WrapHandler(serviceName string, next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}
	tracer := otel.Tracer(serviceName)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		spanName := fmt.Sprintf("%s %s", r.Method, r.URL.Path)
		ctx, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))
		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.path", r.URL.Path),
			attribute.Int("http.status_code", rec.status),
		)
		if rec.status >= http.StatusInternalServerError {
			span.RecordError(fmt.Errorf("http %s %s returned %d", r.Method, r.URL.Path, rec.status))
		}
	})
}

func buildExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	if strings.EqualFold(cfg.Exporter, "otlp") && strings.TrimSpace(cfg.Endpoint) != "" {
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, opts...)
	}
	return stdouttrace.New(stdouttrace.WithPrettyPrint())
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}
