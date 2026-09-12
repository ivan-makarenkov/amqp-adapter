// Package otel предоставляет готовые PublishHeadersBuilder и ConsumeHeadersExtractor
// для propagation correlation ID и OpenTelemetry trace context через AMQP headers.
package otel

import (
	"context"

	"github.com/ivan-makarenkov/amqp-adapter"
	"go.opentelemetry.io/otel/propagation"
)

// PropagationConfig задаёт ключи headers для correlation и trace context.
type PropagationConfig struct {
	CorrelationIDKey string
	TraceKeys        []string
	GetCorrelationID func(ctx context.Context) string
	SetCorrelationID func(ctx context.Context, value string) context.Context
	Propagator       propagation.TextMapPropagator
}

// NewPublishHeadersBuilder создаёт builder для записи correlation и trace headers при публикации.
func NewPublishHeadersBuilder(cfg PropagationConfig) amqpadapter.PublishHeadersBuilder {
	propagator := cfg.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator()
	}

	return func(ctx context.Context) map[string]any {
		headers := make(map[string]any)

		if cfg.GetCorrelationID != nil && cfg.CorrelationIDKey != "" {
			if value := cfg.GetCorrelationID(ctx); value != "" {
				headers[cfg.CorrelationIDKey] = value
			}
		}

		if len(cfg.TraceKeys) > 0 {
			carrier := propagation.MapCarrier{}
			propagator.Inject(ctx, carrier)
			for _, key := range cfg.TraceKeys {
				if value := carrier.Get(key); value != "" {
					headers[key] = value
				}
			}
		}

		return headers
	}
}

// NewConsumeHeadersExtractor создаёт extractor для восстановления context из AMQP headers.
func NewConsumeHeadersExtractor(cfg PropagationConfig) amqpadapter.ConsumeHeadersExtractor {
	propagator := cfg.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator()
	}

	return func(parent context.Context, headers map[string]any) context.Context {
		ctx := parent

		if cfg.SetCorrelationID != nil && cfg.CorrelationIDKey != "" {
			if value, ok := headers[cfg.CorrelationIDKey].(string); ok && value != "" {
				ctx = cfg.SetCorrelationID(ctx, value)
			}
		}

		if len(cfg.TraceKeys) > 0 {
			carrier := propagation.MapCarrier{}
			for _, key := range cfg.TraceKeys {
				if value, ok := headers[key].(string); ok && value != "" {
					carrier.Set(key, value)
				}
			}
			if len(carrier) > 0 {
				ctx = propagator.Extract(ctx, carrier)
			}
		}

		return ctx
	}
}
