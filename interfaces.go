package amqpadapter

import (
	"context"
	"time"
)

// QueueName is a typed RabbitMQ queue name.
type QueueName string

// Logger is a minimal logging interface with slog-style method names.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
}

// PublishMessagePriority is the RabbitMQ message priority (0–9).
type PublishMessagePriority uint8

// DefaultContentType is the default content type for published messages.
const DefaultContentType = "text/plain"

// PublishMessage describes the body and options of a published message.
type PublishMessage struct {
	Body             []byte
	ContentType      string
	MaxRetryDuration *time.Duration
	Priority         *PublishMessagePriority
}

// PublishHeadersBuilder builds AMQP headers from context at publish time.
type PublishHeadersBuilder func(ctx context.Context) map[string]any

// ConsumeHeadersExtractor restores context from AMQP headers on consume.
// parent is the consumer-loop context (from InitConsumer); the returned context
// must be derived from it.
type ConsumeHeadersExtractor func(parent context.Context, headers map[string]any) context.Context

// ConsumerHandler processes a message body.
// Return mq.Retry(err) to request delayed reprocessing.
type ConsumerHandler func(ctx context.Context, data []byte) error

// Queue is the public contract for publish, consume registration, and shutdown.
type Queue interface {
	Publish(ctx context.Context, queue QueueName, msg PublishMessage) error
	AddConsumer(ctx context.Context, queue QueueName, handler ConsumerHandler) error
	AddConsumerN(ctx context.Context, queue QueueName, parallelism int, handler ConsumerHandler) error
	InitConsumer(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
