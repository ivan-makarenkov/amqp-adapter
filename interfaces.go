package amqpadapter

import (
	"context"
	"time"
)

// QueueName — типизированное имя очереди RabbitMQ.
type QueueName string

// Logger предоставляет унифицированный интерфейс для логирования сообщений
// с разными уровнями важности (от отладочных до фатальных),
// а также поддержку добавления мета- и контекстной информации.
type Logger interface {
	Inf(msg string, args ...any)
	Wrn(msg string, args ...any)
	Dbg(msg string, args ...any)
	Err(msg string, args ...any)
	Ftl(msg string, args ...any)
	InfCtx(ctx context.Context, msg string, args ...any)
	WrnCtx(ctx context.Context, msg string, args ...any)
	DbgCtx(ctx context.Context, msg string, args ...any)
	ErrCtx(ctx context.Context, msg string, args ...any)
	FtlCtx(ctx context.Context, msg string, args ...any)
}

// PublishMessagePriority приоритет сообщения в RabbitMQ.
type PublishMessagePriority uint8

// DefaultContentType тип содержимого по умолчанию для публикуемых сообщений.
const DefaultContentType = "text/plain"

// PublishMessage описывает тело и опции публикуемого сообщения.
type PublishMessage struct {
	Body             []byte
	ContentType      string
	MaxRetryDuration *time.Duration
	Priority         *PublishMessagePriority
}

// PublishHeadersBuilder формирует AMQP headers при публикации из context.
type PublishHeadersBuilder func(ctx context.Context) map[string]any

// ConsumeHeadersExtractor восстанавливает context из AMQP headers при потреблении.
// parent — контекст consumer loop (из InitConsumer); возвращённый контекст должен быть его потомком.
type ConsumeHeadersExtractor func(parent context.Context, headers map[string]any) context.Context

// ConsumerHandler обрабатывает тело сообщения.
// Для повторной обработки верните mq.Retry(err).
type ConsumerHandler func(ctx context.Context, data []byte) error

// Queue определяет контракт для работы с RabbitMQ:
// публикация, регистрация обработчиков, инициализация потребителей и корректное завершение работы.
type Queue interface {
	Publish(ctx context.Context, queue QueueName, msg PublishMessage) error
	AddConsumer(ctx context.Context, queue QueueName, handler ConsumerHandler) error
	AddConsumerN(ctx context.Context, queue QueueName, parallelism int, handler ConsumerHandler) error
	InitConsumer(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
