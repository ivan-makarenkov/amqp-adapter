package amqpadapter

import "time"

// Config holds RabbitMQ connection settings and per-queue parameters.
type Config struct {
	URL            string
	ReconnectDelay time.Duration
	ReInitDelay    time.Duration
	ResendDelay    time.Duration
	QueueParams    map[QueueName]QueueItem
}

// RetryConfig describes retry parameters for a queue.
type RetryConfig struct {
	Delay       time.Duration
	MaxDuration time.Duration
}

// QueueItem describes processing options for a single queue.
type QueueItem struct {
	// Retry enables retry mode; nil means no retry.
	Retry *RetryConfig
	// ConsumerOnly means consume-only (no publisher connection).
	ConsumerOnly bool
}
