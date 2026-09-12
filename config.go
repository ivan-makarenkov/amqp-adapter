package amqpadapter

import "time"

// Config содержит настройки подключения к RabbitMQ и параметры очередей.
type Config struct {
	URL            string
	ReconnectDelay time.Duration
	ReInitDelay    time.Duration
	ResendDelay    time.Duration
	QueueParams    map[QueueName]QueueItem
}

// RetryConfig описывает параметры retry-механизма для очереди.
type RetryConfig struct {
	Delay       time.Duration
	MaxDuration time.Duration
}

// QueueItem описывает параметры обработки для отдельной очереди.
type QueueItem struct {
	// Retry включает retry-режим; nil — без retry.
	Retry *RetryConfig
	// ConsumerOnly — только потребление, без publisher-соединения.
	ConsumerOnly bool
}
