package amqpadapter

import "errors"

// ErrClosingQueueConnection is returned when closing the queue connection fails.
var ErrClosingQueueConnection = errors.New("error closing queue connection")

// ErrClosingQueueChannel is returned when closing the queue channel fails.
var ErrClosingQueueChannel = errors.New("error closing queue channel")

// ErrStartingQueueConsumption is returned when starting consume fails.
var ErrStartingQueueConsumption = errors.New("error starting queue consumption")

// ErrPublishFailed is returned when publishing a message fails.
var ErrPublishFailed = errors.New("error publishing message to queue")

// ErrPublishNack is returned when the broker nacks a publish (publisher confirm).
var ErrPublishNack = errors.New("broker nacked the published message")

// ErrEmptyURL is returned when the RabbitMQ URL is empty.
var ErrEmptyURL = errors.New("RabbitMQ URL is not set")

// ErrInvalidDelay is returned when a configured delay is not positive.
var ErrInvalidDelay = errors.New("config delay must be greater than zero")

// ErrInvalidRetryConfig is returned when retry parameters are invalid.
var ErrInvalidRetryConfig = errors.New("invalid retry configuration")

// ErrInvalidPriority is returned when message priority is outside 0–9.
var ErrInvalidPriority = errors.New("message priority must be in range 0–9")

// ErrPushQueueConnectionClosed is returned when publishing on a closed connection.
var ErrPushQueueConnectionClosed = errors.New("queue connection closed during publish")

// ErrConsumeQueueConnectionClosed is returned when consuming on a closed connection.
var ErrConsumeQueueConnectionClosed = errors.New("queue connection closed during consume")

// ErrOpenChannel is returned when opening an AMQP channel fails.
var ErrOpenChannel = errors.New("failed to open queue channel")

// ErrEnableConfirms is returned when enabling publisher confirms fails.
var ErrEnableConfirms = errors.New("failed to enable publisher confirms")

// ErrConnectToQueue is returned when dialing the broker fails.
var ErrConnectToQueue = errors.New("failed to connect to queue at address")

// ErrDoneSignalBeforeAck is returned when the client is closed before publish confirm.
var ErrDoneSignalBeforeAck = errors.New("done signal received before publish confirm")

// ErrContextCanceledBeforeAck is returned when the context is canceled before publish confirm.
var ErrContextCanceledBeforeAck = errors.New("context canceled before publish confirm")

// ErrBindQueueToExchange is returned when binding a queue to an exchange fails.
var ErrBindQueueToExchange = errors.New("error binding queue to exchange")

// ErrQueueNotFound is returned when the queue is missing from configuration.
var ErrQueueNotFound = errors.New("queue not found in configuration")

// ErrNoConsumersRegistered is returned when InitConsumer is called with no consumers.
var ErrNoConsumersRegistered = errors.New("no consumers registered")

// ErrConsumersAlreadyStarted is returned when InitConsumer was already called.
var ErrConsumersAlreadyStarted = errors.New("consumers already started")

// ErrQueueConnectionClosed is returned when the queue connection is closed.
var ErrQueueConnectionClosed = errors.New("queue connection closed")

// ErrClientClosedBeforeReady is returned when the client is closed before becoming ready.
var ErrClientClosedBeforeReady = errors.New("client closed before ready")

// ErrFailHandlerRequired is returned when retry is enabled but WithFailHandler is missing.
var ErrFailHandlerRequired = errors.New("retry enabled but WithFailHandler was not provided")

// ErrQueueConsumerOnly is returned when publishing to a ConsumerOnly queue.
var ErrQueueConsumerOnly = errors.New("queue is configured for consume only (ConsumerOnly)")

// ErrContextCanceledOnConsumerStart is returned when the context is canceled during InitConsumer.
var ErrContextCanceledOnConsumerStart = errors.New("context canceled while starting consumers")
