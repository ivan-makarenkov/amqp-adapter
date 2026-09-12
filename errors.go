package amqpadapter

import "errors"

// ErrClosingQueueConnection ошибка при закрытии соединения с очередью.
var ErrClosingQueueConnection = errors.New("ошибка при закрытии соединения с очередью")

// ErrClosingQueueChannel ошибка при закрытии канала очереди.
var ErrClosingQueueChannel = errors.New("ошибка при закрытии канала очереди")

// ErrStartingQueueConsumption ошибка при начале чтения сообщений из очереди.
var ErrStartingQueueConsumption = errors.New("ошибка при начале чтения сообщений из очереди")

// ErrPublishFailed ошибка при публикации сообщения в очередь.
var ErrPublishFailed = errors.New("ошибка при публикации сообщения в очередь")

// ErrPublishNack брокер отклонил публикацию (publisher confirm nack).
var ErrPublishNack = errors.New("брокер отклонил публикацию сообщения")

// ErrEmptyURL не задан URL подключения к RabbitMQ.
var ErrEmptyURL = errors.New("не задан URL подключения к RabbitMQ")

// ErrInvalidDelay некорректная задержка в конфигурации.
var ErrInvalidDelay = errors.New("задержка в конфигурации должна быть больше нуля")

// ErrInvalidRetryConfig некорректные параметры retry.
var ErrInvalidRetryConfig = errors.New("некорректные параметры retry")

// ErrInvalidPriority приоритет сообщения вне диапазона 0–9.
var ErrInvalidPriority = errors.New("приоритет сообщения должен быть в диапазоне 0–9")

// ErrPushQueueConnectionClosed при публикации закрыто соединение с очередью.
var ErrPushQueueConnectionClosed = errors.New("при публикации закрыто соединение с очередью")

// ErrConsumeQueueConnectionClosed при чтении закрыто соединение с очередью.
var ErrConsumeQueueConnectionClosed = errors.New("при чтении закрыто соединение с очередью")

// ErrOpenChannel не удалось открыть канал к очереди.
var ErrOpenChannel = errors.New("не удалось открыть канал к очереди")

// ErrEnableConfirms не удалось включить подтверждение публикации.
var ErrEnableConfirms = errors.New("не удалось включить подтверждение публикации")

// ErrConnectToQueue не удалось подключиться к очереди.
var ErrConnectToQueue = errors.New("не удалось подключиться к очереди по адресу")

// ErrDoneSignalBeforeAck пришел сигнал done до подтверждения публикации в очередь.
var ErrDoneSignalBeforeAck = errors.New("пришел сигнал done до подтверждения публикации в очередь")

// ErrContextCanceledBeforeAck контекст завершен до подтверждения публикации в очередь.
var ErrContextCanceledBeforeAck = errors.New("контекст завершен до подтверждения публикации в очередь")

// ErrBindQueueToExchange ошибка при связывании очереди и обменника.
var ErrBindQueueToExchange = errors.New("ошибка при связывании очереди и обменника")

// ErrQueueNotFound очередь не найдена в конфигурации.
var ErrQueueNotFound = errors.New("очередь не найдена в конфигурации")

// ErrNoConsumersRegistered consumer'ы не зарегистрированы.
var ErrNoConsumersRegistered = errors.New("consumer'ы не зарегистрированы")

// ErrConsumersAlreadyStarted InitConsumer уже был вызван.
var ErrConsumersAlreadyStarted = errors.New("consumer'ы уже запущены")

// ErrQueueConnectionClosed соединение с очередью закрыто.
var ErrQueueConnectionClosed = errors.New("соединение с очередью закрыто")

// ErrClientClosedBeforeReady клиент закрыт до готовности.
var ErrClientClosedBeforeReady = errors.New("клиент закрыт до готовности")

// ErrFailHandlerRequired не передан WithFailHandler для очереди с retry.
var ErrFailHandlerRequired = errors.New("для очереди включён retry, но не передан WithFailHandler")

// ErrQueueConsumerOnly очередь настроена только для потребления.
var ErrQueueConsumerOnly = errors.New("очередь настроена только для потребления (ConsumerOnly)")

// ErrContextCanceledOnConsumerStart контекст отменён при запуске consumer'ов.
var ErrContextCanceledOnConsumerStart = errors.New("контекст отменён при запуске consumer'ов")
