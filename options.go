package amqpadapter

import (
	"context"
	"fmt"
	"time"
)

// FailedJob описывает задачу, не обработанную после исчерпания retry.
type FailedJob struct {
	Queue QueueName
	Body  []byte
	Err   error
}

// FailJobHandler вызывается при окончательном сбое обработки сообщения в retry-режиме.
type FailJobHandler func(job FailedJob) error

type options struct {
	lgr                     Logger
	failHandler             FailJobHandler
	publishHeadersBuilder   PublishHeadersBuilder
	consumeHeadersExtractor ConsumeHeadersExtractor
}

// Option настраивает Queue при создании через New.
type Option func(*options)

// WithLogger задаёт логгер; по умолчанию используется noop-логгер.
func WithLogger(lgr Logger) Option {
	return func(o *options) {
		if lgr != nil {
			o.lgr = lgr
		}
	}
}

// WithFailHandler задаёт обработчик окончательно проваленных задач (обязателен при Retry в QueueItem).
func WithFailHandler(h FailJobHandler) Option {
	return func(o *options) {
		o.failHandler = h
	}
}

// WithPublishHeadersBuilder задаёт функцию формирования AMQP headers при публикации.
func WithPublishHeadersBuilder(b PublishHeadersBuilder) Option {
	return func(o *options) {
		if b != nil {
			o.publishHeadersBuilder = b
		}
	}
}

// WithConsumeHeadersExtractor задаёт функцию извлечения context из AMQP headers при потреблении.
func WithConsumeHeadersExtractor(e ConsumeHeadersExtractor) Option {
	return func(o *options) {
		if e != nil {
			o.consumeHeadersExtractor = e
		}
	}
}

func defaultOptions() options {
	return options{
		lgr:         nilLogger{},
		failHandler: nil,
		publishHeadersBuilder: func(context.Context) map[string]any {
			return map[string]any{}
		},
		consumeHeadersExtractor: func(parent context.Context, _ map[string]any) context.Context {
			return parent
		},
	}
}

func validateConfig(conf Config, opts options) error {
	if conf.URL == "" {
		return ErrEmptyURL
	}

	if conf.ReconnectDelay <= 0 {
		return fmt.Errorf("%w: ReconnectDelay", ErrInvalidDelay)
	}

	if conf.ReInitDelay <= 0 {
		return fmt.Errorf("%w: ReInitDelay", ErrInvalidDelay)
	}

	if conf.ResendDelay <= 0 {
		return fmt.Errorf("%w: ResendDelay", ErrInvalidDelay)
	}

	if conf.QueueParams == nil {
		return nil
	}

	for name, item := range conf.QueueParams {
		err := validateRetryQueue(name, item, opts)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateRetryQueue(name QueueName, item QueueItem, opts options) error {
	if item.Retry == nil {
		return nil
	}

	if opts.failHandler == nil {
		return fmt.Errorf("%w: %s", ErrFailHandlerRequired, name)
	}

	if item.Retry.Delay < time.Millisecond {
		return fmt.Errorf("%w: %s Delay", ErrInvalidRetryConfig, name)
	}

	if item.Retry.MaxDuration <= 0 {
		return fmt.Errorf("%w: %s MaxDuration", ErrInvalidRetryConfig, name)
	}

	return nil
}
