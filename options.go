package amqpadapter

import (
	"context"
	"fmt"
	"time"
)

// FailedJob describes a job that failed after retries were exhausted.
type FailedJob struct {
	Queue QueueName
	Body  []byte
	Err   error
}

// FailJobHandler is called on permanent failure in retry mode.
type FailJobHandler func(job FailedJob) error

type options struct {
	lgr                     Logger
	failHandler             FailJobHandler
	publishHeadersBuilder   PublishHeadersBuilder
	consumeHeadersExtractor ConsumeHeadersExtractor
}

// Option configures Queue at New time.
type Option func(*options)

// WithLogger sets the logger; the default is a noop logger.
func WithLogger(lgr Logger) Option {
	return func(o *options) {
		if lgr != nil {
			o.lgr = lgr
		}
	}
}

// WithFailHandler sets the permanent-failure handler (required when QueueItem.Retry is set).
func WithFailHandler(h FailJobHandler) Option {
	return func(o *options) {
		o.failHandler = h
	}
}

// WithPublishHeadersBuilder sets the function that builds AMQP headers on publish.
func WithPublishHeadersBuilder(b PublishHeadersBuilder) Option {
	return func(o *options) {
		if b != nil {
			o.publishHeadersBuilder = b
		}
	}
}

// WithConsumeHeadersExtractor sets the function that restores context from AMQP headers on consume.
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
