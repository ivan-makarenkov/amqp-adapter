package functional

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/amqp-adapter"
)

func TestRetry_MessageRetriedUntilSuccess(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "retry_ok")
	const retryDelay = 2 * time.Second

	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {
			Retry: &mq.RetryConfig{
				Delay:       retryDelay,
				MaxDuration: 2 * time.Minute,
			},
		},
	}, testQueueOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const wantSuccessOnAttempt = 3

	var attempts atomic.Int64
	var succeeded atomic.Bool

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, data []byte) error {
		attempt := attempts.Add(1)
		t.Logf("попытка обработки #%d, body=%q", attempt, data)

		if attempt < wantSuccessOnAttempt {
			return mq.Retry(errors.New("временная ошибка"))
		}

		succeeded.Store(true)

		return nil
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte("retry-me")}, 15*time.Second)

	// Две задержки retry + запас на обработку.
	waitUntil(t, retryDelay*time.Duration(wantSuccessOnAttempt)+10*time.Second, func() bool {
		return succeeded.Load()
	}, "успешная обработка после retry")

	if got := attempts.Load(); got < wantSuccessOnAttempt {
		t.Fatalf("попыток обработки %d, want >= %d", got, wantSuccessOnAttempt)
	}
}

func TestRetry_PermanentErrorCallsFailHandler(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "retry_permanent")
	const retryDelay = 1 * time.Second

	var failedJobs atomic.Int64

	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {
			Retry: &mq.RetryConfig{
				Delay:       retryDelay,
				MaxDuration: 5 * time.Second,
			},
		},
	}, testQueueOptions{
		failHandler: func(job mq.FailedJob) error {
			failedJobs.Add(1)
			t.Logf("fail handler: queue=%s body=%q err=%v", job.Queue, job.Body, job.Err)

			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, _ []byte) error {
		// Неповторяемая ошибка — сразу в fail handler.
		return errors.New("неисправимая ошибка")
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte("will-fail")}, 15*time.Second)

	waitUntil(t, 10*time.Second, func() bool {
		return failedJobs.Load() >= 1
	}, "вызов fail handler при неповторяемой ошибке")

	if got := failedJobs.Load(); got < 1 {
		t.Fatalf("fail handler вызван %d раз, want >= 1", got)
	}
}

func TestRetry_ExhaustedCallsFailHandler(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "retry_fail")
	const retryDelay = 1 * time.Second

	var failedJobs atomic.Int64

	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {
			Retry: &mq.RetryConfig{
				Delay:       retryDelay,
				MaxDuration: 5 * time.Second,
			},
		},
	}, testQueueOptions{
		failHandler: func(job mq.FailedJob) error {
			failedJobs.Add(1)
			t.Logf("fail handler: queue=%s body=%q err=%v", job.Queue, job.Body, job.Err)

			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, _ []byte) error {
		return mq.Retry(errors.New("постоянная ошибка"))
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	maxRetry := 3 * time.Second
	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{
		Body:             []byte("will-fail"),
		MaxRetryDuration: &maxRetry,
	}, 15*time.Second)

	// Ждём истечения MaxRetryDuration + несколько циклов delay-очереди.
	waitUntil(t, 20*time.Second, func() bool {
		return failedJobs.Load() >= 1
	}, "вызов fail handler после исчерпания retry")

	if got := failedJobs.Load(); got < 1 {
		t.Fatalf("fail handler вызван %d раз, want >= 1", got)
	}
}
