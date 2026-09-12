package functional

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/amqp-adapter"
)

func TestReconnect_AfterRabbitMQRestart(t *testing.T) {
	requireFunctional(t)
	requireDocker(t)

	queueName := uniqueQueueName(t, "reconnect")
	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {},
	}, testQueueOptions{
		// failHandler не нужен — retry выключен
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, data []byte) error {
		t.Logf("получено сообщение: %q", data)
		processed.Add(1)

		return nil
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	// Сообщение до остановки RabbitMQ.
	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte("before-stop")}, 20*time.Second)

	waitUntil(t, 15*time.Second, func() bool {
		return processed.Load() >= 1
	}, "обработка сообщения до остановки RabbitMQ")

	t.Log("остановка RabbitMQ...")
	runCompose(t, "stop", "rabbitmq")

	time.Sleep(2 * time.Second)

	t.Log("запуск RabbitMQ...")
	runCompose(t, "start", "rabbitmq")
	waitRabbitReady(t, 90*time.Second)

	// Даём клиенту время на переподключение.
	time.Sleep(3 * time.Second)

	// Сообщение после восстановления RabbitMQ.
	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte("after-start")}, 60*time.Second)

	waitUntil(t, 60*time.Second, func() bool {
		return processed.Load() >= 2
	}, "обработка сообщения после переподключения")

	if got := processed.Load(); got < 2 {
		t.Fatalf("обработано %d сообщений, want >= 2", got)
	}
}
