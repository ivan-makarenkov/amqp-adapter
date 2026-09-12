package amqpadapter

import (
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const testRabbitURL = "amqp://guest:guest@127.0.0.1:5672/"

func requireRabbitMQ(t *testing.T) *amqp.Connection {
	t.Helper()

	conn, err := amqp.Dial(testRabbitURL)
	if err == nil {
		return conn
	}

	if os.Getenv("MQ_REQUIRE_INTEGRATION") == "1" {
		t.Fatalf("RabbitMQ недоступен: %v", err)
	}

	t.Skipf("пропуск теста: не удалось подключиться к RabbitMQ: %v", err)
	return nil
}

func TestDateFormat(t *testing.T) {
	t.Run("RFC3339 формат", func(t *testing.T) {
		nowUTC := time.Now().UTC()
		formatted := nowUTC.Format(time.RFC3339)

		parsed, err := parseExpiredTime(formatted)
		if err != nil {
			t.Fatalf("parseExpiredTime() error = %v", err)
		}

		diff := nowUTC.Sub(parsed)
		if diff < 0 {
			diff = -diff
		}
		if diff > time.Second {
			t.Errorf("разница во времени UTC = %v, want < 1s", diff)
		}
	})

	t.Run("legacy формат обратная совместимость", func(t *testing.T) {
		legacy := "2030-01-01 00:00:00"

		parsed, err := parseExpiredTime(legacy)
		if err != nil {
			t.Fatalf("parseExpiredTime() error = %v", err)
		}

		if parsed.Year() != 2030 {
			t.Errorf("год = %d, want 2030", parsed.Year())
		}
	})
}

func TestPublishMessagePriority(t *testing.T) {
	t.Run("валидный приоритет", func(t *testing.T) {
		priority := PublishMessagePriority(9)
		if priority != 9 {
			t.Errorf("PublishMessagePriority = %v, want 9", priority)
		}
	})
}

// TestInitQueue_Integration интеграционный тест для initQueue.
// Требует запущенного RabbitMQ на localhost:5672.
func TestInitQueue_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("пропуск интеграционного теста в коротком режиме")
	}

	conn := requireRabbitMQ(t)
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("не удалось создать канал: %v", err)
	}
	defer channel.Close()

	queueName := "test_queue_" + time.Now().Format("20060102150405")
	queue, err := initQueue(queueName, channel)
	if err != nil {
		t.Fatalf("initQueue() error = %v", err)
	}

	if queue.Name != queueName {
		t.Errorf("initQueue() queue.Name = %v, want %v", queue.Name, queueName)
	}

	_, err = channel.QueueDelete(queueName, false, false, false)
	if err != nil {
		t.Logf("не удалось удалить очередь: %v", err)
	}
}

// TestInitQueueWithRetry_Integration интеграционный тест для initQueueWithRetry.
// Требует запущенного RabbitMQ на localhost:5672.
func TestInitQueueWithRetry_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("пропуск интеграционного теста в коротком режиме")
	}

	conn := requireRabbitMQ(t)
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("не удалось создать канал: %v", err)
	}
	defer channel.Close()

	queueName := "test_queue_retry_" + time.Now().Format("20060102150405")
	retryDelay := 1 * time.Minute

	queue, err := initQueueWithRetry(queueName, channel, retryDelay)
	if err != nil {
		t.Fatalf("initQueueWithRetry() error = %v", err)
	}

	if queue.Name != queueName {
		t.Errorf("initQueueWithRetry() queue.Name = %v, want %v", queue.Name, queueName)
	}

	_, err = channel.QueueDelete(queueName, false, false, false)
	if err != nil {
		t.Logf("не удалось удалить очередь: %v", err)
	}
	_, err = channel.QueueDelete(queueName+".delay", false, false, false)
	if err != nil {
		t.Logf("не удалось удалить очередь delay: %v", err)
	}
}
