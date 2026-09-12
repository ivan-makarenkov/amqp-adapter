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
		t.Fatalf("RabbitMQ unavailable: %v", err)
	}

	t.Skipf("skipping test: failed to connect to RabbitMQ: %v", err)
	return nil
}

func TestDateFormat(t *testing.T) {
	t.Run("RFC3339 format", func(t *testing.T) {
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
			t.Errorf("UTC time diff = %v, want < 1s", diff)
		}
	})

	t.Run("legacy format backwards compatibility", func(t *testing.T) {
		legacy := "2030-01-01 00:00:00"

		parsed, err := parseExpiredTime(legacy)
		if err != nil {
			t.Fatalf("parseExpiredTime() error = %v", err)
		}

		if parsed.Year() != 2030 {
			t.Errorf("year = %d, want 2030", parsed.Year())
		}
	})
}

func TestPublishMessagePriority(t *testing.T) {
	t.Run("valid priority", func(t *testing.T) {
		priority := PublishMessagePriority(9)
		if priority != 9 {
			t.Errorf("PublishMessagePriority = %v, want 9", priority)
		}
	})
}

// TestInitQueue_Integration is an integration test for initQueue.
// Requires RabbitMQ on localhost:5672.
func TestInitQueue_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	conn := requireRabbitMQ(t)
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
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
		t.Logf("failed to delete queue: %v", err)
	}
}

// TestInitQueueWithRetry_Integration is an integration test for initQueueWithRetry.
// Requires RabbitMQ on localhost:5672.
func TestInitQueueWithRetry_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	conn := requireRabbitMQ(t)
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to create channel: %v", err)
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
		t.Logf("failed to delete queue: %v", err)
	}
	_, err = channel.QueueDelete(queueName+".delay", false, false, false)
	if err != nil {
		t.Logf("failed to delete delay queue: %v", err)
	}
}
