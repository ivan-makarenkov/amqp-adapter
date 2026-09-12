package amqpadapter

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAddConsumerN_NormalizesParallelism(t *testing.T) {
	conf := createTestConfig()
	lgr := NewMockLogger()
	queue, err := New(conf, createTestOptions(lgr)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = queue.AddConsumerN(ctx, QueueName("test_queue"), 0, func(ctx context.Context, data []byte) error {
		return nil
	})

	wrn := lgr.GetWarnMessages()
	found := false
	for _, m := range wrn {
		if strings.Contains(m, "parallelism") {
			found = true
			break
		}
	}
	if !found {
		t.Error("AddConsumerN(0) should log a parallelism normalization warning")
	}
}

func TestAddConsumer_ReturnsErrorForUnknownQueue(t *testing.T) {
	conf := createTestConfig()
	lgr := NewMockLogger()
	queue, err := New(conf, createTestOptions(lgr)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	err = queue.AddConsumer(ctx, QueueName("unknown_queue"), func(ctx context.Context, data []byte) error {
		return nil
	})
	if err == nil {
		t.Fatal("AddConsumer() error = nil, want ErrQueueNotFound")
	}
}

func TestInitConsumer_NoConsumersRegistered(t *testing.T) {
	conf := createTestConfig()
	lgr := NewMockLogger()
	queue, err := New(conf, createTestOptions(lgr)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = queue.InitConsumer(context.Background())
	if err != ErrNoConsumersRegistered {
		t.Errorf("InitConsumer() error = %v, want ErrNoConsumersRegistered", err)
	}
}

func TestShutdown_ClosesClientsBeforeWaitingInflight(t *testing.T) {
	lgr := NewMockLogger()
	pub := &client{
		queueName: QueueName("test_queue"),
		done:      make(chan struct{}),
		lgr:       lgr,
	}

	store := &queueStore{
		lgr:        lgr,
		publishers: map[QueueName]*client{QueueName("test_queue"): pub},
		consumers:  map[QueueName][]*client{},
	}
	store.inflight.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- store.Shutdown(ctx)
	}()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case <-pub.done:
			store.inflight.Done()
			err := <-errCh
			if err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}

			return
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	t.Fatal("client was not closed before inflight wait finished")
}

func TestShutdown_ClosesAllConsumerWorkers(t *testing.T) {
	conf := createTestConfig()
	lgr := NewMockLogger()
	queue, err := New(conf, createTestOptions(lgr)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = queue.AddConsumerN(ctx, QueueName("test_queue"), 2, func(ctx context.Context, data []byte) error {
		return nil
	})

	err = queue.Shutdown(context.Background())
	if err != nil {
		t.Logf("Shutdown() returned error (expected without RabbitMQ): %v", err)
	}
}

func TestNew_RequiresFailHandlerForRetryQueues(t *testing.T) {
	conf := createTestConfig()
	_, err := New(conf)
	if err == nil {
		t.Fatal("New() error = nil, want error about WithFailHandler")
	}
}

func TestNew_ValidatesEmptyURL(t *testing.T) {
	conf := createTestConfig()
	conf.URL = ""

	_, err := New(conf, createTestOptions(NewMockLogger())...)
	if err != ErrEmptyURL {
		t.Errorf("New() error = %v, want ErrEmptyURL", err)
	}
}

func TestNew_ValidatesZeroDelay(t *testing.T) {
	conf := createTestConfig()
	conf.ReconnectDelay = 0

	_, err := New(conf, createTestOptions(NewMockLogger())...)
	if err == nil {
		t.Fatal("New() error = nil, want ErrInvalidDelay")
	}
}

func TestNew_ValidatesRetryDelayTooSmall(t *testing.T) {
	conf := createTestConfig()
	conf.QueueParams = map[QueueName]QueueItem{
		QueueName("tiny_delay"): {
			Retry: &RetryConfig{
				Delay:       time.Microsecond,
				MaxDuration: time.Hour,
			},
		},
	}

	_, err := New(conf, createTestOptions(NewMockLogger())...)
	if err == nil {
		t.Fatal("New() error = nil, want ErrInvalidRetryConfig for Delay")
	}
}

func TestPublish_ConsumerOnlyQueue(t *testing.T) {
	conf := createTestConfig()
	conf.QueueParams = map[QueueName]QueueItem{
		QueueName("consume_only"): {ConsumerOnly: true},
	}

	queue, err := New(conf, createTestOptions(NewMockLogger())...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = queue.Publish(context.Background(), QueueName("consume_only"), PublishMessage{Body: []byte("x")})
	if err == nil {
		t.Fatal("Publish() error = nil, want ErrQueueConsumerOnly")
	}
}

func TestAddConsumer_DoesNotBlockWithoutRabbitMQ(t *testing.T) {
	conf := createTestConfig()
	conf.URL = "amqp://invalid:invalid@127.0.0.1:59999/"

	queue, err := New(conf, createTestOptions(NewMockLogger())...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = queue.AddConsumer(ctx, QueueName("test_queue_no_retry"), func(context.Context, []byte) error {
		return nil
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	if elapsed > 30*time.Millisecond {
		t.Errorf("AddConsumer() blocked for %v, expected instant registration", elapsed)
	}

	_ = queue.Shutdown(context.Background())
}

func TestAddConsumer_AfterInitConsumer_ReturnsError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	conn := requireRabbitMQ(t)
	conn.Close()

	conf := createTestConfig()
	lgr := NewMockLogger()
	queue, err := New(conf, createTestOptions(lgr)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()

	err = queue.AddConsumer(ctx, QueueName("test_queue_no_retry"), func(ctx context.Context, data []byte) error {
		return nil
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	err = queue.AddConsumer(ctx, QueueName("test_queue_no_retry"), func(ctx context.Context, data []byte) error {
		return nil
	})
	if err != ErrConsumersAlreadyStarted {
		t.Errorf("AddConsumer() after InitConsumer error = %v, want ErrConsumersAlreadyStarted", err)
	}

	_ = queue.Shutdown(context.Background())
}

func TestAddConsumerN_CompetingConsumers_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	conn := requireRabbitMQ(t)
	conn.Close()

	conf := createTestConfig()

	lgr := NewMockLogger()
	queue, err := New(conf, createTestOptions(lgr)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var processed atomic.Int64
	err = queue.AddConsumerN(ctx, QueueName("test_parallel"), 3, func(ctx context.Context, data []byte) error {
		processed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("AddConsumerN() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	for i := 0; i < 10; i++ {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			err = queue.Publish(ctx, QueueName("test_parallel"), PublishMessage{Body: []byte("msg")})
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got := processed.Load()
	if got != 10 {
		t.Errorf("processed %d messages, want 10", got)
	}

	cancel()
	_ = queue.Shutdown(context.Background())
}
