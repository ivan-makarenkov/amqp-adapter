package amqpadapter

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryableError(t *testing.T) {
	root := errors.New("root")
	err := Retry(root)

	if !IsRetryable(err) {
		t.Error("IsRetryable() = false, want true")
	}

	var re *RetryableError
	if !errors.As(err, &re) {
		t.Fatal("errors.As() = false, want true")
	}
	if !errors.Is(err, root) {
		t.Error("errors.Is(root) = false, want true")
	}
}

func TestClient_withRetry(t *testing.T) {
	tests := []struct {
		name  string
		retry *RetryConfig
		want  bool
	}{
		{
			name:  "with retry mode",
			retry: &RetryConfig{Delay: 1 * time.Minute},
			want:  true,
		},
		{
			name:  "without retry mode",
			retry: nil,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clnt := &client{
				retry: tt.retry,
			}
			if got := clnt.withRetry(); got != tt.want {
				t.Errorf("withRetry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_isExpired(t *testing.T) {
	tests := []struct {
		name        string
		expiredTime string
		want        bool
	}{
		{
			name:        "empty string is expired",
			expiredTime: "",
			want:        true,
		},
		{
			name:        "invalid format is expired",
			expiredTime: "invalid-format",
			want:        true,
		},
		{
			name:        "past time is expired",
			expiredTime: "2000-01-01 00:00:00",
			want:        true,
		},
		{
			name:        "future time is not expired",
			expiredTime: time.Now().Add(1 * time.Hour).Format(time.RFC3339),
			want:        false,
		},
		{
			name:        "fixed past time is expired",
			expiredTime: "2020-01-01 00:00:00",
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clnt := &client{}
			if got := clnt.isExpired(tt.expiredTime); got != tt.want {
				t.Errorf("isExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_WaitReady(t *testing.T) {
	t.Run("ready successfully", func(t *testing.T) {
		clnt := &client{
			queueName: QueueName("test_queue"),
			ready:     make(chan struct{}),
			done:      make(chan struct{}),
		}

		go func() {
			time.Sleep(10 * time.Millisecond)
			close(clnt.ready)
		}()

		ctx := context.Background()
		err := clnt.WaitReady(ctx)
		if err != nil {
			t.Errorf("WaitReady() error = %v, want nil", err)
		}
	})

	t.Run("context canceled", func(t *testing.T) {
		clnt := &client{
			queueName: QueueName("test_queue"),
			ready:     make(chan struct{}),
			done:      make(chan struct{}),
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := clnt.WaitReady(ctx)
		if err == nil {
			t.Error("WaitReady() error = nil, want error")
		}
	})

	t.Run("client closed before ready", func(t *testing.T) {
		clnt := &client{
			queueName: QueueName("test_queue"),
			ready:     make(chan struct{}),
			done:      make(chan struct{}),
		}

		close(clnt.done)

		ctx := context.Background()
		err := clnt.WaitReady(ctx)
		if err == nil {
			t.Error("WaitReady() error = nil, want error")
		}
	})
}

func TestClient_close(t *testing.T) {
	t.Run("successful close", func(t *testing.T) {
		clnt := &client{
			queueName: QueueName("test_queue"),
			done:      make(chan struct{}),
			lgr:       NewMockLogger(),
		}

		err := clnt.close()
		if err != nil {
			t.Errorf("close() error = %v, want nil", err)
		}

		select {
		case <-clnt.done:
		default:
			t.Error("done channel not closed")
		}
	})

	t.Run("idempotent close", func(t *testing.T) {
		clnt := &client{
			queueName: QueueName("test_queue"),
			done:      make(chan struct{}),
			lgr:       NewMockLogger(),
		}

		err1 := clnt.close()
		if err1 != nil {
			t.Errorf("close() first call error = %v, want nil", err1)
		}

		err2 := clnt.close()
		if err2 != nil {
			t.Errorf("close() second call error = %v, want nil", err2)
		}
	})
}

func TestClient_push_ConnectionClosed(t *testing.T) {
	clnt := &client{
		queueName: QueueName("test_queue"),
		lgr:       NewMockLogger(),
		done:      make(chan struct{}),
		ready:     make(chan struct{}),
	}
	close(clnt.done)

	ctx := context.Background()
	msg := PublishMessage{
		Body: []byte("test message"),
	}

	err := clnt.push(ctx, msg)
	if err == nil {
		t.Fatal("push() error = nil, want error")
	}
	if !errors.Is(err, ErrClientClosedBeforeReady) {
		t.Errorf("push() error = %v, want ErrClientClosedBeforeReady", err)
	}
}

func TestClient_push_WaitsForReady(t *testing.T) {
	clnt := &client{
		queueName: QueueName("test_queue"),
		lgr:       NewMockLogger(),
		done:      make(chan struct{}),
		ready:     make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- clnt.push(ctx, PublishMessage{Body: []byte("test message")})
	}()

	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("push() returned before ready: %v", err)
	default:
	}

	close(clnt.done)

	err := <-errCh
	if !errors.Is(err, ErrClientClosedBeforeReady) {
		t.Fatalf("push() error = %v, want ErrClientClosedBeforeReady", err)
	}
}

func TestClient_consume_ConnectionClosed(t *testing.T) {
	clnt := &client{
		queueName: QueueName("test_queue"),
	}

	_, err := clnt.consume()
	if err == nil {
		t.Error("consume() error = nil, want error")
	}
}

func TestClient_unsafePush_ConnectionClosed(t *testing.T) {
	clnt := &client{
		queueName: QueueName("test_queue"),
	}

	msg := PublishMessage{
		Body: []byte("test message"),
	}

	err := clnt.unsafePush(context.Background(), msg, map[string]any{"test": "value"})
	if err == nil {
		t.Error("unsafePush() error = nil, want error")
	}
}

func TestClient_connect_Error(t *testing.T) {
	clnt := &client{
		queueName: QueueName("test_queue"),
		lgr:       NewMockLogger(),
	}

	_, err := clnt.connect("invalid://address")
	if err == nil {
		t.Error("connect() error = nil, want error")
	}
}
