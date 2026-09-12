package functional

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/amqp-adapter"
	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultRabbitURL = "amqp://guest:guest@127.0.0.1:5672/"

var composeDir = func() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}

	return filepath.Dir(file)
}()

func rabbitURL() string {
	if url := os.Getenv("MQ_TEST_URL"); url != "" {
		return url
	}

	return defaultRabbitURL
}

func requireFunctional(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("пропуск функционального теста в short режиме")
	}

	conn, err := amqp.Dial(rabbitURL())
	if err != nil {
		t.Fatalf("RabbitMQ недоступен (%s): %v", rabbitURL(), err)
	}

	_ = conn.Close()
}

func requireDocker(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker не найден в PATH")
	}
}

func uniqueQueueName(t *testing.T, suffix string) mq.QueueName {
	t.Helper()

	safe := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())

	return mq.QueueName(fmt.Sprintf("func_%s_%s_%d", safe, suffix, time.Now().UnixNano()))
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("таймаут ожидания: %s", msg)
}

func waitRabbitReady(t *testing.T, timeout time.Duration) {
	t.Helper()

	waitUntil(t, timeout, func() bool {
		conn, err := amqp.Dial(rabbitURL())
		if err != nil {
			return false
		}

		_ = conn.Close()

		return true
	}, "готовность RabbitMQ")
}

type tLogger struct {
	t *testing.T
}

func (l tLogger) Inf(msg string, args ...any)                       { l.t.Logf("INF: "+msg, args...) }
func (l tLogger) Wrn(msg string, args ...any)                       { l.t.Logf("WRN: "+msg, args...) }
func (l tLogger) Dbg(msg string, args ...any)                       { l.t.Logf("DBG: "+msg, args...) }
func (l tLogger) Err(msg string, args ...any)                       { l.t.Logf("ERR: "+msg, args...) }
func (l tLogger) Ftl(msg string, args ...any)                       { l.t.Logf("FTL: "+msg, args...) }
func (l tLogger) InfCtx(_ context.Context, msg string, args ...any) { l.Inf(msg, args...) }
func (l tLogger) WrnCtx(_ context.Context, msg string, args ...any) { l.Wrn(msg, args...) }
func (l tLogger) DbgCtx(_ context.Context, msg string, args ...any) { l.Dbg(msg, args...) }
func (l tLogger) ErrCtx(_ context.Context, msg string, args ...any) { l.Err(msg, args...) }
func (l tLogger) FtlCtx(_ context.Context, msg string, args ...any) { l.Ftl(msg, args...) }

type testQueueOptions struct {
	failHandler mq.FailJobHandler
}

func newTestQueue(t *testing.T, queues map[mq.QueueName]mq.QueueItem, opts testQueueOptions) mq.Queue {
	t.Helper()

	failHandler := opts.failHandler
	if failHandler == nil {
		failHandler = func(job mq.FailedJob) error {
			t.Logf("fail job: queue=%s body=%q err=%v", job.Queue, job.Body, job.Err)

			return nil
		}
	}

	needsFailHandler := false
	for _, item := range queues {
		if item.Retry != nil {
			needsFailHandler = true
			break
		}
	}

	mqOpts := []mq.Option{mq.WithLogger(tLogger{t: t})}
	if needsFailHandler {
		mqOpts = append(mqOpts, mq.WithFailHandler(failHandler))
	}

	conf := mq.Config{
		URL:            rabbitURL(),
		ReconnectDelay: 500 * time.Millisecond,
		ReInitDelay:    200 * time.Millisecond,
		ResendDelay:    100 * time.Millisecond,
		QueueParams:    queues,
	}

	queue, err := mq.New(conf, mqOpts...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := queue.Shutdown(shutdownCtx); err != nil {
			t.Logf("Shutdown() error = %v", err)
		}
	})

	return queue
}

func publishUntilSuccess(t *testing.T, ctx context.Context, queue mq.Queue, name mq.QueueName, msg mq.PublishMessage, timeout time.Duration) {
	t.Helper()

	var lastErr error

	waitUntil(t, timeout, func() bool {
		lastErr = queue.Publish(ctx, name, msg)
		return lastErr == nil
	}, "успешная публикация")

	if lastErr != nil {
		t.Fatalf("Publish() не удалась: %v", lastErr)
	}
}

func runCompose(t *testing.T, args ...string) {
	t.Helper()

	cmd := exec.Command("docker", append([]string{"compose", "-f", "docker-compose.yml"}, args...)...)
	cmd.Dir = composeDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func composeUp() error {
	cmd := exec.Command("docker", "compose", "-f", "docker-compose.yml", "up", "-d", "--wait")
	cmd.Dir = composeDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}

	return nil
}

func composeDown() error {
	cmd := exec.Command("docker", "compose", "-f", "docker-compose.yml", "down", "-v")
	cmd.Dir = composeDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}

	return nil
}
