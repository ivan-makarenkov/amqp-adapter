package amqpadapter

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	dateFormatLegacy          = "2006-01-02 15:04:05"
	maxPushProcessingTime     = 30 * time.Second
	maxPublishMessagePriority = 9
)

type client struct {
	conn                    *amqp.Connection
	ch                      *amqp.Channel
	notifyConnClose         chan *amqp.Error
	notifyChanClose         chan *amqp.Error
	notifyConfirm           chan amqp.Confirmation
	done                    chan struct{}
	ready                   chan struct{}
	queueName               QueueName
	isReady                 atomic.Int32
	pushMu                  sync.Mutex
	closeOnce               sync.Once
	startOnce               sync.Once
	brokerURL               string
	lgr                     Logger
	retry                   *RetryConfig
	ReconnectDelay          time.Duration
	ReInitDelay             time.Duration
	ResendDelay             time.Duration
	callback                ConsumerHandler
	consumerDone            <-chan struct{}
	PublishHeadersBuilder   PublishHeadersBuilder
	ConsumeHeadersExtractor ConsumeHeadersExtractor
}

// WaitReady waits until the client is ready or the context is canceled.
func (clnt *client) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled while waiting for client %s readiness: %w", clnt.queueName, ctx.Err())
	case <-clnt.done:
		return fmt.Errorf("%w %s", ErrClientClosedBeforeReady, clnt.queueName)
	case <-clnt.ready:
		return nil
	}
}

func (clnt *client) handleReconnect(addr string) {
	clnt.lgr.Info("starting connection attempts for queue " + string(clnt.queueName))

	for {
		clnt.isReady.Store(0)

		clnt.lgr.Info("attempting connection for queue " + string(clnt.queueName))

		conn, err := clnt.connect(addr)
		if err != nil {
			clnt.lgr.Info("failed to connect to queue " + string(clnt.queueName) + ", retrying...")

			if clnt.waitOrDone(clnt.ReconnectDelay) {
				return
			}

			continue
		}

		if done := clnt.handleReInit(conn); done {
			break
		}
	}
}

func (clnt *client) waitOrDone(delay time.Duration) bool {
	select {
	case <-clnt.done:
		return true
	case <-time.After(delay):
		return false
	}
}

func (clnt *client) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrConnectToQueue, addr, err)
	}

	clnt.conn = conn
	clnt.notifyConnClose = make(chan *amqp.Error, 1)
	clnt.conn.NotifyClose(clnt.notifyConnClose)

	clnt.lgr.Info("connected to queue " + string(clnt.queueName))

	return conn, nil
}

func (clnt *client) handleReInit(conn *amqp.Connection) bool {
	clnt.lgr.Info("starting channel init for queue " + string(clnt.queueName))

	for {
		clnt.isReady.Store(0)

		err := clnt.init(conn)
		if err != nil {
			clnt.lgr.Error("failed to init channel for " + string(clnt.queueName) + ", retrying...")

			select {
			case <-clnt.done:
				return true
			case <-clnt.notifyConnClose:
				clnt.lgr.Info("connection for " + string(clnt.queueName) + " closed, reconnecting...")

				return false
			case <-time.After(clnt.ReInitDelay):
			}

			continue
		}

		select {
		case <-clnt.done:
			return true
		case <-clnt.notifyConnClose:
			clnt.lgr.Info("established connection for " + string(clnt.queueName) + " closed, reconnecting...")

			return false
		case <-clnt.notifyChanClose:
			clnt.lgr.Info("channel for " + string(clnt.queueName) + " closed, re-initializing...")
		}
	}
}

func (clnt *client) init(conn *amqp.Connection) error {
	clnt.lgr.Info("starting queue init for " + string(clnt.queueName))

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrOpenChannel, err)
	}

	err = channel.Confirm(false)
	if err != nil {
		_ = channel.Close()

		return fmt.Errorf("%w: %w", ErrEnableConfirms, err)
	}

	clnt.ch = channel
	clnt.notifyChanClose = make(chan *amqp.Error, 1)
	clnt.notifyConfirm = make(chan amqp.Confirmation, 1)
	clnt.ch.NotifyClose(clnt.notifyChanClose)
	clnt.ch.NotifyPublish(clnt.notifyConfirm)

	notifyReturn := make(chan amqp.Return, 1)
	clnt.ch.NotifyReturn(notifyReturn)

	go clnt.logReturnedMessages(notifyReturn)

	queueName := string(clnt.queueName)
	if clnt.withRetry() {
		_, err = initQueueWithRetry(queueName, clnt.ch, clnt.retry.Delay)
	} else {
		_, err = initQueue(queueName, clnt.ch)
	}

	if err != nil {
		clnt.lgr.Warn("queue init error", err)
		_ = clnt.ch.Close()
		clnt.ch = nil

		return err
	}

	clnt.isReady.Store(1)

	select {
	case <-clnt.ready:
	default:
		close(clnt.ready)
	}

	clnt.lgr.Info("queue client init completed for " + string(clnt.queueName))

	return nil
}

func (clnt *client) logReturnedMessages(notifyReturn <-chan amqp.Return) {
	for ret := range notifyReturn {
		clnt.lgr.Error(
			"message returned by broker (queue/routing unavailable): " +
				"queue=" + string(clnt.queueName) +
				", replyCode=" + strconv.Itoa(int(ret.ReplyCode)) +
				", replyText=" + ret.ReplyText +
				", exchange=" + ret.Exchange +
				", routingKey=" + ret.RoutingKey,
		)
	}
}

func (clnt *client) push(ctx context.Context, msg PublishMessage) error {
	err := clnt.WaitReady(ctx)
	if err != nil {
		return err
	}

	contextHeaders := clnt.PublishHeadersBuilder(ctx)

	for {
		clnt.pushMu.Lock()

		err = clnt.unsafePush(ctx, msg, contextHeaders)
		if err == nil {
			err = clnt.waitPublishConfirm(ctx)
		}

		clnt.pushMu.Unlock()

		if err == nil {
			return nil
		}

		clnt.lgr.ErrorContext(ctx, "failed to publish to queue "+string(clnt.queueName)+", retrying")

		waitErr := clnt.waitBeforePushRetry(ctx)
		if waitErr != nil {
			return waitErr
		}
	}
}

func (clnt *client) waitBeforePushRetry(ctx context.Context) error {
	select {
	case <-clnt.done:
		return fmt.Errorf("%w %q", ErrDoneSignalBeforeAck, clnt.queueName)
	case <-ctx.Done():
		return fmt.Errorf("%w %q", ErrContextCanceledBeforeAck, clnt.queueName)
	case <-time.After(clnt.ResendDelay):
		return nil
	}
}

func (clnt *client) waitPublishConfirm(ctx context.Context) error {
	select {
	case <-clnt.done:
		return fmt.Errorf("%w %q", ErrDoneSignalBeforeAck, clnt.queueName)
	case <-ctx.Done():
		return fmt.Errorf("%w %q", ErrContextCanceledBeforeAck, clnt.queueName)
	case confirm := <-clnt.notifyConfirm:
		if confirm.Ack {
			deliveryTag := strconv.FormatUint(confirm.DeliveryTag, 10)
			clnt.lgr.InfoContext(ctx, "message published to queue "+string(clnt.queueName)+", deliveryTag="+deliveryTag)

			return nil
		}

		return fmt.Errorf("%w %q", ErrPublishNack, clnt.queueName)
	}
}

func (clnt *client) unsafePush(ctx context.Context, msg PublishMessage, contextHeaders map[string]any) error {
	if clnt.isReady.Load() == 0 {
		return fmt.Errorf("%w %s", ErrQueueConnectionClosed, clnt.queueName)
	}

	pubCtx, cancel := context.WithTimeout(ctx, maxPushProcessingTime)
	defer cancel()

	contentType := msg.ContentType
	if contentType == "" {
		contentType = DefaultContentType
	}

	pub := amqp.Publishing{ //nolint:exhaustruct_v5 // only used AMQP fields are set
		DeliveryMode: amqp.Persistent,
		ContentType:  contentType,
		Body:         msg.Body,
		Timestamp:    time.Now().UTC(),
		Headers:      mapToAMQPTable(contextHeaders),
	}

	if msg.Priority != nil {
		if *msg.Priority > maxPublishMessagePriority {
			return fmt.Errorf("%w: %d", ErrInvalidPriority, *msg.Priority)
		}

		pub.Priority = uint8(*msg.Priority)
	}

	if clnt.withRetry() {
		maxRetryDuration := clnt.retry.MaxDuration
		if msg.MaxRetryDuration != nil {
			maxRetryDuration = *msg.MaxRetryDuration
		}

		now := time.Now().UTC()
		pub.Headers["expired"] = now.Add(maxRetryDuration).Format(time.RFC3339)
	}

	err := clnt.ch.PublishWithContext(
		pubCtx,
		"",
		string(clnt.queueName),
		true,
		false,
		pub,
	)
	if err != nil {
		return fmt.Errorf("%w %s: %w", ErrPublishFailed, clnt.queueName, err)
	}

	return nil
}

func mapToAMQPTable(values map[string]any) amqp.Table {
	if len(values) == 0 {
		return amqp.Table{}
	}

	table := make(amqp.Table, len(values))
	maps.Copy(table, values)

	return table
}

func (clnt *client) consume() (<-chan amqp.Delivery, error) {
	if clnt.isReady.Load() == 0 {
		return nil, fmt.Errorf("%w %s", ErrConsumeQueueConnectionClosed, clnt.queueName)
	}

	deliveries, err := clnt.ch.Consume(
		string(clnt.queueName),
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrStartingQueueConsumption, clnt.queueName, err)
	}

	return deliveries, nil
}

func (clnt *client) withRetry() bool {
	return clnt.retry != nil
}

func (clnt *client) isExpired(expiredTime string) bool {
	if expiredTime == "" {
		return true
	}

	t, err := parseExpiredTime(expiredTime)
	if err != nil {
		return true
	}

	return time.Now().UTC().After(t)
}

func parseExpiredTime(expiredTime string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, expiredTime)
	if err == nil {
		return parsed, nil
	}

	parsed, err = time.ParseInLocation(dateFormatLegacy, expiredTime, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse legacy expired %q: %w", expiredTime, err)
	}

	return parsed, nil
}

// ensureConnected starts the reconnect loop on first use (lazy publisher).
func (clnt *client) ensureConnected() {
	clnt.startOnce.Do(func() {
		go clnt.handleReconnect(clnt.brokerURL)
	})
}

func (clnt *client) close() error {
	var closeErr error

	clnt.closeOnce.Do(func() {
		clnt.lgr.Info("closing mq connection")

		clnt.isReady.Store(0)

		select {
		case <-clnt.done:
		default:
			close(clnt.done)
		}

		if clnt.ch != nil {
			err := clnt.ch.Close()
			if err != nil && closeErr == nil {
				closeErr = fmt.Errorf("%w %s: %w", ErrClosingQueueChannel, clnt.queueName, err)
			}
		}

		if clnt.conn != nil {
			err := clnt.conn.Close()
			if err != nil && closeErr == nil {
				closeErr = fmt.Errorf("%w %s: %w", ErrClosingQueueConnection, clnt.queueName, err)
			}
		}
	})

	return closeErr
}

func initQueue(name string, channel *amqp.Channel) (amqp.Queue, error) {
	queue, err := channel.QueueDeclare(name, true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("error declaring queue %s: %w", name, err)
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("error setting QoS for queue %s: %w", name, err)
	}

	return queue, nil
}

func initQueueWithRetry(name string, channel *amqp.Channel, retryDelay time.Duration) (amqp.Queue, error) {
	err := channel.ExchangeDeclare(name+".ex", "fanout", true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("error declaring queue exchange: %w", err)
	}

	err = channel.ExchangeDeclare(name+".delay.ex", "fanout", true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("error declaring delay exchange: %w", err)
	}

	queue, err := channel.QueueDeclare(name, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": name + ".delay.ex",
	})
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("error declaring queue %s: %w", name, err)
	}

	_, err = channel.QueueDeclare(name+".delay", true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": name + ".ex",
		"x-message-ttl":          int64(retryDelay / time.Millisecond),
	})
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("error declaring queue %s.delay: %w", name, err)
	}

	err = channel.QueueBind(name, name+".bind", name+".ex", false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("%w (for: %q): %w", ErrBindQueueToExchange, name+".ex", err)
	}

	err = channel.QueueBind(name+".delay", name+".delay.bind", name+".delay.ex", false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("%w (for: %q): %w", ErrBindQueueToExchange, name+".delay.ex", err)
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("error setting QoS for queue %s: %w", name, err)
	}

	return queue, nil
}
