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

// WaitReady ждёт готовности клиента или отмены контекста.
func (clnt *client) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("контекст отменён при ожидании готовности клиента %s: %w", clnt.queueName, ctx.Err())
	case <-clnt.done:
		return fmt.Errorf("%w %s", ErrClientClosedBeforeReady, clnt.queueName)
	case <-clnt.ready:
		return nil
	}
}

func (clnt *client) handleReconnect(addr string) {
	clnt.lgr.Inf("начало попытки соединения с очередью " + string(clnt.queueName))

	for {
		clnt.isReady.Store(0)

		clnt.lgr.Inf("попытка соединения с очередью " + string(clnt.queueName))

		conn, err := clnt.connect(addr)
		if err != nil {
			clnt.lgr.Inf("неудачная попытка соединения с очередью " + string(clnt.queueName) + ", повторная попытка...")

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

	clnt.lgr.Inf("соединение с очередью " + string(clnt.queueName) + " прошло успешно")

	return conn, nil
}

func (clnt *client) handleReInit(conn *amqp.Connection) bool {
	clnt.lgr.Inf("начало попытки инициализировать канал для очереди " + string(clnt.queueName))

	for {
		clnt.isReady.Store(0)

		err := clnt.init(conn)
		if err != nil {
			clnt.lgr.Err("не удалось инициализировать канал для " + string(clnt.queueName) + ", повторная попытка...")

			select {
			case <-clnt.done:
				return true
			case <-clnt.notifyConnClose:
				clnt.lgr.Inf("соединение clnt " + string(clnt.queueName) + " разорвано, переподключение...")

				return false
			case <-time.After(clnt.ReInitDelay):
			}

			continue
		}

		select {
		case <-clnt.done:
			return true
		case <-clnt.notifyConnClose:
			clnt.lgr.Inf("удачное соединение clnt " + string(clnt.queueName) + " разорвано, переподключение...")

			return false
		case <-clnt.notifyChanClose:
			clnt.lgr.Inf("канал для " + string(clnt.queueName) + " закрыт, повторный запуск инициализации...")
		}
	}
}

func (clnt *client) init(conn *amqp.Connection) error {
	clnt.lgr.Inf("начало попытки инициализировать очередь " + string(clnt.queueName))

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
		clnt.lgr.Wrn("ошибка при инициализации очереди", err)
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

	clnt.lgr.Inf("инициализация клиента очереди " + string(clnt.queueName) + " завершена")

	return nil
}

func (clnt *client) logReturnedMessages(notifyReturn <-chan amqp.Return) {
	for ret := range notifyReturn {
		clnt.lgr.Err(
			"сообщение возвращено брокером (недоступна очередь/маршрутизация): "+
				"очередь="+string(clnt.queueName)+
				", replyCode="+strconv.Itoa(int(ret.ReplyCode))+
				", replyText="+ret.ReplyText+
				", exchange="+ret.Exchange+
				", routingKey="+ret.RoutingKey,
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

		clnt.lgr.ErrCtx(ctx, "отправка сообщения в очередь "+string(clnt.queueName)+" не удалась, повторная попытка")

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
			clnt.lgr.InfCtx(ctx, "сообщение отправлено в очередь "+string(clnt.queueName)+", deliveryTag="+deliveryTag)

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

	pub := amqp.Publishing{ //nolint:exhaustruct // заполняются только используемые поля AMQP
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
		return time.Time{}, fmt.Errorf("ошибка разбора legacy expired %q: %w", expiredTime, err)
	}

	return parsed, nil
}

// ensureConnected запускает reconnect-цикл при первом обращении (lazy publisher).
func (clnt *client) ensureConnected() {
	clnt.startOnce.Do(func() {
		go clnt.handleReconnect(clnt.brokerURL)
	})
}

func (clnt *client) close() error {
	var closeErr error

	clnt.closeOnce.Do(func() {
		clnt.lgr.Inf("закрытие соединения mq")

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
		return amqp.Queue{}, fmt.Errorf("ошибка при объявлении очереди %s: %w", name, err)
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("ошибка при объявлении Qos для очереди %s: %w", name, err)
	}

	return queue, nil
}

func initQueueWithRetry(name string, channel *amqp.Channel, retryDelay time.Duration) (amqp.Queue, error) {
	err := channel.ExchangeDeclare(name+".ex", "fanout", true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("ошибка при объявлении обменника очереди: %w", err)
	}

	err = channel.ExchangeDeclare(name+".delay.ex", "fanout", true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("ошибка при объявлении обменника очереди: %w", err)
	}

	queue, err := channel.QueueDeclare(name, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": name + ".delay.ex",
	})
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("ошибка при объявлении очереди %s: %w", name, err)
	}

	_, err = channel.QueueDeclare(name+".delay", true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": name + ".ex",
		"x-message-ttl":          int64(retryDelay / time.Millisecond),
	})
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("ошибка при объявлении очереди %s.delay: %w", name, err)
	}

	err = channel.QueueBind(name, name+".bind", name+".ex", false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("%w (для: %q): %w", ErrBindQueueToExchange, name+".ex", err)
	}

	err = channel.QueueBind(name+".delay", name+".delay.bind", name+".delay.ex", false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("%w (для: %q): %w", ErrBindQueueToExchange, name+".delay.ex", err)
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("ошибка при объявлении Qos для очереди %s: %w", name, err)
	}

	return queue, nil
}
