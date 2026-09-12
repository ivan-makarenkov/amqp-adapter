package amqpadapter

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type queueStore struct {
	config                  Config
	lgr                     Logger
	failJobHandler          FailJobHandler
	publishHeadersBuilder   PublishHeadersBuilder
	consumeHeadersExtractor ConsumeHeadersExtractor
	publishers              map[QueueName]*client
	consumers               map[QueueName][]*client
	mu                      sync.RWMutex
	consumersStarted        atomic.Bool
	inflight                sync.WaitGroup
}

// New creates and initializes a Queue from config and options.
func New(conf Config, opts ...Option) (Queue, error) { //nolint:ireturn // public contract returns Queue interface
	store, err := newQueueStore(conf, opts...)
	if err != nil {
		return nil, err
	}

	return store, nil
}

func newQueueStore(conf Config, opts ...Option) (*queueStore, error) {
	cfgOpts := defaultOptions()
	for _, opt := range opts {
		opt(&cfgOpts)
	}

	err := validateConfig(conf, cfgOpts)
	if err != nil {
		return nil, err
	}

	if conf.QueueParams == nil {
		conf.QueueParams = make(map[QueueName]QueueItem)
	}

	store := &queueStore{ //nolint:exhaustruct_v5 // mu, consumersStarted, inflight use zero values
		config:                  conf,
		lgr:                     cfgOpts.lgr,
		failJobHandler:          cfgOpts.failHandler,
		publishHeadersBuilder:   cfgOpts.publishHeadersBuilder,
		consumeHeadersExtractor: cfgOpts.consumeHeadersExtractor,
		publishers:              make(map[QueueName]*client),
		consumers:               make(map[QueueName][]*client),
	}

	for name, param := range conf.QueueParams {
		if param.ConsumerOnly {
			continue
		}

		clnt := newPublisherClient(name, param, conf, cfgOpts)

		store.mu.Lock()
		store.publishers[name] = clnt
		store.mu.Unlock()
	}

	return store, nil
}

func newClient(name QueueName, param QueueItem, conf Config, lgr Logger) *client {
	return &client{ //nolint:exhaustruct_v5 // connection fields are set on connect
		queueName:      name,
		brokerURL:      conf.URL,
		lgr:            lgr,
		done:           make(chan struct{}),
		ready:          make(chan struct{}),
		retry:          param.Retry,
		ReconnectDelay: conf.ReconnectDelay,
		ReInitDelay:    conf.ReInitDelay,
		ResendDelay:    conf.ResendDelay,
	}
}

func newPublisherClient(name QueueName, param QueueItem, conf Config, cfgOpts options) *client {
	clnt := newClient(name, param, conf, cfgOpts.lgr)
	clnt.PublishHeadersBuilder = cfgOpts.publishHeadersBuilder

	return clnt
}

func (queue *queueStore) AddConsumerN(
	ctx context.Context, name QueueName, parallelism int, callback ConsumerHandler,
) (err error) {
	if queue.consumersStarted.Load() {
		return ErrConsumersAlreadyStarted
	}

	if _, ok := queue.config.QueueParams[name]; !ok {
		return fmt.Errorf("%w: %s", ErrQueueNotFound, name)
	}

	if parallelism <= 0 {
		queue.lgr.WarnContext(ctx, "parallelism <= 0 for queue "+string(name)+", using 1")

		parallelism = 1
	}

	queue.lgr.InfoContext(ctx, "adding "+strconv.Itoa(parallelism)+" handlers for queue "+string(name))

	var clients []*client

	defer func() {
		if err != nil {
			for _, consumer := range clients {
				_ = consumer.close()
			}
		}
	}()

	for range parallelism {
		var clnt *client

		clnt, err = queue.addConsumerClient(ctx, name, callback)
		if err != nil {
			return err
		}

		clients = append(clients, clnt)
	}

	queue.mu.Lock()
	if queue.consumersStarted.Load() {
		queue.mu.Unlock()

		return ErrConsumersAlreadyStarted
	}

	queue.consumers[name] = append(queue.consumers[name], clients...)
	queue.mu.Unlock()

	return nil
}

func (queue *queueStore) AddConsumer(
	ctx context.Context, name QueueName, callback ConsumerHandler,
) error {
	return queue.AddConsumerN(ctx, name, 1, callback)
}

func (queue *queueStore) Publish(ctx context.Context, name QueueName, msg PublishMessage) error {
	param, ok := queue.config.QueueParams[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrQueueNotFound, name)
	}

	if param.ConsumerOnly {
		return fmt.Errorf("%w: %s", ErrQueueConsumerOnly, name)
	}

	queue.mu.RLock()
	publisher := queue.publishers[name]
	queue.mu.RUnlock()

	if publisher == nil {
		return fmt.Errorf("%w: %s", ErrQueueNotFound, name)
	}

	publisher.ensureConnected()

	return publisher.push(ctx, msg)
}

func (queue *queueStore) InitConsumer(ctx context.Context) error {
	err := queue.guardConsumerStart(ctx)
	if err != nil {
		return err
	}

	consumers, total := queue.snapshotConsumers()
	if total == 0 {
		return ErrNoConsumersRegistered
	}

	if !queue.consumersStarted.CompareAndSwap(false, true) {
		return ErrConsumersAlreadyStarted
	}

	queue.startConsumerWorkers(ctx, consumers)

	return nil
}

func (queue *queueStore) Shutdown(ctx context.Context) error {
	publishers, consumers := queue.snapshotClients()

	// Stop accepting new messages first, then wait for in-flight handlers.
	firstErr := queue.closeClients(ctx, publishers, consumers)

	waitErr := queue.waitInflight(ctx)
	if waitErr != nil && firstErr == nil {
		firstErr = waitErr
	}

	if ctx.Err() != nil {
		if firstErr != nil {
			return fmt.Errorf("%w: %w", ctx.Err(), firstErr)
		}

		return fmt.Errorf("%w", ctx.Err())
	}

	return firstErr
}

func (queue *queueStore) waitInflight(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		queue.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for in-flight message processing: %w", ctx.Err())
	}
}

func (queue *queueStore) addConsumerClient(
	ctx context.Context, name QueueName, callback ConsumerHandler,
) (*client, error) {
	param, ok := queue.config.QueueParams[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrQueueNotFound, name)
	}

	clnt := newClient(name, param, queue.config, queue.lgr)
	clnt.callback = callback
	clnt.consumerDone = ctx.Done()
	clnt.ConsumeHeadersExtractor = queue.consumeHeadersExtractor

	go clnt.handleReconnect(queue.config.URL)

	return clnt, nil
}

func (queue *queueStore) guardConsumerStart(ctx context.Context) error {
	if queue.consumersStarted.Load() {
		return ErrConsumersAlreadyStarted
	}

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrContextCanceledOnConsumerStart, err)
	}

	return nil
}

func (queue *queueStore) snapshotConsumers() (map[QueueName][]*client, int) {
	queue.mu.RLock()
	defer queue.mu.RUnlock()

	total := 0
	consumers := make(map[QueueName][]*client, len(queue.consumers))

	for name, clients := range queue.consumers {
		consumers[name] = append([]*client(nil), clients...)
		total += len(clients)
	}

	return consumers, total
}

func (queue *queueStore) startConsumerWorkers(ctx context.Context, consumers map[QueueName][]*client) {
	for _, clients := range consumers {
		for _, clnt := range clients {
			go queue.runConsumerLoop(ctx, clnt)
		}
	}
}

func (queue *queueStore) snapshotClients() (map[QueueName]*client, map[QueueName][]*client) {
	queue.mu.RLock()
	defer queue.mu.RUnlock()

	publishers := make(map[QueueName]*client, len(queue.publishers))
	maps.Copy(publishers, queue.publishers)

	consumers := make(map[QueueName][]*client, len(queue.consumers))
	for name, clients := range queue.consumers {
		consumers[name] = append([]*client(nil), clients...)
	}

	return publishers, consumers
}

func (queue *queueStore) closeClients(
	ctx context.Context,
	publishers map[QueueName]*client,
	consumers map[QueueName][]*client,
) error {
	var firstErr error

	for name, publisher := range publishers {
		firstErr = queue.closeOneClient(ctx, name, 0, "publisher", publisher, firstErr)
	}

	for name, clients := range consumers {
		for i, consumer := range clients {
			firstErr = queue.closeOneClient(ctx, name, i, "consumer", consumer, firstErr)
		}
	}

	return firstErr
}

func (queue *queueStore) closeOneClient(
	ctx context.Context,
	name QueueName,
	idx int,
	role string,
	clnt *client,
	firstErr error,
) error {
	if ctx.Err() != nil && firstErr == nil {
		firstErr = fmt.Errorf("%w", ctx.Err())
	}

	err := clnt.close()
	if err != nil && firstErr == nil {
		return fmt.Errorf("error closing %s %s[%d]: %w", role, name, idx, err)
	}

	if err != nil && firstErr != nil {
		queue.lgr.Warn("error closing "+role+" "+string(name), err)
	}

	return firstErr
}

func amqpTableToMap(table amqp.Table) map[string]any {
	if len(table) == 0 {
		return map[string]any{}
	}

	out := make(map[string]any, len(table))
	maps.Copy(out, table)

	return out
}
