package amqpadapter

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	maxConsumerBackoff    = 30 * time.Second
	initialConsumerDelay  = time.Second
	consumerBackoffFactor = 2
)

func (queue *queueStore) runConsumerLoop(loopCtx context.Context, clnt *client) {
	done := clnt.consumerDone
	callback := clnt.callback
	backoffDelay := initialConsumerDelay

consumerLoop:
	for {
		if queue.consumerCanceled(done) {
			_ = clnt.close()

			return
		}

		backoffDelay = queue.waitUntilClientReady(clnt, done, backoffDelay)
		if backoffDelay == 0 {
			return
		}

		deliveries, err := clnt.consume()
		if err != nil {
			queue.lgr.Warn("error consuming queue "+string(clnt.queueName)+", retrying", err)

			backoffDelay = queue.sleepConsumerBackoff(done, clnt.done, backoffDelay)
			if backoffDelay == 0 {
				return
			}

			continue consumerLoop
		}

		if !queue.processDeliveries(loopCtx, clnt, callback, deliveries, done) {
			return
		}
	}
}

func (queue *queueStore) consumerCanceled(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func (queue *queueStore) waitUntilClientReady(
	clnt *client, done <-chan struct{}, backoffDelay time.Duration,
) time.Duration {
	for clnt.isReady.Load() == 0 {
		select {
		case <-done:
			return 0
		case <-clnt.done:
			return 0
		case <-time.After(backoffDelay):
			backoffDelay = nextConsumerBackoff(backoffDelay)
		}
	}

	return initialConsumerDelay
}

func (queue *queueStore) sleepConsumerBackoff(
	done <-chan struct{}, clientDone <-chan struct{}, backoffDelay time.Duration,
) time.Duration {
	select {
	case <-done:
		return 0
	case <-clientDone:
		return 0
	case <-time.After(backoffDelay):
		return nextConsumerBackoff(backoffDelay)
	}
}

func nextConsumerBackoff(current time.Duration) time.Duration {
	next := current * consumerBackoffFactor
	if next > maxConsumerBackoff {
		return maxConsumerBackoff
	}

	return next
}

func (queue *queueStore) processDeliveries(
	loopCtx context.Context,
	clnt *client,
	callback ConsumerHandler,
	deliveries <-chan amqp.Delivery,
	done <-chan struct{},
) bool {
	chClosedCh := make(chan *amqp.Error, 1)
	if clnt.ch != nil {
		clnt.ch.NotifyClose(chClosedCh)
	}

	for {
		select {
		case <-done:
			_ = clnt.close()

			return false

		case amqErr := <-chClosedCh:
			queue.logChannelClosed(clnt.queueName, amqErr)

			return true

		case delivery, ok := <-deliveries:
			if !ok {
				queue.lgr.Warn("delivery channel for queue " + string(clnt.queueName) + " closed, resubscribing")

				return true
			}

			queue.handleDelivery(loopCtx, clnt, callback, delivery)
		}
	}
}

func (queue *queueStore) logChannelClosed(name QueueName, amqErr *amqp.Error) {
	if amqErr != nil {
		queue.lgr.Warn(
			"channel for queue "+string(name)+" was closed",
			amqErr,
		)

		return
	}

	queue.lgr.Warn("channel for queue " + string(name) + " was closed")
}
