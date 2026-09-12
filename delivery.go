package amqpadapter

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (queue *queueStore) handleDelivery(
	loopCtx context.Context, clnt *client, callback ConsumerHandler, delivery amqp.Delivery,
) {
	queue.inflight.Add(1)
	defer queue.inflight.Done()

	expired := extractExpiredHeader(queue, clnt, delivery)

	handlerCtx := clnt.ConsumeHeadersExtractor(loopCtx, amqpTableToMap(delivery.Headers))

	panicked, cErr := queue.invokeHandler(handlerCtx, clnt, callback, delivery.Body)
	if panicked {
		nackAfterPanic(queue, clnt, delivery)

		return
	}

	if cErr == nil {
		ackDelivery(queue, clnt, delivery)

		return
	}

	queue.handleConsumerError(clnt, delivery, expired, cErr)
}

func (queue *queueStore) invokeHandler(
	ctx context.Context, clnt *client, callback ConsumerHandler, body []byte,
) (bool, error) {
	var panicked bool

	var cErr error

	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				panicked = true

				queue.lgr.Error(
					"panic while processing message in queue "+string(clnt.queueName),
					recovered,
				)
			}
		}()

		cErr = callback(ctx, body)
	}()

	return panicked, cErr
}

func nackAfterPanic(queue *queueStore, clnt *client, delivery amqp.Delivery) {
	err := delivery.Nack(false, false)
	if err != nil {
		queue.lgr.Warn(
			"error nacking after panic in queue "+string(clnt.queueName),
			err,
		)
	}
}

func extractExpiredHeader(queue *queueStore, clnt *client, delivery amqp.Delivery) string {
	if !clnt.withRetry() {
		if v, ok := delivery.Headers["expired"].(string); ok {
			return v
		}

		return ""
	}

	v, ok := delivery.Headers["expired"].(string)
	if ok && v != "" {
		return v
	}

	queue.lgr.Warn(
		"missing or invalid expired header for queue " +
			string(clnt.queueName) + ", treating as expired",
	)

	return ""
}

func (queue *queueStore) handleConsumerError(
	clnt *client, delivery amqp.Delivery, expired string, cErr error,
) {
	retryable := IsRetryable(cErr)

	if clnt.withRetry() {
		queue.handleRetryError(clnt, delivery, expired, cErr, retryable)

		return
	}

	nackDelivery(queue, clnt, delivery, cErr)
}

func (queue *queueStore) handleRetryError(
	clnt *client, delivery amqp.Delivery, expired string, cErr error, retryable bool,
) {
	isExpired := expired == "" || clnt.isExpired(expired)
	if isExpired || !retryable {
		queue.finalizeFailedRetry(clnt, delivery, cErr, isExpired, retryable)

		return
	}

	rejectDelivery(queue, clnt, delivery, cErr)
}

func (queue *queueStore) finalizeFailedRetry(
	clnt *client, delivery amqp.Delivery, cErr error, isExpired bool, retryable bool,
) {
	switch {
	case !retryable:
		queue.lgr.Info(
			"non-retryable processing error for queue "+string(clnt.queueName),
			cErr,
		)
	case isExpired:
		queue.lgr.Info(
			"retry deadline expired for queue "+string(clnt.queueName)+
				", expired="+fmt.Sprint(delivery.Headers["expired"]),
		)
	}

	if queue.failJobHandler != nil {
		err := queue.failJobHandler(FailedJob{
			Queue: clnt.queueName,
			Body:  delivery.Body,
			Err:   cErr,
		})
		if err != nil {
			queue.lgr.Warn(
				"error in fail handler for queue "+string(clnt.queueName),
				err,
			)
		}
	}

	err := delivery.Ack(false)
	if err != nil {
		queue.lgr.Warn(
			"error acking message after failed retry for "+
				string(clnt.queueName),
			err,
		)
	}
}

func rejectDelivery(queue *queueStore, clnt *client, delivery amqp.Delivery, cErr error) {
	err := delivery.Reject(false)
	if err != nil {
		queue.lgr.Warn(
			"error rejecting message in queue "+string(clnt.queueName),
			err,
		)
	}

	queue.lgr.Warn(
		"error while retrying message in queue "+string(clnt.queueName),
		cErr,
	)
}

func nackDelivery(queue *queueStore, clnt *client, delivery amqp.Delivery, cErr error) {
	err := delivery.Nack(false, true)
	if err != nil {
		queue.lgr.Warn(
			"error nacking message in queue "+string(clnt.queueName),
			err,
		)
	}

	queue.lgr.Warn(
		"error processing message in queue "+string(clnt.queueName)+" (no retry mode)",
		cErr,
	)
}

func ackDelivery(queue *queueStore, clnt *client, delivery amqp.Delivery) {
	err := delivery.Ack(false)
	if err != nil {
		queue.lgr.Warn(
			"error acking message in queue "+string(clnt.queueName),
			err,
		)
	}
}
