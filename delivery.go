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

				queue.lgr.Err(
					"panic при обработке сообщения в очереди "+string(clnt.queueName),
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
		queue.lgr.Wrn(
			"ошибка при nack после panic в очереди "+string(clnt.queueName),
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

	queue.lgr.Wrn(
		"в заголовке сообщения отсутствует или некорректен expired для очереди " +
			string(clnt.queueName) + ", считаем expired",
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
		queue.lgr.Inf(
			"неповторяемая ошибка обработки для очереди "+string(clnt.queueName),
			cErr,
		)
	case isExpired:
		queue.lgr.Inf(
			"истекло время retry для очереди "+string(clnt.queueName)+
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
			queue.lgr.Wrn(
				"ошибка при обработке неудачно завершённой задачи в очереди "+string(clnt.queueName),
				err,
			)
		}
	}

	err := delivery.Ack(false)
	if err != nil {
		queue.lgr.Wrn(
			"ошибка при подтверждении обработки сообщения после повторной попытки для "+
				string(clnt.queueName),
			err,
		)
	}
}

func rejectDelivery(queue *queueStore, clnt *client, delivery amqp.Delivery, cErr error) {
	err := delivery.Reject(false)
	if err != nil {
		queue.lgr.Wrn(
			"ошибка при неподтверждении обработки сообщения в очереди "+string(clnt.queueName),
			err,
		)
	}

	queue.lgr.Wrn(
		"ошибка при повторной обработке сообщения в очереди "+string(clnt.queueName),
		cErr,
	)
}

func nackDelivery(queue *queueStore, clnt *client, delivery amqp.Delivery, cErr error) {
	err := delivery.Nack(false, true)
	if err != nil {
		queue.lgr.Wrn(
			"ошибка при неподтверждении обработки сообщения в очереди "+string(clnt.queueName),
			err,
		)
	}

	queue.lgr.Wrn(
		"ошибка при обработке сообщения в очереди "+string(clnt.queueName)+" (без retry режима)",
		cErr,
	)
}

func ackDelivery(queue *queueStore, clnt *client, delivery amqp.Delivery) {
	err := delivery.Ack(false)
	if err != nil {
		queue.lgr.Wrn(
			"ошибка при подтверждении обработки сообщения в очереди "+string(clnt.queueName),
			err,
		)
	}
}
