package amqpadapter

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type mockAcknowledger struct {
	nackCalled bool
	requeue    bool
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	m.nackCalled = true
	m.requeue = requeue

	return nil
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	return nil
}

func TestHandleDelivery_RecoversPanicAndNacksWithoutRequeue(t *testing.T) {
	ack := &mockAcknowledger{}
	lgr := NewMockLogger()

	queue := &queueStore{
		lgr: lgr,
	}

	clnt := &client{
		queueName: QueueName("test_queue"),
		ConsumeHeadersExtractor: func(parent context.Context, _ map[string]any) context.Context {
			return parent
		},
	}

	delivery := amqp.Delivery{
		Acknowledger: ack,
		Body:         []byte("panic-body"),
	}

	queue.handleDelivery(context.Background(), clnt, func(context.Context, []byte) error {
		panic("handler panic")
	}, delivery)

	if !ack.nackCalled {
		t.Fatal("ожидался Nack после panic в handler")
	}

	if ack.requeue {
		t.Error("Nack после panic должен быть без requeue")
	}

	errMsgs := lgr.GetErrMessages()
	if len(errMsgs) == 0 {
		t.Error("ожидался лог ошибки о panic")
	}
}

func TestHandleDelivery_HandlerErrorWithoutPanic(t *testing.T) {
	ack := &mockAcknowledger{}
	lgr := NewMockLogger()

	queue := &queueStore{
		lgr: lgr,
	}

	clnt := &client{
		queueName: QueueName("test_queue"),
		ConsumeHeadersExtractor: func(parent context.Context, _ map[string]any) context.Context {
			return parent
		},
	}

	delivery := amqp.Delivery{
		Acknowledger: ack,
		Body:         []byte("err-body"),
	}

	queue.handleDelivery(context.Background(), clnt, func(context.Context, []byte) error {
		return errors.New("handler error")
	}, delivery)

	if !ack.nackCalled {
		t.Fatal("ожидался Nack при ошибке handler без retry")
	}

	if !ack.requeue {
		t.Error("Nack при ошибке без retry должен быть с requeue=true")
	}
}
