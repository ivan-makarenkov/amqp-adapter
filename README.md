# amqp-adapter

Go-адаптер над [amqp091-go](https://github.com/rabbitmq/amqp091-go): публикация и потребление сообщений RabbitMQ с publisher confirms, авто-reconnect и опциональным retry через delayed queue / DLX.

```bash
go get github.com/ivan-makarenkov/amqp-adapter
```

Пакет: `amqpadapter`.

## Возможности

- Ленивое подключение publisher’а при первом `Publish`
- Publisher confirms (один in-flight publish на соединение)
- Автоматическое переподключение при обрыве connection/channel
- Consumer’ы с параллелизмом (`AddConsumerN`)
- Retry через delay-очередь и заголовок `expired` (нужен `WithFailHandler`)
- Проброс headers/context (`WithPublishHeadersBuilder` / `WithConsumeHeadersExtractor`)
- Подпакет [`otel`](otel/) для correlation ID и OpenTelemetry propagation

## Быстрый старт

```go
package main

import (
	"context"
	"log"
	"time"

	mq "github.com/ivan-makarenkov/amqp-adapter"
)

func main() {
	ctx := context.Background()

	q, err := mq.New(mq.Config{
		URL:            "amqp://guest:guest@localhost:5672/",
		ReconnectDelay: time.Second,
		ReInitDelay:    time.Second,
		ResendDelay:    time.Second,
		QueueParams: map[mq.QueueName]mq.QueueItem{
			"jobs": {},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = q.Shutdown(shutdownCtx)
	}()

	err = q.AddConsumer(ctx, "jobs", func(ctx context.Context, body []byte) error {
		log.Printf("got: %s", body)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if err = q.InitConsumer(ctx); err != nil {
		log.Fatal(err)
	}

	err = q.Publish(ctx, "jobs", mq.PublishMessage{Body: []byte("hello")})
	if err != nil {
		log.Fatal(err)
	}

	select {}
}
```

## Конфигурация

| Поле | Смысл |
|------|--------|
| `URL` | AMQP URL |
| `ReconnectDelay` | Пауза между попытками dial |
| `ReInitDelay` | Пауза между повторными init channel/queue |
| `ResendDelay` | Пауза перед повторной публикацией после ошибки/nack |
| `QueueParams` | Параметры по имени очереди |

`QueueItem`:

- `Retry` — `nil` = без retry; иначе delay/DLX-топология
- `ConsumerOnly` — только consumer, без publisher-соединения

## Retry

При `Retry != nil` библиотека объявляет exchange’и и delay-очередь. Для повторной обработки handler должен вернуть `amqpadapter.Retry(err)`.

После исчерпания `MaxDuration` (заголовок `expired`) или не-retryable ошибки вызывается `WithFailHandler`, сообщение ack’ается (убирается из очереди).

```go
q, err := mq.New(conf,
	mq.WithFailHandler(func(job mq.FailedJob) error {
		log.Printf("failed %s: %v", job.Queue, job.Err)
		return nil
	}),
)

// в handler:
return mq.Retry(err) // отложить и повторить
return err           // без retry: финальный fail (в retry-режиме)
```

`WithFailHandler` обязателен, если хотя бы у одной очереди задан `Retry`.

## Consumer’ы

```go
_ = q.AddConsumer(ctx, "jobs", handler)      // 1 воркер
_ = q.AddConsumerN(ctx, "jobs", 4, handler) // 4 соединения/воркера
_ = q.InitConsumer(ctx)                     // старт циклов чтения
```

`AddConsumer*` — только до `InitConsumer`. Prefetch: QoS = 1 на канал.

## Shutdown

`Shutdown` сначала закрывает соединения (останавливает приём), затем ждёт уже запущенные обработки (`inflight`) в пределах контекста.

## Опции

- `WithLogger` — свой логгер (иначе noop)
- `WithFailHandler` — финальные fail в retry-режиме
- `WithPublishHeadersBuilder` — headers из `context` при publish
- `WithConsumeHeadersExtractor` — восстановление `context` из headers

Пример с otel:

```go
import "github.com/ivan-makarenkov/amqp-adapter/otel"

cfg := otel.PropagationConfig{
	CorrelationIDKey: "x-correlation-id",
	TraceKeys:        []string{"traceparent", "tracestate"},
	// GetCorrelationID / SetCorrelationID / Propagator — по необходимости
}

q, err := mq.New(conf,
	mq.WithPublishHeadersBuilder(otel.NewPublishHeadersBuilder(cfg)),
	mq.WithConsumeHeadersExtractor(otel.NewConsumeHeadersExtractor(cfg)),
)
```

## Тесты

```bash
go test ./...                  # unit
go test -short ./...           # без интеграционных
# functional (нужен RabbitMQ), см. tests/ и Makefile
```
