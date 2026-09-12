package functional

import (
	"fmt"
	"os"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMain(m *testing.M) {
	if os.Getenv("MQ_TEST_SKIP_COMPOSE") == "1" {
		os.Exit(m.Run())
	}

	composeStarted := false

	if !rabbitAvailable() {
		if err := composeUp(); err != nil {
			fmt.Fprintf(os.Stderr, "не удалось поднять RabbitMQ через docker compose: %v\n", err)
			os.Exit(1)
		}

		composeStarted = true
	}

	code := m.Run()

	if composeStarted {
		_ = composeDown()
	}

	os.Exit(code)
}

func rabbitAvailable() bool {
	conn, err := amqp.Dial(rabbitURL())
	if err != nil {
		return false
	}

	_ = conn.Close()

	return true
}
