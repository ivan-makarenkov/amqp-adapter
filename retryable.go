package amqpadapter

import (
	"errors"
	"fmt"
)

// RetryableError оборачивает ошибку, сигнализируя consumer loop о необходимости повторной обработки.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "повторяемая ошибка обработки сообщения"
	}

	return fmt.Sprintf("повторяемая ошибка обработки сообщения: %v", e.Err)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// Retry оборачивает ошибку как повторяемую для ConsumerHandler.
func Retry(err error) error {
	if err == nil {
		return nil
	}

	return &RetryableError{Err: err}
}

// IsRetryable проверяет, помечена ли ошибка как повторяемая.
func IsRetryable(err error) bool {
	var re *RetryableError

	return errors.As(err, &re)
}
