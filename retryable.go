package amqpadapter

import (
	"errors"
	"fmt"
)

// RetryableError wraps an error to signal that the consumer loop should retry.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable message processing error"
	}

	return fmt.Sprintf("retryable message processing error: %v", e.Err)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// Retry wraps err as retryable for ConsumerHandler.
func Retry(err error) error {
	if err == nil {
		return nil
	}

	return &RetryableError{Err: err}
}

// IsRetryable reports whether err is marked as retryable.
func IsRetryable(err error) bool {
	var re *RetryableError

	return errors.As(err, &re)
}
