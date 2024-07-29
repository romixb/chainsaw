package utils

import (
	"errors"
	"fmt"
)

type RetryableError struct {
	msg string
}

func (e *RetryableError) Error() string {
	return e.msg
}
func NewRetryableError(format string, args ...interface{}) *RetryableError {
	return &RetryableError{
		msg: fmt.Sprintf(format, args...),
	}
}
func IsRetryable(err error) bool {
	var retryableErr *RetryableError
	return errors.As(err, &retryableErr)
}
