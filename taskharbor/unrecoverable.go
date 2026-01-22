package taskharbor

import "errors"

/*
UnrecoverableError marks a failure as non-retryable.
Wrap an error with Unrecoverable(err) inside a handler to FORCE DLQ.

Example:

	return taskharbor.Unrecoverable(fmt.Errorf("bad input: %w", err))

Core checks this using errors.As.
Drivers do not interpret this.

Note: if err is nil, Unrecoverable(nil) returns nil.
*/

type UnrecoverableError struct {
	Err error
}

func (e UnrecoverableError) Error() string {
	if e.Err == nil {
		return "unrecoverable"
	}
	return e.Err.Error()
}

func (e UnrecoverableError) Unwrap() error {
	return e.Err
}

func Unrecoverable(err error) error {
	if err == nil {
		return nil
	}
	return UnrecoverableError{Err: err}
}

func IsUnrecoverable(err error) bool {
	var ue UnrecoverableError
	return errors.As(err, &ue)
}
