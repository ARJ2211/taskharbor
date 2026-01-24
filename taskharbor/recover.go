package taskharbor

import (
	"context"
	"fmt"
	"runtime/debug"
)

/*
Stack is captured but not included in Error() by default (keeps DLQ reason small).
TODO: add logging hooks (milestone 11) and log PanicError.Stack.
*/
type PanicError struct {
	Value any
	Stack []byte
}

/*
Keeping this short so LastError does not
take up a lot of storage.
*/
func (e PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

/*
Recovery middleware will convert a panic inside
the job handler function into errors so that the
worker can apply the normal retry/DLQ path.
*/
func RecoverMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job Job) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = PanicError{
						Value: r,
						Stack: debug.Stack(),
					}
				}
			}()
			return next(ctx, job)
		}
	}
}
