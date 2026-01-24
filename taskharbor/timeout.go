package taskharbor

import "context"

/*
The time out middleware applies per-job timeout using
job.Timeout attribute. If the timeout is 0 or less, it
does nothing.
*/
func TimeoutMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job Job) error {
			if job.Timeout <= 0 {
				return next(ctx, job)
			}

			jobCtx, cancel := context.WithTimeout(ctx, job.Timeout)
			defer cancel()

			return next(jobCtx, job)
		}
	}
}
