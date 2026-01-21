package taskharbor

import "context"

/*
Handler will be the function that will process
the different jobs.
*/
type Handler func(ctx context.Context, job Job) error

/*
Middleware will wrap the handler and return a new
handler. TODO: PANIC REC, TIMEOUTS, RETRIES, etc...
*/
type Middleware func(Handler) Handler
