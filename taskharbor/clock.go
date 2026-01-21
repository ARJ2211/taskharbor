package taskharbor

import "time"

/*
Clock is responsible for returning the current time.
We inject this so tests can control time deterministically.
*/
type Clock interface {
	Now() time.Time
}

/*
RealClock is the default clock implementation.
*/
type RealClock struct{}

/*
This function returns the current UTC time.
*/
func (c RealClock) Now() time.Time {
	return time.Now().UTC()
}
