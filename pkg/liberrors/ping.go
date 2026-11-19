package liberrors

import "errors"

// errors returned by ping operations.
var (
	// ErrPingNotConnected is returned when a ping is requested on a connection
	// that has not been initialized yet.
	ErrPingNotConnected = errors.New("connection not initialized")

	// ErrPingTimeout is returned when the peer does not respond to a ping
	// before the timeout expires.
	ErrPingTimeout = errors.New("ping timeout")

	// ErrPingClosed is returned when the connection is closed while a ping is
	// in progress, or when a ping is requested on a closed connection.
	ErrPingClosed = errors.New("connection closed")

	// ErrPingTooFrequent is returned when a new ping is requested while
	// another one is still in progress.
	ErrPingTooFrequent = errors.New("another ping is already in progress")
)
