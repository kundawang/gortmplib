package liberrors

import (
	"fmt"
)

// ErrChunkSizeInvalid is returned when a chunk size is zero or exceeds the
// maximum allowed value.
type ErrChunkSizeInvalid struct {
	Value uint32
}

// Error implements the error interface.
func (e ErrChunkSizeInvalid) Error() string {
	return fmt.Sprintf("invalid chunk size: %d", e.Value)
}

// ErrChunkSizePeerInvalid is returned by a reader when the peer announces an
// invalid chunk size.
type ErrChunkSizePeerInvalid struct {
	Value uint32
}

// Error implements the error interface.
func (e ErrChunkSizePeerInvalid) Error() string {
	return fmt.Sprintf("invalid chunk size received from peer: %d", e.Value)
}

// ErrChunkSizeUnsupported is returned when a connection does not support
// changing the chunk size at runtime.
type ErrChunkSizeUnsupported struct{}

// Error implements the error interface.
func (e ErrChunkSizeUnsupported) Error() string {
	return "chunk size changes are not supported by this connection"
}

// ErrClientNotInitialized is returned when the client has not been initialized.
type ErrClientNotInitialized struct{}

// Error implements the error interface.
func (e ErrClientNotInitialized) Error() string {
	return "client is not initialized"
}

// ErrClientClosed is returned when the client connection has been closed.
type ErrClientClosed struct{}

// Error implements the error interface.
func (e ErrClientClosed) Error() string {
	return "client is closed"
}

// ErrServerConnNotInitialized is returned when the server connection has not been initialized.
type ErrServerConnNotInitialized struct{}

// Error implements the error interface.
func (e ErrServerConnNotInitialized) Error() string {
	return "server connection is not initialized"
}
