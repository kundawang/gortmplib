package liberrors

// ErrClientClosed is returned when the client connection has been closed.
type ErrClientClosed struct{}

// Error implements the error interface.
func (e ErrClientClosed) Error() string {
	return "client closed"
}

// ErrClientNotPlaying is returned when Pause() or Resume() is called on a
// client that is not in playing state.
type ErrClientNotPlaying struct{}

// Error implements the error interface.
func (e ErrClientNotPlaying) Error() string {
	return "client is not playing"
}

// ErrClientAlreadyPaused is returned when Pause() is called on an already
// paused client.
type ErrClientAlreadyPaused struct{}

// Error implements the error interface.
func (e ErrClientAlreadyPaused) Error() string {
	return "client is already paused"
}

// ErrClientNotPaused is returned when Resume() is called on a client that is
// not paused.
type ErrClientNotPaused struct{}

// Error implements the error interface.
func (e ErrClientNotPaused) Error() string {
	return "client is not paused"
}
