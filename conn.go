package gortmplib

import (
	"github.com/bluenviron/gortmplib/pkg/message"
)

// Conn is implemented by Client and ServerConn.
type Conn interface {
	BytesReceived() uint64
	BytesSent() uint64
	Read() (message.Message, error)
	Write(msg message.Message) error
}

// ChunkSizeUpdater is implemented by connections that allow changing the
// outgoing chunk size while the connection is established.
//
// SetChunkSize sends a SetChunkSize control message to the peer using the
// previous chunk size and then applies the new size to subsequent writes.
// Implementations must make the operation atomic with respect to concurrent
// Write calls, so that message boundaries are preserved.
type ChunkSizeUpdater interface {
	// SetChunkSize changes the outgoing chunk size.
	// It returns an error if the size is not valid, the message cannot be
	// written, or the connection is closed; in that case the chunk size is
	// left unchanged.
	SetChunkSize(v uint32) error
	// ChunkSize returns the outgoing chunk size.
	ChunkSize() uint32
	// PeerChunkSize returns the chunk size currently used by the peer, as
	// announced by the last received SetChunkSize message.
	PeerChunkSize() uint32
}

var (
	_ ChunkSizeUpdater = (*Client)(nil)
	_ ChunkSizeUpdater = (*ServerConn)(nil)
)
