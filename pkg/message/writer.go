package message

import (
	"io"
	"sync"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/rawmessage"
)

// Writer is a message writer.
// It also drains incoming messages.
type Writer struct {
	BW               io.Writer
	BCW              *bytecounter.Writer
	CheckAcknowledge bool

	mutex sync.Mutex
	rmw   *rawmessage.Writer
}

// Initialize initializes the Writer.
func (w *Writer) Initialize() {
	w.rmw = &rawmessage.Writer{
		BW:               w.BW,
		BCW:              w.BCW,
		CheckAcknowledge: w.CheckAcknowledge,
	}
	w.rmw.Initialize()
}

// NewWriter allocates a Writer.
//
// Deprecated: use Initialize() instead.
func NewWriter(
	bw io.Writer,
	bcw *bytecounter.Writer,
	checkAcknowledge bool,
) *Writer {
	w := &Writer{
		BW:               bw,
		BCW:              bcw,
		CheckAcknowledge: checkAcknowledge,
	}
	w.Initialize()
	return w
}

// SetAcknowledgeValue sets the value of the last received acknowledge.
func (w *Writer) SetAcknowledgeValue(v uint32) {
	w.rmw.SetAcknowledgeValue(v)
}

// Write writes a message. It is safe to call it concurrently.
func (w *Writer) Write(msg Message) error {
	// this is necessary to synchronize rmw.Write with SetChunkSize and SetWindowAckSize.
	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.writeUnlocked(msg)
}

// writeUnlocked writes a message without acquiring the mutex.
// The caller must hold w.mutex.
func (w *Writer) writeUnlocked(msg Message) error {
	if tmsg, ok := msg.(*SetChunkSize); ok {
		// validate before writing anything, so that an invalid size never
		// reaches the wire.
		if err := rawmessage.ValidateChunkSize(tmsg.Value); err != nil {
			return err
		}
	}

	raw, err := msg.marshal()
	if err != nil {
		return err
	}

	err = w.rmw.Write(raw)
	if err != nil {
		return err
	}

	switch tmsg := msg.(type) {
	case *SetChunkSize:
		// already validated above; the error cannot be non-nil.
		_ = w.rmw.SetChunkSize(tmsg.Value)

	case *SetWindowAckSize:
		w.rmw.SetWindowAckSize(tmsg.Value)
	}

	return nil
}

// SetChunkSize changes the chunk size used to write messages and sends a
// SetChunkSize control message to the peer.
//
// The control message is written with the previous chunk size, and the new
// size is applied only to messages written after it, so that the message
// boundaries on the wire are never broken. It is safe to call it concurrently
// with Write; the operation is atomic with respect to other writes.
//
// If value is zero or greater than rawmessage.MaxChunkSize, no message is
// written and an error is returned. If value is already in use, nothing is
// written and nil is returned.
func (w *Writer) SetChunkSize(value uint32) error {
	// validate before taking the lock, to avoid writing anything.
	if err := rawmessage.ValidateChunkSize(value); err != nil {
		return err
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.rmw.ChunkSize() == value {
		return nil
	}

	return w.writeUnlocked(&SetChunkSize{
		Value: value,
	})
}

// ChunkSize returns the chunk size currently used to write messages.
func (w *Writer) ChunkSize() uint32 {
	return w.rmw.ChunkSize()
}
