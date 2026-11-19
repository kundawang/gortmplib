package message

import (
	"io"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
)

// ReadWriter is a message reader/writer.
type ReadWriter struct {
	r *Reader
	w *Writer
}

// NewReadWriter allocates a ReadWriter.
func NewReadWriter(
	rw io.ReadWriter,
	bcrw *bytecounter.ReadWriter,
	checkAcknowledge bool,
) *ReadWriter {
	w := &Writer{
		BW:               rw,
		BCW:              bcrw.Writer,
		CheckAcknowledge: checkAcknowledge,
	}
	w.Initialize()

	r := &Reader{
		BR:  rw,
		BCR: bcrw.Reader,
		OnAckNeeded: func(count uint32) error {
			return w.Write(&Acknowledge{
				Value: count,
			})
		},
	}
	r.Initialize()

	return &ReadWriter{
		r: r,
		w: w,
	}
}

// Read reads a message.
func (rw *ReadWriter) Read() (Message, error) {
	msg, err := rw.r.Read()
	if err != nil {
		return nil, err
	}

	switch tmsg := msg.(type) {
	case *Acknowledge:
		rw.w.SetAcknowledgeValue(tmsg.Value)

	case *UserControlPingRequest:
		err = rw.w.Write(&UserControlPingResponse{
			ServerTime: tmsg.ServerTime,
		})
		if err != nil {
			return nil, err
		}
	}

	return msg, nil
}

// Write writes a message.
func (rw *ReadWriter) Write(msg Message) error {
	return rw.w.Write(msg)
}

// SetChunkSize changes the chunk size used to write messages and notifies the
// peer with a SetChunkSize control message.
//
// The change is atomic with respect to concurrent Write calls: messages
// already in progress are completed with the previous size, while messages
// written after this call use the new size.
func (rw *ReadWriter) SetChunkSize(value uint32) error {
	return rw.w.SetChunkSize(value)
}

// ChunkSize returns the chunk size currently used to write messages.
func (rw *ReadWriter) ChunkSize() uint32 {
	return rw.w.ChunkSize()
}

// PeerChunkSize returns the chunk size currently used by the peer to send
// messages. It is updated whenever a valid SetChunkSize control message is
// received.
func (rw *ReadWriter) PeerChunkSize() uint32 {
	return rw.r.ChunkSize()
}
