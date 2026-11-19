package message

import (
	"io"
	"sync"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
)

// ReadWriter is a message reader/writer.
type ReadWriter struct {
	r *Reader
	w *Writer
	p pingTracker

	// readMutex protects the fields below and the scheduling of reads.
	readMutex   sync.Mutex
	readCond    *sync.Cond
	readerOwned bool
	pingReading bool
	pending     []Message
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

	readWriter := &ReadWriter{
		r: r,
		w: w,
	}
	readWriter.readCond = sync.NewCond(&readWriter.readMutex)
	readWriter.p.initialize()

	return readWriter
}

// Read reads a message.
//
// Messages buffered while a ping was in progress are returned first, in order.
// Ping responses are consumed internally and are not returned.
func (rw *ReadWriter) Read() (Message, error) {
	for {
		rw.readMutex.Lock()

		for {
			if len(rw.pending) != 0 {
				msg := rw.pending[0]
				rw.pending = rw.pending[1:]

				if len(rw.pending) == 0 {
					rw.pending = nil // release memory
				}

				rw.readCond.Broadcast()
				rw.readMutex.Unlock()

				return msg, nil
			}

			// while the ping reader owns the stream, wait for it to complete
			// or to buffer something, so that messages keep being returned in
			// order.
			if !rw.pingReading {
				break
			}

			rw.readCond.Wait()
		}

		rw.readerOwned = true
		rw.readMutex.Unlock()

		msg, err := rw.r.Read()

		rw.readMutex.Lock()
		rw.readerOwned = false
		rw.readCond.Broadcast()

		// a ping may have started while Read owned the stream. Hand the stream
		// over so that the response is processed even if the caller stops
		// reading.
		if err == nil && rw.p.hasRequest() {
			rw.startPingReaderLocked()
		}

		rw.readMutex.Unlock()

		if err != nil {
			rw.handleReadError(err)
			return nil, err
		}

		matched, err := rw.process(msg)
		if err != nil {
			rw.handleReadError(err)
			return nil, err
		}

		// the matching response is handled internally and is not returned to
		// the caller, as it happened before ping support was introduced.
		if !matched {
			return msg, nil
		}
	}
}

// process handles protocol messages that are received internally.
// It returns true when the message must not be forwarded to the caller.
func (rw *ReadWriter) process(msg Message) (bool, error) {
	switch tmsg := msg.(type) {
	case *Acknowledge:
		rw.w.SetAcknowledgeValue(tmsg.Value)

	case *UserControlPingRequest:
		err := rw.w.Write(&UserControlPingResponse{
			ServerTime: tmsg.ServerTime,
		})
		if err != nil {
			return false, err
		}

	case *UserControlPingResponse:
		// a response matching a request issued by this connection is consumed
		// internally. Duplicated and late responses are consumed as well, so
		// that they are not mistaken for media messages by the caller.
		if rw.p.handleResponse(tmsg.ServerTime) || rw.p.isOwnResponse(tmsg.ServerTime) {
			return true, nil
		}
	}

	return false, nil
}

// Write writes a message.
func (rw *ReadWriter) Write(msg Message) error {
	return rw.w.Write(msg)
}
