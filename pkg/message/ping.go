package message

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/bluenviron/gortmplib/pkg/liberrors"
)

// recentPingIDCount is the number of recently used ping identifiers that are
// remembered in order to avoid reusing them when the 32-bit space wraps around.
const recentPingIDCount = 64

// maxPingBufferedMessages bounds the number of messages that the ping reader
// buffers for Read while the application is not reading. Beyond this limit the
// ping reader stops and Read takes over.
const maxPingBufferedMessages = 1024

type pingRequest struct {
	id       uint32
	sentAt   time.Time
	done     chan struct{}
	rtt      time.Duration
	err      error
	finished bool
}

func (p *pingRequest) finishLocked(rtt time.Duration, err error) {
	if p.finished {
		return
	}

	p.finished = true
	p.rtt = rtt
	p.err = err
	close(p.done)
}

// pingTracker keeps the state of one ping at a time.
//
// Requests are identified by a 32-bit identifier that is sent in the Server
// Time field and echoed back by the peer. Responses are matched by exact
// identifier, therefore wrap-around of the 32-bit space and duplicated or
// stale responses are handled deterministically.
type pingTracker struct {
	mutex     sync.Mutex
	cur       *pingRequest
	nextID    uint32
	recentIDs [recentPingIDCount]uint32
	recentPos int
	closed    bool
}

func (p *pingTracker) initialize() {
	// seed identifiers with the current time, then advance them monotonically.
	p.nextID = uint32(time.Now().UnixMilli())
}

func (p *pingTracker) allocateIDLocked() uint32 {
	for {
		p.nextID++

		// zero is never used, so that an unset Server Time cannot match.
		if p.nextID == 0 {
			continue
		}

		used := false
		for _, id := range p.recentIDs {
			if id == p.nextID {
				used = true
				break
			}
		}

		if used {
			continue
		}

		p.recentIDs[p.recentPos] = p.nextID
		p.recentPos = (p.recentPos + 1) % recentPingIDCount
		return p.nextID
	}
}

func (p *pingTracker) begin() (*pingRequest, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil, liberrors.ErrPingClosed
	}

	if p.cur != nil {
		return nil, liberrors.ErrPingTooFrequent
	}

	req := &pingRequest{
		id:     p.allocateIDLocked(),
		sentAt: time.Now(),
		done:   make(chan struct{}),
	}
	p.cur = req

	return req, nil
}

func (p *pingTracker) hasRequest() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.cur != nil
}

func (p *pingTracker) finishLocked(req *pingRequest, rtt time.Duration, err error) {
	if p.cur == req {
		p.cur = nil
	}
	req.finishLocked(rtt, err)
}

// finishCurrent completes the request in progress, if any, with the given
// error. It is used when the connection cannot deliver a response anymore.
func (p *pingTracker) finishCurrent(err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.cur != nil {
		p.finishLocked(p.cur, 0, err)
	}
}

// withdraw completes req with a nil result unless it was already completed.
// The second return value reports whether the request was already completed.
func (p *pingTracker) withdraw(req *pingRequest) (time.Duration, bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if req.finished {
		return req.rtt, true, req.err
	}

	p.finishLocked(req, 0, nil)

	return 0, false, nil
}

// handleResponse matches a response identifier with the request in progress.
// It returns true when the response belongs to the request in progress.
func (p *pingTracker) handleResponse(id uint32) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	req := p.cur
	if req == nil || req.id != id {
		// unsolicited, stale, duplicated or out-of-window response.
		return false
	}

	p.finishLocked(req, time.Since(req.sentAt), nil)

	return true
}

// isOwnResponse reports whether id belongs to a request issued by this
// tracker, including completed ones. It allows to discard duplicated or late
// responses instead of forwarding them to the caller.
func (p *pingTracker) isOwnResponse(id uint32) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, recentID := range p.recentIDs {
		if recentID == id {
			return true
		}
	}

	return false
}

// interrupt marks the tracker as closed and completes the request in progress,
// if any, with the given error.
func (p *pingTracker) interrupt(err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.closed = true

	if p.cur != nil {
		p.finishLocked(p.cur, 0, err)
	}
}

// isClosedError reports whether err means that the connection is closed.
func isClosedError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed)
}

// Ping sends a ping request to the peer and waits for the corresponding
// response, returning the measured round-trip time.
//
// Responses are matched with requests through the Server Time field, so media
// messages received in the meanwhile are left untouched and continue to be
// returned by Read. Ping never runs the media read callbacks: when another
// goroutine is reading from the connection, that goroutine processes the
// response; when no goroutine is reading, Ping reads incoming messages itself
// and buffers everything that is not the matching response (including media
// messages), so that subsequent Read calls return them in order.
//
// Only one ping can be in progress at a time: calling Ping while another ping
// is in progress returns ErrPingTooFrequent.
//
// When ctx expires, Ping returns ErrPingTimeout if the deadline was exceeded,
// or the context error if the context was canceled. When the connection is
// closed, Ping returns ErrPingClosed.
func (rw *ReadWriter) Ping(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, liberrors.ErrPingTimeout
		}
		return 0, err
	}

	req, err := rw.p.begin()
	if err != nil {
		return 0, err
	}

	err = rw.w.Write(&UserControlPingRequest{
		ServerTime: req.id,
	})
	if err != nil {
		if isClosedError(err) {
			rw.InterruptPing()
		} else {
			rw.p.finishCurrent(err)
		}

		<-req.done
		return req.rtt, req.err
	}

	// when no other goroutine is reading, read the response by ourselves.
	rw.startPingReader()

	select {
	case <-req.done:
		return req.rtt, req.err

	case <-ctx.Done():
		// a response might have been handled concurrently with the context
		// expiring. In that case, prefer the response.
		rtt, completed, err2 := rw.p.withdraw(req)
		if completed {
			return rtt, err2
		}

		// wake up readers, so that they can take over once the ping reader
		// releases the stream.
		rw.broadcast()

		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return 0, liberrors.ErrPingTimeout
		}
		return 0, ctx.Err()
	}
}

// startPingReader starts the goroutine that reads incoming messages on behalf
// of Ping, unless another goroutine is already reading.
//
// When Read owns the stream, it will start the ping reader as soon as its
// current read completes (see Read), so that the response is processed even if
// the application stops reading in the meanwhile.
func (rw *ReadWriter) startPingReader() {
	rw.readMutex.Lock()
	defer rw.readMutex.Unlock()

	rw.startPingReaderLocked()
}

func (rw *ReadWriter) startPingReaderLocked() {
	if rw.pingReading || rw.readerOwned {
		return
	}

	// only start the reader if there is a request to wait for.
	if !rw.p.hasRequest() {
		return
	}

	rw.pingReading = true
	go rw.pingReader()
}

// pingReader reads incoming messages while a ping is in progress, so that the
// response is processed even if the application is not reading.
//
// Messages that are not the matching response are buffered unchanged, so that
// Read returns the same stream it would have returned without the ping, in the
// same order. The goroutine releases the stream as soon as the ping completes;
// if a read is blocked when that happens, it releases the stream as soon as
// the read returns.
func (rw *ReadWriter) pingReader() {
	for {
		rw.readMutex.Lock()

		// this check and the release of the stream happen while holding the
		// lock, so that startPingReader either observes the stream as owned
		// (and the next request is served by this goroutine), or observes it
		// as free and starts a new goroutine.
		if !rw.p.hasRequest() {
			rw.pingReading = false
			rw.readCond.Broadcast()
			rw.readMutex.Unlock()
			return
		}

		rw.readMutex.Unlock()

		msg, err := rw.r.Read()

		var matched bool
		if err == nil {
			// process outside of the lock: it can perform network writes.
			matched, err = rw.process(msg)
		}

		rw.readMutex.Lock()

		if err != nil {
			rw.pingReading = false
			rw.readCond.Broadcast()
			rw.readMutex.Unlock()

			rw.handleReadError(err)
			return
		}

		// the matching response belongs to Ping and is not returned by Read;
		// everything else is preserved.
		if !matched {
			rw.pending = append(rw.pending, msg)
		}

		// avoid unbounded buffering when the application is not reading. The
		// ping reader stops and Read takes over; a ping that is still in
		// progress then has to be served by Read itself, and times out
		// otherwise.
		stop := len(rw.pending) >= maxPingBufferedMessages
		if stop {
			rw.pingReading = false
		}

		rw.readCond.Broadcast()
		rw.readMutex.Unlock()

		if stop {
			return
		}
	}
}

// handleReadError reports an error that prevents the connection from
// delivering further messages.
func (rw *ReadWriter) handleReadError(err error) {
	if isClosedError(err) {
		rw.InterruptPing()
	} else {
		rw.p.finishCurrent(err)
	}
}

// InterruptPing interrupts a ping in progress reporting the connection as
// closed, and makes future pings fail with ErrPingClosed.
// It is called when the connection is closed or when reading fails.
func (rw *ReadWriter) InterruptPing() {
	rw.p.interrupt(liberrors.ErrPingClosed)
	rw.broadcast()
}

func (rw *ReadWriter) broadcast() {
	rw.readMutex.Lock()
	rw.readCond.Broadcast()
	rw.readMutex.Unlock()
}
