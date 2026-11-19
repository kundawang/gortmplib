package message_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/liberrors"
	"github.com/bluenviron/gortmplib/pkg/message"
)

// testPeer is a RTMP message peer whose behavior can be controlled by tests.
type testPeer struct {
	conn net.Conn
	r    *message.Reader
	w    *message.Writer

	mutex   sync.Mutex
	handler func(message.Message)
}

func newTestPeer(conn net.Conn) *testPeer {
	bc := bytecounter.NewReadWriter(conn)

	r := &message.Reader{
		BR:  bc,
		BCR: bc.Reader,
	}
	r.Initialize()

	w := &message.Writer{
		BW:  bc,
		BCW: bc.Writer,
	}
	w.Initialize()

	return &testPeer{
		conn: conn,
		r:    r,
		w:    w,
	}
}

// setHandler changes the function that processes incoming messages.
func (p *testPeer) setHandler(handler func(message.Message)) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.handler = handler
}

// serve runs a single loop that reads messages and dispatches them to the
// current handler. It stops when the connection is closed.
func (p *testPeer) serve() {
	for {
		msg, err := p.r.Read()
		if err != nil {
			return
		}

		p.mutex.Lock()
		handler := p.handler
		p.mutex.Unlock()

		if handler != nil {
			handler(msg)
		}
	}
}

// newPipeClients allocates a ReadWriter and a testPeer connected through an
// in-memory pipe.
func newPipeClients(t *testing.T) (*message.ReadWriter, *testPeer) {
	t.Helper()

	local, remote := net.Pipe()

	bc := bytecounter.NewReadWriter(local)
	rw := message.NewReadWriter(bc, bc, false)

	peer := newTestPeer(remote)
	go peer.serve()

	t.Cleanup(func() {
		local.Close()
		remote.Close()
	})

	return rw, peer
}

// respondToPings returns a handler that responds to ping requests, optionally
// after a delay.
func (p *testPeer) respondToPings(delay time.Duration) func(message.Message) {
	return p.respondToPingsWithMedia(delay, false)
}

// respondToPingsWithMedia returns a handler that responds to ping requests,
// optionally after a delay and optionally after sending a media message.
func (p *testPeer) respondToPingsWithMedia(
	delay time.Duration,
	withMedia bool,
) func(message.Message) {
	return func(msg message.Message) {
		req, ok := msg.(*message.UserControlPingRequest)
		if !ok {
			return
		}

		if withMedia {
			_ = p.w.Write(&message.UserControlStreamBegin{StreamID: 1})
		}

		if delay != 0 {
			time.Sleep(delay)
		}

		_ = p.w.Write(&message.UserControlPingResponse{
			ServerTime: req.ServerTime,
		})
	}
}

// ignorePings returns a handler that never responds to ping requests.
func (p *testPeer) ignorePings() func(message.Message) {
	return func(_ message.Message) {}
}

func TestPing(t *testing.T) {
	for _, ca := range []struct {
		name  string
		delay time.Duration
	}{
		{name: "no delay"},
		{name: "delay", delay: 50 * time.Millisecond},
	} {
		t.Run(ca.name, func(t *testing.T) {
			rw, peer := newPipeClients(t)
			peer.setHandler(peer.respondToPings(ca.delay))

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			rtt, err := rw.Ping(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(t, rtt, ca.delay)
			require.Less(t, rtt, time.Second)
		})
	}
}

func TestPingTimeout(t *testing.T) {
	rw, peer := newPipeClients(t)
	peer.setHandler(peer.ignorePings())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := rw.Ping(ctx)
	require.ErrorIs(t, err, liberrors.ErrPingTimeout)
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)

	// after the timeout, the connection is still usable and a new ping works.
	peer.setHandler(peer.respondToPings(0))

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()

	_, err = rw.Ping(ctx2)
	require.NoError(t, err)
}

func TestPingContextCanceled(t *testing.T) {
	rw, peer := newPipeClients(t)
	peer.setHandler(peer.ignorePings())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := rw.Ping(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPingAlreadyExpiredContext(t *testing.T) {
	rw, _ := newPipeClients(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rw.Ping(ctx)
	require.ErrorIs(t, err, context.Canceled)

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err = rw.Ping(ctx)
	require.ErrorIs(t, err, liberrors.ErrPingTimeout)
}

func TestPingTooFrequent(t *testing.T) {
	rw, peer := newPipeClients(t)

	firstRequest := make(chan struct{}, 1)

	peer.setHandler(func(msg message.Message) {
		if _, ok := msg.(*message.UserControlPingRequest); ok {
			select {
			case firstRequest <- struct{}{}:
			default:
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := rw.Ping(ctx)
		errCh <- err
	}()

	// wait until the first request reached the peer.
	select {
	case <-firstRequest:
	case <-time.After(time.Second):
		t.Fatal("first ping was not sent")
	}

	// a second ping while the first one is in progress is rejected.
	_, err := rw.Ping(context.Background())
	require.ErrorIs(t, err, liberrors.ErrPingTooFrequent)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	// once the slot is free again, a new ping works.
	peer.setHandler(peer.respondToPings(0))

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()

	_, err = rw.Ping(ctx2)
	require.NoError(t, err)
}

func TestPingInterrupted(t *testing.T) {
	rw, peer := newPipeClients(t)
	peer.setHandler(peer.ignorePings())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := rw.Ping(ctx)
		errCh <- err
	}()

	time.Sleep(30 * time.Millisecond)
	rw.InterruptPing()

	require.ErrorIs(t, <-errCh, liberrors.ErrPingClosed)

	// future pings fail deterministically.
	_, err := rw.Ping(context.Background())
	require.ErrorIs(t, err, liberrors.ErrPingClosed)
}

func TestPingPeerClosed(t *testing.T) {
	rw, peer := newPipeClients(t)
	peer.setHandler(peer.ignorePings())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := rw.Ping(ctx)
		errCh <- err
	}()

	time.Sleep(30 * time.Millisecond)
	peer.conn.Close()

	require.ErrorIs(t, <-errCh, liberrors.ErrPingClosed)
}

func TestPingMediaPassthrough(t *testing.T) {
	rw, peer := newPipeClients(t)

	media := []message.Message{
		&message.UserControlStreamBegin{StreamID: 1},
		&message.UserControlSetBufferLength{StreamID: 2, BufferLength: 3},
		&message.SetChunkSize{Value: 128},
	}

	peer.setHandler(func(msg message.Message) {
		req, ok := msg.(*message.UserControlPingRequest)
		if !ok {
			return
		}

		// send media messages, then the response.
		for _, m := range media {
			_ = peer.w.Write(m)
		}

		_ = peer.w.Write(&message.UserControlPingResponse{
			ServerTime: req.ServerTime,
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	rtt, err := rw.Ping(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, rtt, time.Duration(0))

	// every media message is returned by Read, in order, without the response.
	for _, expected := range media {
		msg, err2 := rw.Read()
		require.NoError(t, err2)
		require.Equal(t, expected, msg)
	}
}

func TestPingMediaWhileReading(t *testing.T) {
	rw, peer := newPipeClients(t)

	peer.setHandler(func(msg message.Message) {
		req, ok := msg.(*message.UserControlPingRequest)
		if !ok {
			return
		}

		_ = peer.w.Write(&message.UserControlStreamBegin{StreamID: 1})
		_ = peer.w.Write(&message.UserControlPingResponse{
			ServerTime: req.ServerTime,
		})
	})

	received := make(chan message.Message, 1)

	go func() {
		msg, err := rw.Read()
		if err != nil {
			return
		}

		// the ping response must never be returned to the caller.
		if _, ok := msg.(*message.UserControlPingResponse); ok {
			t.Errorf("unexpected ping response returned by Read")
		}

		received <- msg
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := rw.Ping(ctx)
	require.NoError(t, err)

	select {
	case msg := <-received:
		require.Equal(t, &message.UserControlStreamBegin{StreamID: 1}, msg)
	case <-time.After(time.Second):
		t.Fatal("media message not returned")
	}
}

func TestPingDuplicatedResponse(t *testing.T) {
	rw, peer := newPipeClients(t)

	peer.setHandler(func(msg message.Message) {
		req, ok := msg.(*message.UserControlPingRequest)
		if !ok {
			return
		}

		for range 2 {
			_ = peer.w.Write(&message.UserControlPingResponse{
				ServerTime: req.ServerTime,
			})
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := rw.Ping(ctx)
	require.NoError(t, err)

	// the duplicated response is consumed internally. The next read must not
	// return it, and must return the media message that follows it.
	readCh := make(chan message.Message, 1)
	readErrCh := make(chan error, 1)

	go func() {
		msg, err2 := rw.Read()
		if err2 != nil {
			readErrCh <- err2
			return
		}
		readCh <- msg
	}()

	err = peer.w.Write(&message.UserControlStreamBegin{StreamID: 5})
	require.NoError(t, err)

	select {
	case msg := <-readCh:
		require.Equal(t, &message.UserControlStreamBegin{StreamID: 5}, msg)
	case readErr := <-readErrCh:
		require.NoError(t, readErr)
	case <-time.After(time.Second):
		t.Fatal("message not returned")
	}
}

func TestPingStaleResponse(t *testing.T) {
	rw, peer := newPipeClients(t)

	peer.setHandler(func(msg message.Message) {
		req, ok := msg.(*message.UserControlPingRequest)
		if !ok {
			return
		}

		// send an unsolicited response with an unrelated identifier, followed
		// by the correct one.
		_ = peer.w.Write(&message.UserControlPingResponse{
			ServerTime: req.ServerTime + 1000,
		})
		_ = peer.w.Write(&message.UserControlPingResponse{
			ServerTime: req.ServerTime,
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := rw.Ping(ctx)
	require.NoError(t, err)
}

func TestPingAutomaticResponsePreserved(t *testing.T) {
	local, remote := net.Pipe()
	t.Cleanup(func() {
		local.Close()
		remote.Close()
	})

	bc1 := bytecounter.NewReadWriter(local)
	rw1 := message.NewReadWriter(bc1, bc1, false)

	bc2 := bytecounter.NewReadWriter(remote)
	rw2 := message.NewReadWriter(bc2, bc2, false)

	// rw2 sends a ping request while reading the response concurrently;
	// rw1 must respond automatically even when the application reads the
	// request.
	type result struct {
		msg message.Message
		err error
	}
	responseCh := make(chan result, 1)

	go func() {
		err := rw2.Write(&message.UserControlPingRequest{ServerTime: 12345})

		var res result
		if err != nil {
			res.err = err
		} else {
			res.msg, res.err = rw2.Read()
		}
		responseCh <- res
	}()

	msg, err := rw1.Read()
	require.NoError(t, err)
	require.Equal(t, &message.UserControlPingRequest{ServerTime: 12345}, msg)

	res := <-responseCh
	require.NoError(t, res.err)
	require.Equal(t, &message.UserControlPingResponse{ServerTime: 12345}, res.msg)
}

func TestPingConcurrent(t *testing.T) {
	rw, peer := newPipeClients(t)
	peer.setHandler(peer.respondToPings(0))

	var wg sync.WaitGroup

	// only one ping can run at a time: concurrent callers either get a result
	// or ErrPingTooFrequent, but never hang or corrupt the stream.
	for range 10 {
		wg.Go(func() {
			for range 5 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := rw.Ping(ctx)
				cancel()
				require.True(t,
					err == nil || errors.Is(err, liberrors.ErrPingTooFrequent),
					"unexpected error: %v", err)
			}
		})
	}

	wg.Wait()

	// the connection is still consistent after concurrent pings.
	peer.setHandler(peer.respondToPings(0))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := rw.Ping(ctx)
	require.NoError(t, err)
}

func TestPingWhileReadBlocked(t *testing.T) {
	rw, peer := newPipeClients(t)
	peer.setHandler(peer.respondToPings(0))

	// Read is already blocked on the stream when the ping starts: Read must
	// hand the stream over to the ping reader, so that the response is
	// processed even if the application stops reading.
	readStarted := make(chan struct{})
	readCh := make(chan error, 1)

	go func() {
		close(readStarted)

		// block until the connection is closed; the ping response is consumed
		// internally and must not unblock this read.
		for {
			_, err := rw.Read()
			if err != nil {
				readCh <- err
				return
			}
		}
	}()

	<-readStarted

	// give Read the time to block on the stream.
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := rw.Ping(ctx)
	require.NoError(t, err)

	// the ping response was consumed internally: closing the connection must
	// be what unblocks the reader.
	rw.InterruptPing()

	select {
	case readErr := <-readCh:
		require.Error(t, readErr)
	case <-time.After(100 * time.Millisecond):
		// the read is still blocked on the stream, which is expected: only a
		// connection error can unblock it.
	}
}
