package gortmplib_test

import (
	"context"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/gortmplib/pkg/liberrors"
	"github.com/bluenviron/gortmplib/pkg/message"
)

// newPingPair establishes a client/server connection over a loopback TCP
// listener. The server does not start any media goroutine; the caller is in
// charge of draining the server side.
func newPingPair(t *testing.T) (*gortmplib.Client, *gortmplib.ServerConn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:9121")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	serverReady := make(chan *gortmplib.ServerConn, 1)
	serverErr := make(chan error, 1)

	go func() {
		nconn, err2 := ln.Accept()
		if err2 != nil {
			serverErr <- err2
			return
		}

		sc := &gortmplib.ServerConn{RW: nconn}
		err2 = sc.Initialize()
		if err2 != nil {
			nconn.Close()
			serverErr <- err2
			return
		}

		err2 = sc.AcceptConn()
		if err2 != nil {
			nconn.Close()
			serverErr <- err2
			return
		}

		err2 = sc.AcceptAction()
		if err2 != nil {
			nconn.Close()
			serverErr <- err2
			return
		}

		serverReady <- sc
	}()

	u, err := url.Parse("rtmp://127.0.0.1:9121/stream")
	require.NoError(t, err)

	c := &gortmplib.Client{
		URL:     u,
		Publish: true,
	}
	err = c.Initialize(context.Background())
	require.NoError(t, err)
	t.Cleanup(c.Close)

	select {
	case sc := <-serverReady:
		t.Cleanup(func() { sc.RW.(net.Conn).Close() })
		return c, sc

	case err = <-serverErr:
		require.NoError(t, err)
		return nil, nil

	case <-time.After(2 * time.Second):
		t.Fatal("server did not accept the connection")
		return nil, nil
	}
}

func TestClientServerConnPing(t *testing.T) {
	client, server := newPingPair(t)

	// keep draining the server side, so that it can answer pings.
	go func() {
		for {
			_, err := server.Read()
			if err != nil {
				return
			}
		}
	}()

	// the client is not reading: Ping must read the response by itself.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	rtt, err := client.Ping(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, rtt, time.Duration(0))
	require.Less(t, rtt, time.Second)
}

func TestServerConnPingClient(t *testing.T) {
	client, server := newPingPair(t)

	go func() {
		for {
			_, err := client.Read()
			if err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	rtt, err := server.Ping(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, rtt, time.Duration(0))
	require.Less(t, rtt, time.Second)
}

func TestPingTimeoutEndToEnd(t *testing.T) {
	client, _ := newPingPair(t)

	// the server is frozen and does not read nor answer.

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.Ping(ctx)
	require.ErrorIs(t, err, liberrors.ErrPingTimeout)
	require.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)
}

func TestPingMediaPassthroughEndToEnd(t *testing.T) {
	client, server := newPingPair(t)

	// the server sends a media message and replies to pings.
	go func() {
		err := server.Write(&message.UserControlStreamBegin{StreamID: 1})
		if err != nil {
			return
		}

		for {
			_, err2 := server.Read()
			if err2 != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	rtt, err := client.Ping(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, rtt, time.Duration(0))

	// the ping was executed by Ping itself (the application was not reading);
	// the media message must still be available.
	msg, err := client.Read()
	require.NoError(t, err)
	require.Equal(t, &message.UserControlStreamBegin{StreamID: 1}, msg)
}

func TestPingClosedEndToEnd(t *testing.T) {
	client, _ := newPingPair(t)

	// the server is frozen: it neither reads nor answers.
	pingDone := make(chan error, 1)
	go func() {
		_, err := client.Ping(context.Background())
		pingDone <- err
	}()

	time.Sleep(50 * time.Millisecond)
	client.Close()

	select {
	case err := <-pingDone:
		require.ErrorIs(t, err, liberrors.ErrPingClosed)

	case <-time.After(time.Second):
		t.Fatal("Ping did not return after Close")
	}

	// after Close, Ping fails deterministically.
	_, err := client.Ping(context.Background())
	require.ErrorIs(t, err, liberrors.ErrPingClosed)
}

func TestPingNotConnected(t *testing.T) {
	c := &gortmplib.Client{
		URL: &url.URL{Scheme: "rtmp", Host: "127.0.0.1:9121", Path: "stream"},
	}

	_, err := c.Ping(context.Background())
	require.ErrorIs(t, err, liberrors.ErrPingNotConnected)

	sc := &gortmplib.ServerConn{}
	_, err = sc.Ping(context.Background())
	require.ErrorIs(t, err, liberrors.ErrPingNotConnected)
}

func TestPingTooFrequentEndToEnd(t *testing.T) {
	client, _ := newPingPair(t)

	// the server is frozen and does not answer pings.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx)
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)

	_, err := client.Ping(context.Background())
	require.ErrorIs(t, err, liberrors.ErrPingTooFrequent)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

// delayedConn delays reads, which in turn delays the automatic ping responses
// generated by ServerConn, allowing to verify the measured round-trip time.
type delayedConn struct {
	net.Conn

	delay atomic.Int64
}

func (c *delayedConn) Read(p []byte) (int, error) {
	if d := c.delay.Load(); d != 0 {
		time.Sleep(time.Duration(d))
	}

	return c.Conn.Read(p)
}

func newDelayedServer(t *testing.T, delay time.Duration) *gortmplib.Client {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:9121")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	serverReady := make(chan struct{})

	go func() {
		nconn, err2 := ln.Accept()
		if err2 != nil {
			return
		}
		defer nconn.Close()

		dc := &delayedConn{Conn: nconn}

		sc := &gortmplib.ServerConn{RW: dc}
		err2 = sc.Initialize()
		if err2 != nil {
			return
		}

		err2 = sc.AcceptConn()
		if err2 != nil {
			return
		}

		err2 = sc.AcceptAction()
		if err2 != nil {
			return
		}

		dc.delay.Store(int64(delay))
		close(serverReady)

		// drain and automatically answer pings.
		for {
			_, err3 := sc.Read()
			if err3 != nil {
				return
			}
		}
	}()

	u, err := url.Parse("rtmp://127.0.0.1:9121/stream")
	require.NoError(t, err)

	c := &gortmplib.Client{
		URL:     u,
		Publish: true,
	}
	err = c.Initialize(context.Background())
	require.NoError(t, err)
	t.Cleanup(c.Close)

	select {
	case <-serverReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not accept the connection")
	}

	return c
}

func TestPingRTTWithDelay(t *testing.T) {
	const delay = 100 * time.Millisecond

	client := newDelayedServer(t, delay)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rtt, err := client.Ping(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, rtt, delay)
	require.Less(t, rtt, delay+time.Second)
}

func TestPingDoesNotRunMediaCallbacks(t *testing.T) {
	client, server := newPingPair(t)

	// the server replies to pings and, before doing so, sends media messages.
	go func() {
		for {
			msg, err := server.Read()
			if err != nil {
				return
			}

			req, ok := msg.(*message.UserControlPingRequest)
			if !ok {
				continue
			}

			err = server.Write(&message.UserControlStreamBegin{StreamID: 1})
			if err != nil {
				return
			}

			err = server.Write(&message.UserControlPingResponse{
				ServerTime: req.ServerTime,
			})
			if err != nil {
				return
			}
		}
	}()

	// no media reader is started: Ping must not deliver anything to any
	// callback, since gortmplib has no notification mechanism here, and the
	// messages must remain available to Read.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := client.Ping(ctx)
	require.NoError(t, err)

	msg, err := client.Read()
	require.NoError(t, err)
	require.Equal(t, &message.UserControlStreamBegin{StreamID: 1}, msg)
}

func TestPingWithMediaStream(t *testing.T) {
	client, server := newPingPair(t)

	// the server publishes a H264 stream, replies to pings and drains the
	// connection; the client plays it.
	w := &gortmplib.Writer{
		Conn: server,
		Tracks: []*gortmplib.Track{{Codec: &codecs.H264{
			SPS: testCodecH264.SPS,
			PPS: testCodecH264.PPS,
		}}},
	}
	err := w.Initialize()
	require.NoError(t, err)

	publishDone := make(chan error, 1)

	go func() {
		for i := range 5 {
			err2 := w.WriteH264(
				w.Tracks[0],
				time.Duration(i)*time.Second,
				time.Duration(i)*time.Second,
				[][]byte{{0x09, 0xf0}},
			)
			if err2 != nil {
				publishDone <- err2
				return
			}
		}
		publishDone <- nil
	}()

	r := &gortmplib.Reader{Conn: client}
	err = r.Initialize()
	require.NoError(t, err)

	tracks := r.Tracks()
	require.Len(t, tracks, 1)

	frames := make(chan time.Duration, 10)
	r.OnDataH264(tracks[0], func(pts time.Duration, _ time.Duration, _ [][]byte) {
		frames <- pts
	})

	readDone := make(chan error, 1)
	go func() {
		for {
			err2 := r.Read()
			if err2 != nil {
				readDone <- err2
				return
			}
		}
	}()

	// perform pings while media is being received; each ping must be
	// independent from the media flow.
	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err2 := client.Ping(ctx)
		cancel()
		require.NoError(t, err2)
	}

	require.NoError(t, <-publishDone)

	// 5 frames plus the initial SPS/PPS access unit.
	received := map[time.Duration]struct{}{}
	for range 6 {
		select {
		case pts := <-frames:
			received[pts] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for frames")
		}
	}

	for i := range 5 {
		_, ok := received[time.Duration(i)*time.Second]
		require.True(t, ok, "missing frame at %v", time.Duration(i)*time.Second)
	}

	client.Close()
	require.Error(t, <-readDone)
}
