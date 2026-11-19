package gortmplib_test

import (
	"bytes"
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/gortmplib/pkg/liberrors"
	"github.com/bluenviron/gortmplib/pkg/message"
	"github.com/bluenviron/gortmplib/pkg/rawmessage"
)

// newPublishPair creates a client in publish mode and the matching server
// connection, both of them initialized.
func newPublishPair(t *testing.T) (*gortmplib.Client, *gortmplib.ServerConn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	serverConnCh := make(chan *gortmplib.ServerConn, 1)
	serverErrCh := make(chan error, 1)

	go func() {
		nconn, err2 := ln.Accept()
		if err2 != nil {
			serverErrCh <- err2
			return
		}
		t.Cleanup(func() { nconn.Close() })

		sc := &gortmplib.ServerConn{RW: nconn}

		err2 = sc.Initialize()
		if err2 != nil {
			serverErrCh <- err2
			return
		}

		err2 = sc.AcceptConn()
		if err2 != nil {
			serverErrCh <- err2
			return
		}

		err2 = sc.AcceptAction()
		if err2 != nil {
			serverErrCh <- err2
			return
		}

		serverConnCh <- sc
	}()

	u, err := url.Parse("rtmp://" + ln.Addr().String() + "/stream")
	require.NoError(t, err)

	c := &gortmplib.Client{
		URL:     u,
		Publish: true,
	}
	err = c.Initialize(context.Background())
	require.NoError(t, err)
	t.Cleanup(c.Close)

	select {
	case err = <-serverErrCh:
		require.NoError(t, err)
		t.FailNow()
		return nil, nil

	case sc := <-serverConnCh:
		t.Cleanup(func() { sc.RW.(net.Conn).Close() })
		return c, sc
	}
}

func TestClientSetChunkSizeRuntime(t *testing.T) {
	c, sc := newPublishPair(t)

	// the initial size announced during initialization
	require.Equal(t, uint32(65536), c.ChunkSize())
	require.Equal(t, uint32(65536), c.PeerChunkSize())
	require.Equal(t, uint32(65536), sc.PeerChunkSize())

	type received struct {
		control *message.SetChunkSize
		video   *message.Video
	}

	receivedCh := make(chan received, 16)
	readErrCh := make(chan error, 1)

	go func() {
		for {
			msg, err := sc.Read()
			if err != nil {
				readErrCh <- err
				return
			}

			switch msg := msg.(type) {
			case *message.SetChunkSize:
				receivedCh <- received{control: msg}
			case *message.Video:
				receivedCh <- received{video: msg}
			}
		}
	}()

	payloads := [][]byte{
		bytes.Repeat([]byte{0x11}, 300*1024),
		bytes.Repeat([]byte{0x22}, 150*1024),
		bytes.Repeat([]byte{0x33}, 20),
	}
	timestamps := []time.Duration{0, 33 * time.Millisecond, 66 * time.Millisecond}

	writeVideo := func(i int) {
		err := c.Write(&message.Video{
			ChunkStreamID:   message.VideoChunkStreamID,
			MessageStreamID: 0x1000000,
			Codec:           message.CodecH264,
			FrameType:       message.VideoFrameTypeKeyFrame,
			PacketType:      message.VideoPacketTypeAU,
			AU:              payloads[i],
			DTS:             timestamps[i],
		})
		require.NoError(t, err)
	}

	writeVideo(0)

	require.NoError(t, c.SetChunkSize(4096))
	require.Equal(t, uint32(4096), c.ChunkSize())
	writeVideo(1)

	require.NoError(t, c.SetChunkSize(128))
	require.Equal(t, uint32(128), c.ChunkSize())
	writeVideo(2)

	// repeated setting of the size in use must not emit another control message.
	require.NoError(t, c.SetChunkSize(128))

	next := func() received {
		select {
		case r := <-receivedCh:
			return r
		case err := <-readErrCh:
			require.NoError(t, err)
			t.FailNow()
			return received{}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
			return received{}
		}
	}

	// first frame, then control message, then second frame, control message, third frame.
	var frames []*message.Video

	for _, size := range []uint32{4096, 128} {
		r := next()
		if r.control == nil {
			frames = append(frames, r.video)
			r = next()
		}

		require.NotNil(t, r.control)
		require.Equal(t, &message.SetChunkSize{Value: size}, r.control)
		// the peer applied the new size right after the control message.
		require.Equal(t, size, sc.PeerChunkSize())
	}

	r := next()
	require.NotNil(t, r.video)
	frames = append(frames, r.video)

	// no additional (repeated) control message
	select {
	case r = <-receivedCh:
		require.Nil(t, r.control)
		frames = append(frames, r.video)

	case <-time.After(100 * time.Millisecond):
	}

	require.Len(t, frames, 3)
	for i, f := range frames {
		require.Equal(t, payloads[i], f.AU)
		require.Equal(t, timestamps[i], f.DTS)
		require.Equal(t, byte(message.VideoChunkStreamID), f.ChunkStreamID)
		require.Equal(t, uint32(0x1000000), f.MessageStreamID)
	}

	// the server can change the size used towards the client as well.
	require.NoError(t, sc.SetChunkSize(2048))
	require.Equal(t, uint32(2048), sc.ChunkSize())

	msg, err := c.Read()
	require.NoError(t, err)
	require.Equal(t, &message.SetChunkSize{Value: 2048}, msg)
	require.Equal(t, uint32(2048), c.PeerChunkSize())
}

func TestClientSetChunkSizeErrors(t *testing.T) {
	t.Run("not initialized", func(t *testing.T) {
		c := &gortmplib.Client{}
		err := c.SetChunkSize(1024)
		require.ErrorAs(t, err, &liberrors.ErrClientNotInitialized{})
	})

	t.Run("invalid", func(t *testing.T) {
		c, _ := newPublishPair(t)

		err := c.SetChunkSize(0)
		require.ErrorAs(t, err, &liberrors.ErrChunkSizeInvalid{})
		require.Equal(t, uint32(65536), c.ChunkSize())

		err = c.SetChunkSize(rawmessage.MaxChunkSize + 1)
		require.ErrorAs(t, err, &liberrors.ErrChunkSizeInvalid{})
		require.Equal(t, uint32(65536), c.ChunkSize())
	})

	t.Run("closed", func(t *testing.T) {
		c, _ := newPublishPair(t)
		c.Close()

		err := c.SetChunkSize(1024)
		require.ErrorAs(t, err, &liberrors.ErrClientClosed{})
	})
}

type failWriteConn struct {
	net.Conn
	mu   sync.Mutex
	fail bool
}

func (c *failWriteConn) setFail(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fail = v
}

func (c *failWriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	fail := c.fail
	c.mu.Unlock()

	if fail {
		return 0, &net.OpError{Op: "write", Err: net.ErrClosed}
	}

	return c.Conn.Write(p)
}

func TestClientSetChunkSizeWriteFailure(t *testing.T) {
	fc := &failWriteConn{}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		nconn, err2 := ln.Accept()
		if err2 != nil {
			return
		}
		defer nconn.Close()

		sc := &gortmplib.ServerConn{RW: nconn}
		if sc.Initialize() != nil || sc.AcceptConn() != nil || sc.AcceptAction() != nil {
			return
		}

		for {
			_, err2 = sc.Read()
			if err2 != nil {
				return
			}
		}
	}()

	u, err := url.Parse("rtmp://" + ln.Addr().String() + "/stream")
	require.NoError(t, err)

	c := &gortmplib.Client{
		URL:     u,
		Publish: true,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			nconn, err2 := (&net.Dialer{}).DialContext(ctx, network, address)
			if err2 != nil {
				return nil, err2
			}
			fc.Conn = nconn
			return fc, nil
		},
	}
	err = c.Initialize(context.Background())
	require.NoError(t, err)
	defer c.Close()

	require.Equal(t, uint32(65536), c.ChunkSize())

	fc.setFail(true)
	err = c.SetChunkSize(1024)
	require.Error(t, err)
	require.NotErrorAs(t, err, &liberrors.ErrChunkSizeInvalid{})
	// the chunk size must be unchanged after a failed write.
	require.Equal(t, uint32(65536), c.ChunkSize())

	// like any write on a failed connection, subsequent writes fail as well:
	// the writer error is sticky.
	fc.setFail(false)
	require.Error(t, c.SetChunkSize(1024))
	require.Equal(t, uint32(65536), c.ChunkSize())

	c.Close()
	<-serverDone
}

func TestServerConnSetChunkSizeErrors(t *testing.T) {
	sc := &gortmplib.ServerConn{}
	err := sc.SetChunkSize(1024)
	require.ErrorAs(t, err, &liberrors.ErrServerConnNotInitialized{})
}

func TestWriterSetChunkSizeUnsupported(t *testing.T) {
	// dummyConn does not implement ChunkSizeUpdater.
	var input bytes.Buffer
	var output bytes.Buffer
	c := &dummyConn{
		rw: &readWriter{
			Reader: &input,
			Writer: &output,
		},
	}
	c.initialize()

	w := &gortmplib.Writer{
		Conn:   c,
		Tracks: []*gortmplib.Track{{Codec: testCodecH264}},
	}
	err := w.Initialize()
	require.NoError(t, err)

	err = w.SetChunkSize(1024)
	require.ErrorAs(t, err, &liberrors.ErrChunkSizeUnsupported{})
	require.Zero(t, w.ChunkSize())
}

// TestWriterChunkSizeChangeEndToEnd publishes H264 (large frames) and AAC
// (small frames) concurrently while changing the outgoing chunk size, and
// verifies that the receiving side reassembles every message with the original
// track, timestamp and payload.
func TestWriterChunkSizeChangeEndToEnd(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	const (
		videoFrames = 8
		audioFrames = 24
		videoSize   = 300 * 1024
	)

	type videoFrame struct {
		pts time.Duration
		dts time.Duration
		au  [][]byte
	}

	type audioFrame struct {
		pts time.Duration
		au  []byte
	}

	videoCh := make(chan videoFrame, videoFrames)
	audioCh := make(chan audioFrame, audioFrames)
	readyCh := make(chan error, 1)
	initCh := make(chan error, 1)
	finalPeerChunkSize := make(chan uint32, 1)

	go func() {
		nconn, err2 := ln.Accept()
		if err2 != nil {
			readyCh <- err2
			return
		}
		defer nconn.Close()

		sc := &gortmplib.ServerConn{RW: nconn}

		err2 = sc.Initialize()
		if err2 != nil {
			readyCh <- err2
			return
		}

		err2 = sc.AcceptConn()
		if err2 != nil {
			readyCh <- err2
			return
		}

		err2 = sc.AcceptAction()
		if err2 != nil {
			readyCh <- err2
			return
		}

		readyCh <- nil

		r := &gortmplib.Reader{Conn: sc}

		err2 = r.Initialize()
		if err2 != nil {
			initCh <- err2
			return
		}

		tracks := r.Tracks()
		require.Len(t, tracks, 2)
		require.IsType(t, &codecs.H264{}, tracks[0].Codec)
		require.IsType(t, &codecs.MPEG4Audio{}, tracks[1].Codec)

		r.OnDataH264(tracks[0], func(pts, dts time.Duration, au [][]byte) {
			// skip the SPS/PPS configuration, which is not a media frame.
			if len(au) != 0 && h264.NALUType(au[0][0]&0x1F) == h264.NALUTypeSPS {
				return
			}

			cp := make([][]byte, len(au))
			for i := range au {
				cp[i] = append([]byte(nil), au[i]...)
			}
			videoCh <- videoFrame{pts: pts, dts: dts, au: cp}
		})

		r.OnDataMPEG4Audio(tracks[1], func(pts time.Duration, au []byte) {
			audioCh <- audioFrame{pts: pts, au: append([]byte(nil), au...)}
		})

		initCh <- nil

		for {
			err2 = r.Read()
			if err2 != nil {
				finalPeerChunkSize <- sc.PeerChunkSize()
				return
			}
		}
	}()

	u, err := url.Parse("rtmp://" + ln.Addr().String() + "/stream")
	require.NoError(t, err)

	c := &gortmplib.Client{
		URL:     u,
		Publish: true,
	}
	err = c.Initialize(context.Background())
	require.NoError(t, err)
	defer c.Close()

	// the server side has completed its handshake once the client is initialized.
	require.NoError(t, <-readyCh)

	w := &gortmplib.Writer{
		Conn: c,
		Tracks: []*gortmplib.Track{
			{Codec: testCodecH264},
			{Codec: &codecs.MPEG4Audio{
				Config: &mpeg4audio.AudioSpecificConfig{
					Type:          2,
					SampleRate:    44100,
					ChannelConfig: 2,
					ChannelCount:  2, //nolint:staticcheck
				},
			}},
		},
	}
	err = w.Initialize()
	require.NoError(t, err)

	// warm-up media, consumed by Reader.Initialize() while it detects tracks.
	// These frames are delivered to the callbacks as well, since the reader
	// rewinds and replays the messages it inspected.
	warmupVideo := make([]videoFrame, 3)
	for i := range warmupVideo {
		warmupVideo[i] = videoFrame{
			pts: time.Duration(i) * time.Second,
			dts: time.Duration(i) * time.Second,
			au:  [][]byte{{byte(h264.NALUTypeNonIDR), 0x01}},
		}

		err = w.WriteH264(w.Tracks[0],
			warmupVideo[i].pts,
			warmupVideo[i].dts,
			warmupVideo[i].au)
		require.NoError(t, err)
	}

	require.NoError(t, <-initCh)

	newVideoNALU := func(i int) []byte {
		b := make([]byte, videoSize)
		b[0] = byte(h264.NALUTypeNonIDR)
		b[1] = byte(i)
		for j := 2; j < len(b); j++ {
			b[j] = byte(i + 1)
		}
		return b
	}

	expectedVideo := make([]videoFrame, videoFrames)
	for i := range videoFrames {
		expectedVideo[i] = videoFrame{
			pts: time.Duration(3+i) * time.Second,
			dts: time.Duration(3+i) * time.Second,
			au:  [][]byte{newVideoNALU(i)},
		}
	}

	expectedAudio := make([]audioFrame, audioFrames)
	for i := range audioFrames {
		au := make([]byte, 200)
		au[0] = byte(i)
		for j := 1; j < len(au); j++ {
			au[j] = byte(i + 1)
		}
		expectedAudio[i] = audioFrame{
			pts: time.Duration(3+i) * 500 * time.Millisecond,
			au:  au,
		}
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range videoFrames {
			require.NoError(t, w.WriteH264(w.Tracks[0], expectedVideo[i].pts,
				expectedVideo[i].dts, expectedVideo[i].au))
		}
	})

	wg.Go(func() {
		for i := range audioFrames {
			require.NoError(t, w.WriteMPEG4Audio(w.Tracks[1], expectedAudio[i].pts, expectedAudio[i].au))
		}
	})

	wg.Go(func() {
		for _, size := range []uint32{1024, 65536, 128, 8192, 4096, 65536} {
			require.NoError(t, w.SetChunkSize(size))
			time.Sleep(time.Millisecond)
		}
	})

	for i, want := range append(warmupVideo, expectedVideo...) {
		select {
		case got := <-videoCh:
			require.Equalf(t, want.pts, got.pts, "video frame %d", i)
			require.Equalf(t, want.dts, got.dts, "video frame %d", i)
			require.Equalf(t, want.au, got.au, "video frame %d", i)

		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for video frame")
		}
	}

	for i, want := range expectedAudio {
		select {
		case got := <-audioCh:
			require.Equalf(t, want.pts, got.pts, "audio frame %d", i)
			require.Equalf(t, want.au, got.au, "audio frame %d", i)

		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for audio frame")
		}
	}

	wg.Wait()

	require.Equal(t, uint32(65536), w.ChunkSize())

	// closing the publishing side makes the receiving side drain the
	// remaining messages (including the last SetChunkSize) and fail.
	c.Close()

	select {
	case size := <-finalPeerChunkSize:
		require.Equal(t, uint32(65536), size)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the receiving side to finish")
	}
}
