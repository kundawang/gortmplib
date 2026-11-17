package gortmplib_test

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/amf0"
	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/handshake"
	"github.com/bluenviron/gortmplib/pkg/liberrors"
	"github.com/bluenviron/gortmplib/pkg/message"
)

// pauseFakeServer is a configurable fake RTMP server used to test the
// play-side pause / resume flow.
type pauseFakeServer struct {
	t *testing.T

	ln net.Listener

	mu              sync.Mutex
	receivedPause   []pauseCommandRecord
	onStatusLevel   string // "status" or "error", applied to pause responses
	respondOnStatus bool
	conn            net.Conn
	mrw             *message.ReadWriter
}

type pauseCommandRecord struct {
	commandID int
	pause     bool
	position  float64
}

func newPauseFakeServer(t *testing.T) *pauseFakeServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:9121")
	require.NoError(t, err)

	s := &pauseFakeServer{
		t:               t,
		ln:              ln,
		respondOnStatus: true,
		onStatusLevel:   "status",
	}

	return s
}

func (s *pauseFakeServer) close() {
	s.ln.Close()

	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
}

func (s *pauseFakeServer) pauseCommands() []pauseCommandRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make([]pauseCommandRecord, len(s.receivedPause))
	copy(ret, s.receivedPause)
	return ret
}

// run accepts one connection and executes the RTMP handshake and the
// play setup. It returns a channel that is closed when the setup completes.
func (s *pauseFakeServer) run() <-chan struct{} {
	setupDone := make(chan struct{})

	go func() {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}

		s.mu.Lock()
		s.conn = conn
		s.mu.Unlock()

		bc := bytecounter.NewReadWriter(conn)

		_, _, err = handshake.DoServer(bc, false)
		if err != nil {
			return
		}

		mrw := message.NewReadWriter(bc, bc, true)

		s.mu.Lock()
		s.mrw = mrw
		s.mu.Unlock()

		if !s.handlePlaySetup(mrw) {
			return
		}

		close(setupDone)

		s.handleCommands(mrw)
	}()

	return setupDone
}

func (s *pauseFakeServer) handlePlaySetup(mrw *message.ReadWriter) bool {
	msg, err := mrw.Read()
	require.NoError(s.t, err)
	require.Equal(s.t, &message.SetWindowAckSize{Value: 2500000}, msg)

	msg, err = mrw.Read()
	require.NoError(s.t, err)
	require.Equal(s.t, &message.SetPeerBandwidth{Value: 2500000, Type: 2}, msg)

	msg, err = mrw.Read()
	require.NoError(s.t, err)
	require.Equal(s.t, &message.SetChunkSize{Value: 65536}, msg)

	msg, err = mrw.Read()
	require.NoError(s.t, err)
	require.Equal(s.t, "connect", msg.(*message.CommandAMF0).Name)

	err = mrw.Write(&message.CommandAMF0{
		ChunkStreamID: 3,
		Name:          "_result",
		CommandID:     1,
		Arguments: []any{
			amf0.Object{
				{Key: "fmsVer", Value: "LNX 9,0,124,2"},
				{Key: "capabilities", Value: float64(31)},
			},
			amf0.Object{
				{Key: "level", Value: "status"},
				{Key: "code", Value: "NetConnection.Connect.Success"},
				{Key: "description", Value: "Connection succeeded."},
				{Key: "objectEncoding", Value: float64(0)},
			},
		},
	})
	require.NoError(s.t, err)

	msg, err = mrw.Read()
	require.NoError(s.t, err)
	require.Equal(s.t, "createStream", msg.(*message.CommandAMF0).Name)

	err = mrw.Write(&message.CommandAMF0{
		ChunkStreamID: 3,
		Name:          "_result",
		CommandID:     2,
		Arguments:     []any{nil, float64(1)},
	})
	require.NoError(s.t, err)

	msg, err = mrw.Read()
	require.NoError(s.t, err)
	require.Equal(s.t, &message.UserControlSetBufferLength{BufferLength: 0x64}, msg)

	msg, err = mrw.Read()
	require.NoError(s.t, err)
	require.Equal(s.t, "play", msg.(*message.CommandAMF0).Name)

	err = mrw.Write(&message.CommandAMF0{
		ChunkStreamID:   5,
		MessageStreamID: 0x1000000,
		Name:            "onStatus",
		CommandID:       3,
		Arguments: []any{
			nil,
			amf0.Object{
				{Key: "level", Value: "status"},
				{Key: "code", Value: "NetStream.Play.Reset"},
				{Key: "description", Value: "play reset"},
			},
		},
	})
	require.NoError(s.t, err)

	return true
}

func (s *pauseFakeServer) handleCommands(mrw *message.ReadWriter) {
	for {
		msg, err := mrw.Read()
		if err != nil {
			return
		}

		cmd, ok := msg.(*message.CommandAMF0)
		if !ok {
			continue
		}

		if cmd.Name != "pause" {
			continue
		}

		require.Len(s.t, cmd.Arguments, 3)
		require.Nil(s.t, cmd.Arguments[0])

		pause, ok := cmd.Arguments[1].(bool)
		require.True(s.t, ok)

		position, ok := cmd.Arguments[2].(float64)
		require.True(s.t, ok)

		s.mu.Lock()
		s.receivedPause = append(s.receivedPause, pauseCommandRecord{
			commandID: cmd.CommandID,
			pause:     pause,
			position:  position,
		})
		respond := s.respondOnStatus
		level := s.onStatusLevel
		s.mu.Unlock()

		if !respond {
			continue
		}

		code := "NetStream.Pause.Notify"
		if !pause {
			code = "NetStream.Unpause.Notify"
		}

		err = mrw.Write(&message.CommandAMF0{
			ChunkStreamID:   5,
			MessageStreamID: 0x1000000,
			Name:            "onStatus",
			CommandID:       cmd.CommandID,
			Arguments: []any{
				nil,
				amf0.Object{
					{Key: "level", Value: level},
					{Key: "code", Value: code},
					{Key: "description", Value: code},
				},
			},
		})
		if err != nil {
			return
		}
	}
}

// writeVideo sends a video message with the given timestamp.
func (s *pauseFakeServer) writeVideo(ts time.Duration) {
	s.mu.Lock()
	mrw := s.mrw
	s.mu.Unlock()

	if mrw == nil {
		return
	}

	err := mrw.Write(&message.Video{
		ChunkStreamID:   message.VideoChunkStreamID,
		MessageStreamID: 0x1000000,
		Codec:           message.CodecH264,
		FrameType:       message.VideoFrameTypeInterFrame,
		PacketType:      message.VideoPacketTypeAU,
		DTS:             ts,
		// a valid AVCC access unit: 4-byte big-endian length + NALU.
		AU: []byte{0x00, 0x00, 0x00, 0x02, 0x65, 0x88},
	})
	require.NoError(s.t, err)
}

// writeMedia sends a video and an audio message with the given timestamp.
func (s *pauseFakeServer) writeMedia(ts time.Duration) {
	s.writeVideo(ts)

	s.mu.Lock()
	mrw := s.mrw
	s.mu.Unlock()

	err := mrw.Write(&message.Audio{
		ChunkStreamID:   message.AudioChunkStreamID,
		MessageStreamID: 0x1000000,
		Codec:           message.CodecMPEG4Audio,
		Rate:            message.AudioRate44100,
		Depth:           message.AudioDepth16,
		IsStereo:        true,
		AACType:         message.AudioAACTypeAU,
		DTS:             ts,
		AU:              []byte{0x10, 0x20},
	})
	require.NoError(s.t, err)
}

func newPlayingClient(t *testing.T, rawURL string) *gortmplib.Client {
	t.Helper()

	u, err := url.Parse(rawURL)
	require.NoError(t, err)

	c := &gortmplib.Client{
		URL: u,
	}
	err = c.Initialize(context.Background())
	require.NoError(t, err)

	return c
}

func TestClientPauseResume(t *testing.T) {
	s := newPauseFakeServer(t)
	setupDone := s.run()
	defer s.close()

	c := newPlayingClient(t, "rtmp://127.0.0.1:9121/stream")
	defer c.Close()
	<-setupDone

	// deliver some media and read it back.
	s.writeMedia(100 * time.Millisecond)
	s.writeMedia(200 * time.Millisecond)

	msg, err := c.Read()
	require.NoError(t, err)
	require.Equal(t, 100*time.Millisecond, msg.(*message.Video).DTS)

	msg, err = c.Read()
	require.NoError(t, err)
	require.Equal(t, 100*time.Millisecond, msg.(*message.Audio).DTS)

	msg, err = c.Read()
	require.NoError(t, err)
	require.Equal(t, 200*time.Millisecond, msg.(*message.Video).DTS)

	msg, err = c.Read()
	require.NoError(t, err)
	require.Equal(t, 200*time.Millisecond, msg.(*message.Audio).DTS)

	// pause at the last delivered media timestamp.
	err = c.Pause(context.Background())
	require.NoError(t, err)
	require.True(t, c.Paused())

	cmds := s.pauseCommands()
	require.Len(t, cmds, 1)
	require.Equal(t, true, cmds[0].pause)
	require.Equal(t, float64(200), cmds[0].position)

	// media sent while paused is not delivered during the pause.
	s.writeMedia(300 * time.Millisecond)
	s.writeMedia(400 * time.Millisecond)

	// give the pump time to enqueue the messages, then prove that Read
	// does not deliver anything while paused.
	time.Sleep(100 * time.Millisecond)

	type readResult struct {
		msg message.Message
		err error
	}

	readMsg := make(chan readResult, 1)

	go func() {
		readMsgResult, readErr := c.Read()
		readMsg <- readResult{msg: readMsgResult, err: readErr}
	}()

	select {
	case res := <-readMsg:
		t.Fatalf("Read delivered a message during pause: %v", res.err)

	case <-time.After(300 * time.Millisecond):
	}

	// resume from the pause point.
	err = c.Resume(context.Background())
	require.NoError(t, err)
	require.False(t, c.Paused())

	cmds = s.pauseCommands()
	require.Len(t, cmds, 2)
	require.Equal(t, false, cmds[1].pause)
	require.Equal(t, float64(200), cmds[1].position)

	// the server restarts from the pause point and re-sends an older
	// keyframe first; it must be dropped so timestamps stay monotonic.
	s.writeMedia(150 * time.Millisecond)
	s.writeMedia(500 * time.Millisecond)

	// the messages sent during the pause are discarded while Read is
	// blocked; only the new 500ms media survives (the 150ms frame is
	// out-of-order and dropped by the pump).
	first := <-readMsg
	require.NoError(t, first.err)
	received := make([]time.Duration, 0, 2)
	received = append(received, mediaDTSOrZero(first.msg))

	msg, err = c.Read()
	require.NoError(t, err)
	received = append(received, mediaDTSOrZero(msg))

	require.Equal(t, []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
	}, received)

	for _, ts := range received {
		require.GreaterOrEqual(t, ts, 200*time.Millisecond)
	}
}

func TestClientPauseResumeRepeat(t *testing.T) {
	s := newPauseFakeServer(t)
	setupDone := s.run()
	defer s.close()

	c := newPlayingClient(t, "rtmp://127.0.0.1:9121/stream")
	defer c.Close()
	<-setupDone

	// pause before any media: position is zero.
	err := c.Pause(context.Background())
	require.NoError(t, err)
	require.True(t, c.Paused())

	// repeated pause is an explicit error and sends nothing.
	err = c.Pause(context.Background())
	require.ErrorIs(t, err, liberrors.ErrClientAlreadyPaused{})

	// resume, then repeated resume is an explicit error.
	err = c.Resume(context.Background())
	require.NoError(t, err)

	err = c.Resume(context.Background())
	require.ErrorIs(t, err, liberrors.ErrClientNotPaused{})

	cmds := s.pauseCommands()
	require.Len(t, cmds, 2)
	require.Equal(t, pauseCommandRecord{commandID: 6, pause: true, position: 0}, cmds[0])
	require.Equal(t, pauseCommandRecord{commandID: 7, pause: false, position: 0}, cmds[1])
}

func TestClientPauseNotPlaying(t *testing.T) {
	// publish mode: pause / resume are rejected and no pump is started.
	ln, err := net.Listen("tcp", "127.0.0.1:9121")
	require.NoError(t, err)
	defer ln.Close()

	done := make(chan struct{})

	go func() {
		conn, err2 := ln.Accept()
		require.NoError(t, err2)
		defer conn.Close()
		bc := bytecounter.NewReadWriter(conn)

		_, _, err2 = handshake.DoServer(bc, false)
		require.NoError(t, err2)

		mrw := message.NewReadWriter(bc, bc, true)

		expectCommand := func(name string) {
			for {
				msg, err3 := mrw.Read()
				require.NoError(t, err3)
				if cmd, ok := msg.(*message.CommandAMF0); ok && cmd.Name == name {
					return
				}
			}
		}

		expectCommand("connect")

		err2 = mrw.Write(&message.CommandAMF0{
			ChunkStreamID: 3,
			Name:          "_result",
			CommandID:     1,
			Arguments: []any{
				amf0.Object{{Key: "fmsVer", Value: "LNX 9,0,124,2"}},
				amf0.Object{{Key: "level", Value: "status"}},
			},
		})
		require.NoError(t, err2)

		expectCommand("releaseStream")
		expectCommand("FCPublish")
		expectCommand("createStream")

		err2 = mrw.Write(&message.CommandAMF0{
			ChunkStreamID: 3,
			Name:          "_result",
			CommandID:     4,
			Arguments:     []any{nil, float64(1)},
		})
		require.NoError(t, err2)

		expectCommand("publish")

		err2 = mrw.Write(&message.CommandAMF0{
			ChunkStreamID:   5,
			MessageStreamID: 0x1000000,
			Name:            "onStatus",
			CommandID:       5,
			Arguments: []any{
				nil,
				amf0.Object{
					{Key: "level", Value: "status"},
					{Key: "code", Value: "NetStream.Publish.Start"},
				},
			},
		})
		require.NoError(t, err2)

		close(done)
	}()

	u, err := url.Parse("rtmp://127.0.0.1:9121/stream")
	require.NoError(t, err)

	c := &gortmplib.Client{
		URL:     u,
		Publish: true,
	}
	err = c.Initialize(context.Background())
	require.NoError(t, err)
	defer c.Close()

	<-done

	err = c.Pause(context.Background())
	require.ErrorIs(t, err, liberrors.ErrClientNotPlaying{})

	err = c.Resume(context.Background())
	require.ErrorIs(t, err, liberrors.ErrClientNotPlaying{})
}

func TestClientPauseRejected(t *testing.T) {
	s := newPauseFakeServer(t)
	s.onStatusLevel = "error"
	setupDone := s.run()
	defer s.close()

	c := newPlayingClient(t, "rtmp://127.0.0.1:9121/stream")
	defer c.Close()
	<-setupDone

	err := c.Pause(context.Background())
	require.Error(t, err)
	require.False(t, c.Paused())

	// media keeps flowing since the pause was rejected and state rolled back.
	s.writeMedia(100 * time.Millisecond)

	msg, err := c.Read()
	require.NoError(t, err)
	require.Equal(t, 100*time.Millisecond, mediaDTSOrZero(msg))

	// a later pause can still succeed after a rejection.
	s.onStatusLevel = "status"

	err = c.Pause(context.Background())
	require.NoError(t, err)
	require.True(t, c.Paused())

	// a rejected resume rolls back to paused.
	s.onStatusLevel = "error"

	err = c.Resume(context.Background())
	require.Error(t, err)
	require.True(t, c.Paused())
}

func TestClientPauseTimeout(t *testing.T) {
	s := newPauseFakeServer(t)
	s.respondOnStatus = false
	setupDone := s.run()
	defer s.close()

	c := newPlayingClient(t, "rtmp://127.0.0.1:9121/stream")
	defer c.Close()
	<-setupDone

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := c.Pause(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// the local state was rolled back: media continues to be delivered.
	require.False(t, c.Paused())

	s.writeMedia(100 * time.Millisecond)

	msg, err := c.Read()
	require.NoError(t, err)
	require.Equal(t, 100*time.Millisecond, mediaDTSOrZero(msg))
}

func TestClientPauseConnectionClosed(t *testing.T) {
	t.Run("server closes during pause", func(t *testing.T) {
		s := newPauseFakeServer(t)
		s.respondOnStatus = false
		setupDone := s.run()

		c := newPlayingClient(t, "rtmp://127.0.0.1:9121/stream")
		<-setupDone

		errCh := make(chan error, 1)

		go func() {
			errCh <- c.Pause(context.Background())
		}()

		time.Sleep(100 * time.Millisecond)
		s.close()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, liberrors.ErrClientClosed{})

		case <-time.After(2 * time.Second):
			t.Fatal("Pause did not return after connection close")
		}

		require.False(t, c.Paused())

		_, err := c.Read()
		require.Error(t, err)

		c.Close()
	})

	t.Run("client close during pause", func(t *testing.T) {
		s := newPauseFakeServer(t)
		s.respondOnStatus = false
		setupDone := s.run()
		defer s.close()

		c := newPlayingClient(t, "rtmp://127.0.0.1:9121/stream")
		<-setupDone

		errCh := make(chan error, 1)

		go func() {
			errCh <- c.Pause(context.Background())
		}()

		time.Sleep(100 * time.Millisecond)
		c.Close()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, liberrors.ErrClientClosed{})

		case <-time.After(2 * time.Second):
			t.Fatal("Pause did not return after client close")
		}

		// operations after close have a defined result.
		err := c.Pause(context.Background())
		require.ErrorIs(t, err, liberrors.ErrClientClosed{})

		_, err = c.Read()
		require.Error(t, err)

		// Close is idempotent.
		c.Close()
	})
}

func TestClientPauseConcurrentRead(t *testing.T) {
	s := newPauseFakeServer(t)
	setupDone := s.run()
	defer s.close()

	c := newPlayingClient(t, "rtmp://127.0.0.1:9121/stream")
	defer c.Close()
	<-setupDone

	const readerCount = 4
	const messagesPerReader = 5

	var wg sync.WaitGroup

	results := make([][]time.Duration, readerCount)

	for i := range readerCount {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			for range messagesPerReader {
				msg, err := c.Read()
				if err != nil {
					return
				}

				results[idx] = append(results[idx], mediaDTSOrZero(msg))
			}
		}(i)
	}

	// stream media: 20 video frames + 20 audio frames = 40 messages.
	for i := range 20 {
		s.writeMedia(time.Duration(100+i*100) * time.Millisecond)
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent readers did not receive all messages")
	}

	// every message is delivered exactly once.
	total := 0
	seen := make(map[time.Duration]int)

	for _, r := range results {
		for _, ts := range r {
			seen[ts]++
			total++
		}
	}

	require.Equal(t, readerCount*messagesPerReader, total)

	for ts, count := range seen {
		// each timestamp carries one video and one audio message.
		require.Equal(t, 2, count, "timestamp %v was not delivered exactly twice", ts)
	}
}

func TestClientPauseWithReader(t *testing.T) {
	s := newPauseFakeServer(t)
	setupDone := s.run()
	defer s.close()

	c := newPlayingClient(t, "rtmp://127.0.0.1:9121/stream")
	defer c.Close()
	<-setupDone

	s.mu.Lock()
	mrw := s.mrw
	s.mu.Unlock()

	// video configuration sent before Reader.Initialize.
	err := mrw.Write(&message.Video{
		ChunkStreamID:   message.VideoChunkStreamID,
		MessageStreamID: 0x1000000,
		Codec:           message.CodecH264,
		FrameType:       message.VideoFrameTypeKeyFrame,
		PacketType:      message.VideoPacketTypeConfig,
		AVCConfig:       generateAvcC(t, testCodecH264.SPS, testCodecH264.PPS),
	})
	require.NoError(t, err)

	// coded frames spanning the analyze period are sent before
	// Reader.Initialize; they are replayed afterwards by the rewind
	// mechanism.
	s.writeVideo(0)
	s.writeVideo(2 * time.Second)

	r := &gortmplib.Reader{Conn: c}
	err = r.Initialize()
	require.NoError(t, err)
	require.Len(t, r.Tracks(), 1)

	var decodedDTS []time.Duration

	r.OnDataH264(r.Tracks()[0], func(_ time.Duration, dts time.Duration, _ [][]byte) {
		decodedDTS = append(decodedDTS, dts)
	})

	// drain the replayed messages: the video configuration (which also
	// fires the H264 callback with DTS 0) plus the two coded frames.
	for range 3 {
		err = r.Read()
		require.NoError(t, err)
	}

	require.Equal(t, []time.Duration{0, 0, 2 * time.Second}, decodedDTS)

	// pause: no new media reaches the Reader while paused.
	err = c.Pause(context.Background())
	require.NoError(t, err)
	require.True(t, c.Paused())

	s.writeVideo(3 * time.Second)
	s.writeVideo(4 * time.Second)

	time.Sleep(100 * time.Millisecond)

	// resume: the track configuration set up before the pause is still
	// valid, and timestamps remain monotonic.
	err = c.Resume(context.Background())
	require.NoError(t, err)
	require.False(t, c.Paused())

	s.writeVideo(5 * time.Second)

	// only the post-resume 5s frame is delivered; the frames sent during
	// the pause were dropped.
	err = r.Read()
	require.NoError(t, err)

	require.Equal(t, []time.Duration{0, 0, 2 * time.Second, 5 * time.Second}, decodedDTS)
}

func mediaDTSOrZero(msg message.Message) time.Duration {
	switch msg := msg.(type) {
	case *message.Video:
		return msg.DTS

	case *message.Audio:
		return msg.DTS

	case *message.VideoExCodedFrames:
		return msg.DTS

	case *message.AudioExCodedFrames:
		return msg.DTS

	default:
		return 0
	}
}
