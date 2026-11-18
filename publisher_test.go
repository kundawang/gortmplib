package gortmplib_test

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/amf0"
	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/gortmplib/pkg/handshake"
	"github.com/bluenviron/gortmplib/pkg/message"
)

var (
	testKeyFrameAU = [][]byte{{0x65, 0x88, 0x84, 0x21}}
	testInterAU    = [][]byte{{0x41, 0x9a, 0x22}}
	testAACAU      = []byte{0x01, 0x02, 0x03}
)

func testPublisherTracks() (*gortmplib.Track, *gortmplib.Track) {
	vtrack := &gortmplib.Track{Codec: &codecs.H264{}}
	atrack := &gortmplib.Track{Codec: &codecs.MPEG4Audio{
		Config: &mpeg4audio.AudioSpecificConfig{
			Type:          2,
			SampleRate:    44100,
			ChannelConfig: 2,
			ChannelCount:  2, //nolint:staticcheck
		},
	}}
	return vtrack, atrack
}

// serverPublishHandshake performs the server side of a publish handshake,
// checking the order of incoming commands.
func serverPublishHandshake(t *testing.T, conn net.Conn) *message.ReadWriter {
	t.Helper()

	bc := bytecounter.NewReadWriter(conn)

	_, _, err := handshake.DoServer(bc, false)
	require.NoError(t, err)

	mrw := message.NewReadWriter(bc, bc, true)

	for {
		msg, err2 := mrw.Read()
		require.NoError(t, err2)

		if cmd, ok := msg.(*message.CommandAMF0); ok && cmd.Name == "connect" {
			break
		}
	}

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
	require.NoError(t, err)

	for _, name := range []string{"releaseStream", "FCPublish", "createStream"} {
		msg, err2 := mrw.Read()
		require.NoError(t, err2)
		require.Equal(t, name, msg.(*message.CommandAMF0).Name)
	}

	err = mrw.Write(&message.CommandAMF0{
		ChunkStreamID: 3,
		Name:          "_result",
		CommandID:     4,
		Arguments:     []any{nil, float64(1)},
	})
	require.NoError(t, err)

	msg, err := mrw.Read()
	require.NoError(t, err)
	require.Equal(t, "publish", msg.(*message.CommandAMF0).Name)

	err = mrw.Write(&message.CommandAMF0{
		ChunkStreamID:   5,
		MessageStreamID: 0x1000000,
		Name:            "onStatus",
		CommandID:       5,
		Arguments: []any{
			nil,
			amf0.Object{
				{Key: "level", Value: "status"},
				{Key: "code", Value: "NetStream.Publish.Start"},
				{Key: "description", Value: "publish start"},
			},
		},
	})
	require.NoError(t, err)

	return mrw
}

// serverReadSetup reads and checks metadata and codec configurations.
func serverReadSetup(t *testing.T, mrw *message.ReadWriter) {
	t.Helper()

	msg, err := mrw.Read()
	require.NoError(t, err)
	require.IsType(t, &message.DataAMF0{}, msg)

	msg, err = mrw.Read()
	require.NoError(t, err)
	vmsg := msg.(*message.Video)
	require.Equal(t, message.VideoPacketTypeConfig, vmsg.PacketType)

	msg, err = mrw.Read()
	require.NoError(t, err)
	amsg := msg.(*message.Audio)
	require.Equal(t, message.AudioAACTypeConfig, amsg.AACType)
}

func TestPublisherReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:9123")
	require.NoError(t, err)
	defer ln.Close()

	conn1Closed := make(chan struct{})
	verified := make(chan struct{})
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		// first connection
		conn, err2 := ln.Accept()
		require.NoError(t, err2)

		mrw := serverPublishHandshake(t, conn)
		serverReadSetup(t, mrw)

		// read initial media messages, then drop the connection
		var lastTS time.Duration
		for range 5 {
			msg, err3 := mrw.Read()
			require.NoError(t, err3)

			switch m := msg.(type) {
			case *message.Video:
				require.Equal(t, message.VideoPacketTypeAU, m.PacketType)
				if m.DTS > lastTS {
					lastTS = m.DTS
				}
			case *message.Audio:
				if m.DTS > lastTS {
					lastTS = m.DTS
				}
			}
		}

		conn.Close()
		close(conn1Closed)

		// second connection: the client must re-handshake and re-publish
		conn, err2 = ln.Accept()
		require.NoError(t, err2)
		defer conn.Close()

		mrw = serverPublishHandshake(t, conn)

		// metadata and codec configurations must be sent again
		serverReadSetup(t, mrw)

		// the first video frame after the reconnection must be a key frame,
		// and timestamps must be monotonically increasing
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		gotKeyFrame := false
		mediaCount := 0
		for mediaCount < 4 {
			msg, err3 := mrw.Read()
			require.NoError(t, err3)

			switch m := msg.(type) {
			case *message.Video:
				if m.PacketType != message.VideoPacketTypeAU {
					continue
				}
				if !gotKeyFrame {
					require.Equal(t, message.VideoFrameTypeKeyFrame, m.FrameType)
					gotKeyFrame = true
				}
				require.GreaterOrEqual(t, m.DTS, lastTS)
				lastTS = m.DTS
				mediaCount++

			case *message.Audio:
				require.GreaterOrEqual(t, m.DTS, lastTS)
				lastTS = m.DTS
				mediaCount++
			}
		}

		require.True(t, gotKeyFrame)
		close(verified)

		// wait for the client to close
		for {
			_, err3 := mrw.Read()
			if err3 != nil {
				break
			}
		}
	}()

	vtrack, atrack := testPublisherTracks()

	u, err := url.Parse("rtmp://127.0.0.1:9123/stream")
	require.NoError(t, err)

	p := &gortmplib.Publisher{
		URL:    u,
		Tracks: []*gortmplib.Track{vtrack, atrack},
		Reconnect: &gortmplib.ReconnectConfig{
			MaxRetries:      5,
			InitialBackoff:  10 * time.Millisecond,
			MaxBackoff:      50 * time.Millisecond,
			QueueSize:       32,
			KeyFrameTimeout: 2 * time.Second,
		},
	}
	err = p.Initialize(context.Background())
	require.NoError(t, err)

	w := p.Writer()

	err = w.WriteH264(vtrack, 0, 0, testKeyFrameAU)
	require.NoError(t, err)
	err = w.WriteMPEG4Audio(atrack, 0, testAACAU)
	require.NoError(t, err)
	err = w.WriteH264(vtrack, 40*time.Millisecond, 40*time.Millisecond, testInterAU)
	require.NoError(t, err)
	err = w.WriteH264(vtrack, 80*time.Millisecond, 80*time.Millisecond, testInterAU)
	require.NoError(t, err)
	err = w.WriteMPEG4Audio(atrack, 80*time.Millisecond, testAACAU)
	require.NoError(t, err)

	<-conn1Closed
	time.Sleep(50 * time.Millisecond)

	// inter frames sent before the next key frame must be dropped
	err = w.WriteH264(vtrack, 120*time.Millisecond, 120*time.Millisecond, testInterAU)
	require.NoError(t, err)
	err = w.WriteH264(vtrack, 160*time.Millisecond, 160*time.Millisecond, testInterAU)
	require.NoError(t, err)

	// video resumes from this key frame
	err = w.WriteH264(vtrack, 200*time.Millisecond, 200*time.Millisecond, testKeyFrameAU)
	require.NoError(t, err)
	err = w.WriteMPEG4Audio(atrack, 200*time.Millisecond, testAACAU)
	require.NoError(t, err)
	err = w.WriteH264(vtrack, 240*time.Millisecond, 240*time.Millisecond, testInterAU)
	require.NoError(t, err)
	err = w.WriteMPEG4Audio(atrack, 240*time.Millisecond, testAACAU)
	require.NoError(t, err)
	err = w.WriteH264(vtrack, 280*time.Millisecond, 280*time.Millisecond, testKeyFrameAU)
	require.NoError(t, err)
	err = w.WriteMPEG4Audio(atrack, 280*time.Millisecond, testAACAU)
	require.NoError(t, err)

	<-verified

	p.Close()
	require.NoError(t, p.Wait())

	<-serverDone
}

func TestPublisherReconnectExhausted(t *testing.T) {
	t.Run("connection refused", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:9124")
		require.NoError(t, err)
		defer ln.Close()

		serverDone := make(chan struct{})

		go func() {
			defer close(serverDone)

			conn, err2 := ln.Accept()
			require.NoError(t, err2)

			mrw := serverPublishHandshake(t, conn)
			serverReadSetup(t, mrw)

			conn.Close()
			ln.Close()
		}()

		vtrack, atrack := testPublisherTracks()

		u, err := url.Parse("rtmp://127.0.0.1:9124/stream")
		require.NoError(t, err)

		p := &gortmplib.Publisher{
			URL:    u,
			Tracks: []*gortmplib.Track{vtrack, atrack},
			Reconnect: &gortmplib.ReconnectConfig{
				MaxRetries:     2,
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     20 * time.Millisecond,
			},
		}
		err = p.Initialize(context.Background())
		require.NoError(t, err)
		defer p.Close()

		err = p.Wait()
		require.ErrorIs(t, err, gortmplib.ErrReconnectsExhausted)

		<-serverDone
	})

	t.Run("publish rejected", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:9125")
		require.NoError(t, err)
		defer ln.Close()

		serverDone := make(chan struct{})

		go func() {
			defer close(serverDone)

			conn, err2 := ln.Accept()
			require.NoError(t, err2)

			mrw := serverPublishHandshake(t, conn)
			serverReadSetup(t, mrw)

			conn.Close()

			// reject publish on every reconnection attempt
			for range 2 {
				conn, err2 = ln.Accept()
				require.NoError(t, err2)

				bc := bytecounter.NewReadWriter(conn)

				_, _, err3 := handshake.DoServer(bc, false)
				require.NoError(t, err3)

				mrw = message.NewReadWriter(bc, bc, true)

				for {
					msg, err4 := mrw.Read()
					require.NoError(t, err4)

					if cmd, ok := msg.(*message.CommandAMF0); ok && cmd.Name == "connect" {
						break
					}
				}

				err3 = mrw.Write(&message.CommandAMF0{
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
				require.NoError(t, err3)

				for {
					msg, err4 := mrw.Read()
					require.NoError(t, err4)

					if cmd, ok := msg.(*message.CommandAMF0); ok && cmd.Name == "createStream" {
						break
					}
				}

				err3 = mrw.Write(&message.CommandAMF0{
					ChunkStreamID: 3,
					Name:          "_result",
					CommandID:     4,
					Arguments:     []any{nil, float64(1)},
				})
				require.NoError(t, err3)

				for {
					msg, err4 := mrw.Read()
					require.NoError(t, err4)

					if cmd, ok := msg.(*message.CommandAMF0); ok && cmd.Name == "publish" {
						break
					}
				}

				err3 = mrw.Write(&message.CommandAMF0{
					ChunkStreamID:   5,
					MessageStreamID: 0x1000000,
					Name:            "onStatus",
					CommandID:       5,
					Arguments: []any{
						nil,
						amf0.Object{
							{Key: "level", Value: "error"},
							{Key: "code", Value: "NetStream.Publish.Denied"},
							{Key: "description", Value: "publish denied"},
						},
					},
				})
				require.NoError(t, err3)

				conn.Close()
			}
		}()

		vtrack, atrack := testPublisherTracks()

		u, err := url.Parse("rtmp://127.0.0.1:9125/stream")
		require.NoError(t, err)

		p := &gortmplib.Publisher{
			URL:    u,
			Tracks: []*gortmplib.Track{vtrack, atrack},
			Reconnect: &gortmplib.ReconnectConfig{
				MaxRetries:     2,
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     20 * time.Millisecond,
			},
		}
		err = p.Initialize(context.Background())
		require.NoError(t, err)
		defer p.Close()

		err = p.Wait()
		require.ErrorIs(t, err, gortmplib.ErrReconnectsExhausted)

		<-serverDone
	})
}

func TestPublisherQueueFull(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:9126")
	require.NoError(t, err)
	defer ln.Close()

	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		conn, err2 := ln.Accept()
		require.NoError(t, err2)

		mrw := serverPublishHandshake(t, conn)
		serverReadSetup(t, mrw)

		// simulate a network outage
		conn.Close()
		ln.Close()
	}()

	vtrack, atrack := testPublisherTracks()

	u, err := url.Parse("rtmp://127.0.0.1:9126/stream")
	require.NoError(t, err)

	p := &gortmplib.Publisher{
		URL:    u,
		Tracks: []*gortmplib.Track{vtrack, atrack},
		Reconnect: &gortmplib.ReconnectConfig{
			MaxRetries:     0, // unlimited
			InitialBackoff: 500 * time.Millisecond,
			MaxBackoff:     500 * time.Millisecond,
			QueueSize:      8,
		},
	}
	err = p.Initialize(context.Background())
	require.NoError(t, err)

	w := p.Writer()

	// the queue is bounded: once it fills up, writes fail explicitly
	var fullErr error
	for i := range 1000 {
		err = w.WriteH264(vtrack, time.Duration(i)*40*time.Millisecond,
			time.Duration(i)*40*time.Millisecond, testKeyFrameAU)
		if err != nil {
			fullErr = err
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.ErrorIs(t, fullErr, gortmplib.ErrPublisherQueueFull)

	// closing must stop reconnect attempts immediately
	p.Close()
	require.NoError(t, p.Wait())

	<-serverDone
}

func TestPublisherNoKeyFrameAfterReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:9127")
	require.NoError(t, err)
	defer ln.Close()

	conn1Closed := make(chan struct{})
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		// first connection
		conn, err2 := ln.Accept()
		require.NoError(t, err2)

		mrw := serverPublishHandshake(t, conn)
		serverReadSetup(t, mrw)

		msg, err3 := mrw.Read()
		require.NoError(t, err3)
		require.IsType(t, &message.Video{}, msg)

		conn.Close()
		close(conn1Closed)

		// second connection
		conn, err2 = ln.Accept()
		require.NoError(t, err2)
		defer conn.Close()

		mrw = serverPublishHandshake(t, conn)
		serverReadSetup(t, mrw)

		// no video must arrive, since no key frame is sent
		for {
			_, err3 = mrw.Read()
			if err3 != nil {
				break
			}
		}
	}()

	vtrack, atrack := testPublisherTracks()

	u, err := url.Parse("rtmp://127.0.0.1:9127/stream")
	require.NoError(t, err)

	p := &gortmplib.Publisher{
		URL:    u,
		Tracks: []*gortmplib.Track{vtrack, atrack},
		Reconnect: &gortmplib.ReconnectConfig{
			MaxRetries:      3,
			InitialBackoff:  10 * time.Millisecond,
			MaxBackoff:      20 * time.Millisecond,
			QueueSize:       32,
			KeyFrameTimeout: 200 * time.Millisecond,
		},
	}
	err = p.Initialize(context.Background())
	require.NoError(t, err)

	w := p.Writer()

	err = w.WriteH264(vtrack, 0, 0, testKeyFrameAU)
	require.NoError(t, err)

	<-conn1Closed
	time.Sleep(50 * time.Millisecond)

	// only send inter frames: video cannot resume without a key frame
	for i := 1; i <= 5; i++ {
		err = w.WriteH264(vtrack, time.Duration(i)*40*time.Millisecond,
			time.Duration(i)*40*time.Millisecond, testInterAU)
		require.NoError(t, err)
	}

	err = p.Wait()
	require.ErrorIs(t, err, gortmplib.ErrNoKeyFrameAfterReconnect)

	p.Close()

	<-serverDone
}

func TestPublisherNoReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:9231")
	require.NoError(t, err)
	defer ln.Close()

	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		conn, err2 := ln.Accept()
		require.NoError(t, err2)

		mrw := serverPublishHandshake(t, conn)
		serverReadSetup(t, mrw)

		conn.Close()
	}()

	vtrack, atrack := testPublisherTracks()

	u, err := url.Parse("rtmp://127.0.0.1:9231/stream")
	require.NoError(t, err)

	// without Reconnect, a connection error terminates the publisher
	p := &gortmplib.Publisher{
		URL:    u,
		Tracks: []*gortmplib.Track{vtrack, atrack},
	}
	err = p.Initialize(context.Background())
	require.NoError(t, err)
	defer p.Close()

	err = p.Wait()
	require.Error(t, err)

	<-serverDone
}
