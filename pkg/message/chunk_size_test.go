package message_test

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/liberrors"
	"github.com/bluenviron/gortmplib/pkg/message"
	"github.com/bluenviron/gortmplib/pkg/rawmessage"
)

func newWriterPair(t *testing.T) (*message.Writer, *message.Reader) {
	t.Helper()

	buf := &bytes.Buffer{}
	bcw := bytecounter.NewWriter(buf)
	w := &message.Writer{BW: bcw, BCW: bcw}
	w.Initialize()

	bcr := bytecounter.NewReader(buf)
	r := &message.Reader{BR: bcr, BCR: bcr}
	r.Initialize()

	return w, r
}

func TestWriterSetChunkSize(t *testing.T) {
	w, r := newWriterPair(t)

	require.Equal(t, uint32(128), w.ChunkSize())

	err := w.SetChunkSize(0)
	require.ErrorAs(t, err, &liberrors.ErrChunkSizeInvalid{})
	require.Equal(t, uint32(128), w.ChunkSize())

	err = w.SetChunkSize(rawmessage.MaxChunkSize + 1)
	require.ErrorAs(t, err, &liberrors.ErrChunkSizeInvalid{})
	require.Equal(t, uint32(128), w.ChunkSize())

	// setting the size already in use must not write anything.
	err = w.SetChunkSize(128)
	require.NoError(t, err)
	require.Equal(t, uint32(128), w.ChunkSize())

	err = w.SetChunkSize(4096)
	require.NoError(t, err)
	require.Equal(t, uint32(4096), w.ChunkSize())

	msg, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, &message.SetChunkSize{Value: 4096}, msg)
	require.Equal(t, uint32(4096), r.ChunkSize())

	_, err = r.Read()
	require.ErrorIs(t, err, io.EOF)
}

func TestWriterSetChunkSizeInvalidNotWritten(t *testing.T) {
	buf := &bytes.Buffer{}
	bcw := bytecounter.NewWriter(buf)
	w := &message.Writer{BW: bcw, BCW: bcw}
	w.Initialize()

	require.ErrorAs(t, w.SetChunkSize(0), &liberrors.ErrChunkSizeInvalid{})
	require.ErrorAs(t, w.SetChunkSize(rawmessage.MaxChunkSize+1), &liberrors.ErrChunkSizeInvalid{})
	require.Zero(t, buf.Len())
}

func TestWriterWriteInvalidChunkSize(t *testing.T) {
	buf := &bytes.Buffer{}
	bcw := bytecounter.NewWriter(buf)
	w := &message.Writer{BW: bcw, BCW: bcw}
	w.Initialize()

	err := w.Write(&message.SetChunkSize{Value: 0})
	require.ErrorAs(t, err, &liberrors.ErrChunkSizeInvalid{})
	require.Zero(t, buf.Len())
	require.Equal(t, uint32(128), w.ChunkSize())
}

func TestReaderChunkSizeInvalidFromPeer(t *testing.T) {
	for _, ca := range []struct {
		name  string
		value uint32
	}{
		{"zero", 0},
		{"above max", rawmessage.MaxChunkSize + 1},
	} {
		t.Run(ca.name, func(t *testing.T) {
			// craft the raw message directly, bypassing the validation
			// performed by message.Writer.
			buf := &bytes.Buffer{}
			bcw := bytecounter.NewWriter(buf)
			rmw := &rawmessage.Writer{BW: bcw, BCW: bcw}
			rmw.Initialize()

			v := ca.value
			err := rmw.Write(&rawmessage.Message{
				ChunkStreamID: message.ControlChunkStreamID,
				Type:          uint8(message.TypeSetChunkSize),
				Body:          []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)},
			})
			require.NoError(t, err)

			bcr := bytecounter.NewReader(bytes.NewReader(buf.Bytes()))
			r := &message.Reader{BR: bcr, BCR: bcr}
			r.Initialize()
			require.Equal(t, uint32(128), r.ChunkSize())

			_, err = r.Read()
			require.ErrorAs(t, err, &liberrors.ErrChunkSizePeerInvalid{})

			// the reader keeps the previous size.
			require.Equal(t, uint32(128), r.ChunkSize())
		})
	}
}

func TestWriterSetChunkSizePreservesMessageBoundaries(t *testing.T) {
	const bigBody = 5000

	w, r := newWriterPair(t)

	video := func(dts time.Duration, fill byte) *message.VideoExFramesX {
		return &message.VideoExFramesX{
			ChunkStreamID:   message.VideoChunkStreamID,
			MessageStreamID: 0x1000000,
			FrameType:       message.VideoFrameTypeInterFrame,
			FourCC:          message.FourCCAVC,
			DTS:             dts,
			Payload:         bytes.Repeat([]byte{fill}, bigBody),
		}
	}

	require.NoError(t, w.Write(video(0, 0x01)))
	require.NoError(t, w.SetChunkSize(1000))
	require.NoError(t, w.Write(video(33*time.Millisecond, 0x02)))
	require.NoError(t, w.SetChunkSize(65536))
	require.NoError(t, w.Write(video(66*time.Millisecond, 0x03)))

	// the first message is written before any chunk size change.
	msg, err := r.Read()
	require.NoError(t, err)
	fmsg, ok := msg.(*message.VideoExFramesX)
	require.True(t, ok)
	require.True(t, bytes.Equal(bytes.Repeat([]byte{0x01}, bigBody), fmsg.Payload))
	require.Equal(t, time.Duration(0), fmsg.DTS)

	for i, size := range []uint32{1000, 65536} {
		msg, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, &message.SetChunkSize{Value: size}, msg)
		require.Equal(t, size, r.ChunkSize())

		msg, err = r.Read()
		require.NoError(t, err)

		fmsg, ok := msg.(*message.VideoExFramesX)
		require.True(t, ok)
		require.True(t, bytes.Equal(bytes.Repeat([]byte{byte(i + 2)}, bigBody), fmsg.Payload))
		require.Equal(t, time.Duration(i+1)*33*time.Millisecond, fmsg.DTS)
		require.Equal(t, byte(message.VideoChunkStreamID), fmsg.ChunkStreamID)
		require.Equal(t, uint32(0x1000000), fmsg.MessageStreamID)
	}

	msg, err = r.Read()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, msg)
}

func TestWriterSetChunkSizePreservesFirstMessage(t *testing.T) {
	const bigBody = 5000

	w, r := newWriterPair(t)

	require.NoError(t, w.Write(&message.VideoExFramesX{
		ChunkStreamID:   message.VideoChunkStreamID,
		MessageStreamID: 0x1000000,
		FrameType:       message.VideoFrameTypeInterFrame,
		FourCC:          message.FourCCAVC,
		Payload:         bytes.Repeat([]byte{0x01}, bigBody),
	}))
	require.NoError(t, w.SetChunkSize(1000))

	msg, err := r.Read()
	require.NoError(t, err)
	fmsg, ok := msg.(*message.VideoExFramesX)
	require.True(t, ok)
	require.True(t, bytes.Equal(bytes.Repeat([]byte{0x01}, bigBody), fmsg.Payload))

	msg, err = r.Read()
	require.NoError(t, err)
	require.Equal(t, &message.SetChunkSize{Value: 1000}, msg)

	_, err = r.Read()
	require.ErrorIs(t, err, io.EOF)
}

func TestWriterSetChunkSizeConcurrent(t *testing.T) {
	const messages = 100

	var buf bytes.Buffer
	bcw := bytecounter.NewWriter(&buf)
	w := &message.Writer{BW: bcw, BCW: bcw}
	w.Initialize()

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 3)

	// video writer
	wg.Go(func() {
		<-start
		for i := range messages {
			err := w.Write(&message.Video{
				ChunkStreamID:   message.VideoChunkStreamID,
				MessageStreamID: 0x1000000,
				Codec:           message.CodecH264,
				FrameType:       message.VideoFrameTypeKeyFrame,
				PacketType:      message.VideoPacketTypeAU,
				AU:              bytes.Repeat([]byte{byte(i)}, 4096),
			})
			if err != nil {
				errs <- err
				return
			}
		}
	})

	// audio writer
	wg.Go(func() {
		<-start
		for i := range messages {
			err := w.Write(&message.Audio{
				ChunkStreamID:   message.AudioChunkStreamID,
				MessageStreamID: 0x1000000,
				Codec:           message.CodecMPEG4Audio,
				Rate:            message.AudioRate44100,
				Depth:           message.AudioDepth16,
				IsStereo:        true,
				AACType:         message.AudioAACTypeAU,
				AU:              bytes.Repeat([]byte{byte(i)}, 512),
			})
			if err != nil {
				errs <- err
				return
			}
		}
	})

	// chunk size changer
	wg.Go(func() {
		<-start
		for i := range messages {
			size := uint32(128)
			if i%2 != 0 {
				size = 4096
			}
			if err := w.SetChunkSize(size); err != nil {
				errs <- err
				return
			}
		}
	})

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	bcr := bytecounter.NewReader(bytes.NewReader(buf.Bytes()))
	r := &message.Reader{BR: bcr, BCR: bcr}
	r.Initialize()

	var videoCount, audioCount, chunkSizeCount int
	seenVideo := make(map[int]bool)
	seenAudio := make(map[int]bool)

	for {
		msg, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		switch msg := msg.(type) {
		case *message.SetChunkSize:
			chunkSizeCount++
			require.Equal(t, r.ChunkSize(), msg.Value)

		case *message.Video:
			videoCount++
			require.Len(t, msg.AU, 4096)
			i := int(msg.AU[0])
			require.False(t, seenVideo[i])
			seenVideo[i] = true
			require.Equal(t, bytes.Repeat([]byte{byte(i)}, 4096), msg.AU)
			require.Equal(t, uint32(0x1000000), msg.MessageStreamID)
			require.Equal(t, byte(message.VideoChunkStreamID), msg.ChunkStreamID)

		case *message.Audio:
			audioCount++
			require.Len(t, msg.AU, 512)
			i := int(msg.AU[0])
			require.False(t, seenAudio[i])
			seenAudio[i] = true
			require.Equal(t, bytes.Repeat([]byte{byte(i)}, 512), msg.AU)
			require.Equal(t, byte(message.AudioChunkStreamID), msg.ChunkStreamID)
		}
	}

	require.Equal(t, messages, videoCount)
	require.Equal(t, messages, audioCount)
	// changes that request the size already in use are no-ops.
	require.LessOrEqual(t, chunkSizeCount, messages)
	require.Greater(t, chunkSizeCount, 0)
}
