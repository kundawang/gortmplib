package rawmessage_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/liberrors"
	"github.com/bluenviron/gortmplib/pkg/rawmessage"
)

func TestWriterValidateChunkSize(t *testing.T) {
	for _, ca := range []struct {
		name  string
		value uint32
		valid bool
	}{
		{"zero", 0, false},
		{"one", 1, true},
		{"standard", 128, true},
		{"max", rawmessage.MaxChunkSize, true},
		{"above max", rawmessage.MaxChunkSize + 1, false},
	} {
		t.Run(ca.name, func(t *testing.T) {
			var buf bytes.Buffer
			bc := bytecounter.NewWriter(&buf)
			w := &rawmessage.Writer{BW: bc, BCW: bc}
			w.Initialize()

			err := w.SetChunkSize(ca.value)
			if !ca.valid {
				require.ErrorAs(t, err, &liberrors.ErrChunkSizeInvalid{})
				require.Equal(t, uint32(128), w.ChunkSize())
				require.Zero(t, buf.Len())
				return
			}

			require.NoError(t, err)
			require.Equal(t, ca.value, w.ChunkSize())
		})
	}
}

func TestWriterChunkSizeAffectsOnlyFollowingMessages(t *testing.T) {
	var buf bytes.Buffer
	bc := bytecounter.NewWriter(&buf)
	w := &rawmessage.Writer{BW: bc, BCW: bc}
	w.Initialize()

	write := func(bodyLen int) {
		err := w.Write(&rawmessage.Message{
			ChunkStreamID:   6,
			Timestamp:       0,
			Type:            9,
			MessageStreamID: 1,
			Body:            bytes.Repeat([]byte{0xaa}, bodyLen),
		})
		require.NoError(t, err)
	}

	// 128 is the default chunk size
	write(300)

	// a SetChunkSize control message, written with the previous chunk size
	err := w.Write(&rawmessage.Message{
		ChunkStreamID: 2,
		Type:          1,
		Body:          []byte{0x00, 0x00, 0x00, 0x40},
	})
	require.NoError(t, err)

	require.NoError(t, w.SetChunkSize(64))
	write(300)

	r := rawmessage.NewReader(bytecounter.NewReader(bytes.NewReader(buf.Bytes())),
		bytecounter.NewReader(bytes.NewReader(buf.Bytes())), nil)

	// the first message must be readable with the default size.
	msg, err := r.Read()
	require.NoError(t, err)
	require.Len(t, msg.Body, 300)

	// the control message is parsed with the previous size.
	msg, err = r.Read()
	require.NoError(t, err)
	require.Equal(t, byte(2), msg.ChunkStreamID)
	require.Equal(t, uint8(1), msg.Type)

	// the caller applies the new size to the reader.
	require.NoError(t, r.SetChunkSize(64))

	msg, err = r.Read()
	require.NoError(t, err)
	require.Len(t, msg.Body, 300)

	_, err = r.Read()
	require.ErrorIs(t, err, io.EOF)
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
			var buf bytes.Buffer
			w := &rawmessage.Writer{
				BW:  bytecounter.NewWriter(&buf),
				BCW: bytecounter.NewWriter(&buf),
			}
			w.Initialize()

			err := w.Write(&rawmessage.Message{
				ChunkStreamID: 2,
				Type:          1,
				Body: []byte{
					byte(ca.value >> 24), byte(ca.value >> 16), byte(ca.value >> 8), byte(ca.value),
				},
			})
			require.NoError(t, err)

			bc := bytecounter.NewReader(bytes.NewReader(buf.Bytes()))
			r := &rawmessage.Reader{BR: bc, BCR: bc}
			r.Initialize()

			msg, err := r.Read()
			require.NoError(t, err)
			require.Len(t, msg.Body, 4)

			// validation is performed by the message layer.
			require.ErrorAs(t, r.SetChunkSize(ca.value), &liberrors.ErrChunkSizeInvalid{})
			require.Equal(t, uint32(128), r.ChunkSize())
		})
	}
}

func TestReaderChunkSize(t *testing.T) {
	var buf bytes.Buffer
	w := &rawmessage.Writer{
		BW:  bytecounter.NewWriter(&buf),
		BCW: bytecounter.NewWriter(&buf),
	}
	w.Initialize()

	err := w.Write(&rawmessage.Message{
		ChunkStreamID: 6,
		Timestamp:     0,
		Type:          9,
		Body:          bytes.Repeat([]byte{1}, 10),
	})
	require.NoError(t, err)

	bc := bytecounter.NewReader(bytes.NewReader(buf.Bytes()))
	r := &rawmessage.Reader{BR: bc, BCR: bc}
	r.Initialize()
	require.Equal(t, uint32(128), r.ChunkSize())

	require.NoError(t, r.SetChunkSize(1000))
	require.Equal(t, uint32(1000), r.ChunkSize())

	msg, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{1}, 10), msg.Body)
	require.Equal(t, time.Duration(0), msg.Timestamp)
}
