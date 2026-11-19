package rawmessage

import (
	"github.com/bluenviron/gortmplib/pkg/liberrors"
)

// MaxChunkSize is the maximum chunk size allowed by the RTMP specification.
// The most significant bit of the chunk size field must be zero.
const MaxChunkSize = 0x7FFFFFFF

// ValidateChunkSize checks whether v is a valid RTMP chunk size.
// Chunk size zero is not allowed, since it would prevent any chunk from
// carrying data.
func ValidateChunkSize(v uint32) error {
	if v == 0 || v > MaxChunkSize {
		return liberrors.ErrChunkSizeInvalid{Value: v}
	}

	return nil
}
