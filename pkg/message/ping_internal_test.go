package message

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPingTrackerIDWrapAround(t *testing.T) {
	var p pingTracker
	p.initialize()

	// place the counter right before the 32-bit wrap-around.
	p.nextID = math.MaxUint32 - 1

	issued := make([]uint32, 0, 200)

	for range 200 {
		id := p.allocateIDLocked()

		// zero must never be used, so that an unset Server Time cannot match.
		require.NotZero(t, id)

		issued = append(issued, id)
	}

	// the counter wrapped around at least once.
	require.Less(t, p.nextID, uint32(1000))

	// the first three identifiers are MaxUint32, 1 and 2 (0 is skipped).
	require.Equal(t, uint32(math.MaxUint32), issued[0])
	require.Equal(t, uint32(1), issued[1])
	require.Equal(t, uint32(2), issued[2])

	// no identifier is repeated within the recent-identifier window.
	for i := recentPingIDCount; i < len(issued); i++ {
		for j := i - recentPingIDCount + 1; j < i; j++ {
			require.NotEqual(t, issued[i], issued[j])
		}
	}
}

func TestPingTrackerResponseMatching(t *testing.T) {
	var p pingTracker
	p.initialize()

	req, err := p.begin()
	require.NoError(t, err)

	// a response with a different identifier does not complete the request,
	// and does not belong to this tracker.
	require.False(t, p.handleResponse(req.id+1))
	require.False(t, p.isOwnResponse(req.id+1))
	require.True(t, p.hasRequest())

	// duplicated begin is rejected.
	_, err = p.begin()
	require.Error(t, err)

	// the matching response completes the request.
	require.True(t, p.handleResponse(req.id))
	require.True(t, p.isOwnResponse(req.id))
	require.False(t, p.hasRequest())

	// a duplicated response is recognized as ours but no longer matches.
	require.False(t, p.handleResponse(req.id))
	require.True(t, p.isOwnResponse(req.id))

	<-req.done
	require.NoError(t, req.err)
}
