/*
Package gortmplib is a RTMP library for the Go programming language.

Examples are available at https://github.com/bluenviron/gortmplib/tree/main/examples
*/
package gortmplib

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/bluenviron/gortmplib/pkg/amf0"
	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/handshake"
	"github.com/bluenviron/gortmplib/pkg/liberrors"
	"github.com/bluenviron/gortmplib/pkg/message"
)

const (
	schemeRTMP  = "rtmp"
	schemeRTMPS = "rtmps"

	defaultRTMPPort  = "1935"
	defaultRTMPSPort = "443"
)

const (
	encodingAMF0 = 0
)

// RTMP 1.0 spec, section 7.2.1.1
const (
	supportSndNone  = 0x0001
	supportSndMP3   = 0x0004
	supportSndG711A = 0x0080
	supportSndG711U = 0x0100
	supportSndAAV   = 0x0400
	supportVidH264  = 0x0080
)

var errAuth = errors.New("auth")

func fourCCToString(c message.FourCC) string {
	return string([]byte{byte(c >> 24), byte(c >> 16), byte(c >> 8), byte(c)})
}

func resultIsOK1(res *message.CommandAMF0) bool {
	if len(res.Arguments) < 2 {
		return false
	}

	ma, ok := objectOrArray(res.Arguments[1])
	if !ok {
		return false
	}

	v, ok := ma.Get("level")
	if !ok {
		return false
	}

	return (v == "status")
}

func resultIsOK2(res *message.CommandAMF0) bool {
	if len(res.Arguments) < 2 {
		return false
	}

	v, ok := res.Arguments[1].(float64)
	if !ok {
		return false
	}

	return v == 1
}

func splitURL(u *url.URL) (string, string, string) {
	nu := *u

	// move fragment inside streamKey
	var streamKey string
	streamKey, nu.Fragment = nu.Fragment, ""

	tcURL := nu.String()
	app := strings.TrimPrefix(nu.RequestURI(), "/")

	return tcURL, app, streamKey
}

func readCommand(mrw *message.ReadWriter) (*message.CommandAMF0, error) {
	for {
		msg, err := mrw.Read()
		if err != nil {
			return nil, err
		}

		if cmd, ok := msg.(*message.CommandAMF0); ok {
			return cmd, nil
		}
	}
}

func readCommandResult(
	mrw *message.ReadWriter,
	commandID int,
) (*message.CommandAMF0, error) {
	for {
		msg, err := mrw.Read()
		if err != nil {
			return nil, err
		}

		if cmd, ok := msg.(*message.CommandAMF0); ok {
			if cmd.CommandID == commandID || (cmd.CommandID == 0) &&
				(cmd.Name == "_result" || cmd.Name == "_error") {
				return cmd, nil
			}
		}
	}
}

func waitOnStatus(
	mrw *message.ReadWriter,
	commandID int,
) (*message.CommandAMF0, error) {
	for {
		msg, err := mrw.Read()
		if err != nil {
			return nil, err
		}

		if cmd, ok := msg.(*message.CommandAMF0); ok {
			if cmd.CommandID == commandID || (cmd.CommandID == 0 &&
				cmd.Name == "onStatus") {
				return cmd, nil
			}
		}
	}
}

// Client is a client-side RTMP connection.
type Client struct {
	//
	// Target
	//
	// URL of the RTMP server to connect to.
	// Format is rtmp://user:pass@host:port/path#streamKey
	URL *url.URL
	// Whether to publish or play.
	Publish bool

	//
	// RTMP parameters (all optional)
	//
	// a TLS configuration to connect to RTMPS servers.
	// It defaults to nil.
	TLSConfig *tls.Config

	//
	// system functions (all optional)
	//
	// function used to initialize the TCP client.
	// It defaults to (&net.Dialer{}).DialContext.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)
	// function used to initialize a TLS connection.
	// When nil, DialContext and tls.Client are used in its place.
	// It defaults to nil.
	DialTLSContext func(ctx context.Context, network string, addr string) (net.Conn, error)

	nconn         net.Conn
	bc            *bytecounter.ReadWriter
	mrw           *message.ReadWriter
	authState     int
	authSalt      string
	authChallenge string

	// play-mode read pump (nil in publish mode or before Initialize)
	readDone    chan struct{}
	done        chan struct{}
	messages    chan message.Message
	pumpWake    chan struct{}
	readErr     error
	cmdMu       sync.Mutex
	cmdPending  map[int]chan *message.CommandAMF0
	nextCmdID   int
	pauseMu     sync.Mutex
	paused      bool
	pausePoint  time.Duration
	lastMediaTS atomic.Int64
	closed      bool
}

// clientMessageQueueSize is the size of the queue holding messages decoded by
// the internal read pump and waiting to be returned by Read().
const clientMessageQueueSize = 256

// Initialize initializes Client.
func (c *Client) Initialize(ctx context.Context) error {
	if c.DialContext == nil {
		c.DialContext = (&net.Dialer{}).DialContext
	}

	switch c.URL.Scheme {
	case schemeRTMP, schemeRTMPS:
	default:
		return fmt.Errorf("unsupported scheme: %s", c.URL.Scheme)
	}

	for {
		err := c.initialize2(ctx)
		if errors.Is(err, errAuth) {
			c.authState++
			continue
		}
		return err
	}
}

func (c *Client) initialize2(ctx context.Context) error {
	address := c.URL.Host
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		if strings.Contains(err.Error(), "missing port in address") {
			switch c.URL.Scheme {
			case schemeRTMP:
				address = net.JoinHostPort(c.URL.Host, defaultRTMPPort)
				host = c.URL.Host
			default: // RTMPS
				address = net.JoinHostPort(c.URL.Host, defaultRTMPSPort)
				host = c.URL.Host
			}
		} else {
			return err
		}
	}

	if c.DialTLSContext != nil {
		c.nconn, err = c.DialTLSContext(ctx, "tcp", address)
		if err != nil {
			return err
		}
	} else {
		c.nconn, err = c.DialContext(ctx, "tcp", address)
		if err != nil {
			return err
		}

		if c.URL.Scheme == schemeRTMPS {
			// clone TLS config and fill ServerName if empty.
			// this is the same behavior of http.Client.
			// https://cs.opensource.google/go/go/+/master:src/net/http/transport.go;l=1754;drc=a4b534f5e42fe58d58c0ff0562d76680cedb0466

			tlsConfig := c.TLSConfig

			if tlsConfig == nil {
				tlsConfig = &tls.Config{}
			} else {
				tlsConfig = tlsConfig.Clone()
			}

			if tlsConfig.ServerName == "" {
				tlsConfig.ServerName = host
			}

			c.nconn = tls.Client(c.nconn, tlsConfig)
		}
	}

	closerDone := make(chan struct{})
	defer func() { <-closerDone }()

	closerTerminate := make(chan struct{})
	defer close(closerTerminate)

	nc := c.nconn
	go func() {
		defer close(closerDone)

		select {
		case <-closerTerminate:
		case <-ctx.Done():
			nc.Close()
		}
	}()

	err = c.initialize3()
	if err != nil {
		c.nconn.Close()
		return err
	}

	return nil
}

func (c *Client) initialize3() error {
	c.bc = bytecounter.NewReadWriter(c.nconn)

	_, _, err := handshake.DoClient(c.bc, false, false)
	if err != nil {
		return err
	}

	c.mrw = message.NewReadWriter(c.bc, c.bc, false)

	err = c.mrw.Write(&message.SetWindowAckSize{
		Value: 2500000,
	})
	if err != nil {
		return err
	}

	err = c.mrw.Write(&message.SetPeerBandwidth{
		Value: 2500000,
		Type:  2,
	})
	if err != nil {
		return err
	}

	err = c.mrw.Write(&message.SetChunkSize{
		Value: 65536,
	})
	if err != nil {
		return err
	}

	cleanURL := &url.URL{
		Scheme:      c.URL.Scheme,
		Opaque:      c.URL.Opaque,
		Host:        c.URL.Host,
		Path:        c.URL.Path,
		RawPath:     c.URL.RawPath,
		OmitHost:    c.URL.OmitHost,
		ForceQuery:  c.URL.ForceQuery,
		RawQuery:    c.URL.RawQuery,
		Fragment:    c.URL.Fragment,
		RawFragment: c.URL.RawFragment,
	}

	tcURL, app, streamKey := splitURL(cleanURL)

	switch c.authState {
	case 1:
		user := c.URL.User.Username()

		app += "?authmod=adobe&user=" + user
		tcURL += "?authmod=adobe&user=" + user

	case 2:
		user := c.URL.User.Username()
		pass, _ := c.URL.User.Password()

		clientChallenge := strings.ReplaceAll(uuid.New().String(), "-", "")
		response := authResponse(user, pass, c.authSalt, "", c.authChallenge, clientChallenge)

		app += fmt.Sprintf("?authmod=adobe&user=myuser&challenge=%s&response=%s", clientChallenge, response)
		tcURL += fmt.Sprintf("?authmod=adobe&user=myuser&challenge=%s&response=%s", clientChallenge, response)
	}

	connectArg := amf0.Object{
		{Key: "app", Value: app},
		{Key: "flashVer", Value: "LNX 9,0,124,2"},
		{Key: "tcUrl", Value: tcURL},
		{Key: "objectEncoding", Value: float64(encodingAMF0)},
	}

	if !c.Publish {
		connectArg = append(connectArg,
			amf0.ObjectEntry{
				Key:   "fpad",
				Value: false,
			},
			amf0.ObjectEntry{
				Key:   "capabilities",
				Value: float64(15),
			},
			amf0.ObjectEntry{
				Key: "audioCodecs",
				Value: float64(
					supportSndNone | supportSndMP3 | supportSndG711A | supportSndG711U | supportSndAAV),
			},
			amf0.ObjectEntry{
				Key:   "videoCodecs",
				Value: float64(supportVidH264),
			},
			amf0.ObjectEntry{
				Key:   "videoFunction",
				Value: float64(0),
			},
			amf0.ObjectEntry{
				Key: "fourCcList",
				Value: amf0.StrictArray{
					fourCCToString(message.FourCCAV1),
					fourCCToString(message.FourCCVP9),
					fourCCToString(message.FourCCHEVC),
					fourCCToString(message.FourCCAVC),
					fourCCToString(message.FourCCOpus),
					fourCCToString(message.FourCCFLAC),
					fourCCToString(message.FourCCAC3),
					fourCCToString(message.FourCCMP4A),
					fourCCToString(message.FourCCMP3),
				},
			},
		)
	}

	err = c.mrw.Write(&message.CommandAMF0{
		ChunkStreamID: 3,
		Name:          "connect",
		CommandID:     1,
		Arguments:     []any{connectArg},
	})
	if err != nil {
		return err
	}

	res, err := readCommandResult(c.mrw, 1)
	if err != nil {
		return err
	}

	switch res.Name {
	case "_result":

	case "_error":
		if len(res.Arguments) < 2 {
			return fmt.Errorf("bad result: %v", res)
		}

		ma, ok := objectOrArray(res.Arguments[1])
		if !ok {
			return fmt.Errorf("bad result: %v", res)
		}

		desc, ok := ma.GetString("description")
		if !ok {
			return fmt.Errorf("bad result: %v", res)
		}

		if desc == "code=403 need auth; authmod=adobe" {
			if c.URL.User == nil {
				return fmt.Errorf("credentials are required")
			}

			if c.authState != 0 {
				return fmt.Errorf("authentication error")
			}

			return errAuth
		}

		if !strings.HasPrefix(desc, "authmod=adobe ?") {
			return fmt.Errorf("bad result: %v", res)
		}

		desc = desc[len("authmod=adobe ?"):]
		vals := queryDecode(desc)

		reason := vals["reason"]
		c.authSalt = vals["salt"]
		c.authChallenge = vals["challenge"]

		if reason != "needauth" || c.authSalt == "" || c.authChallenge == "" {
			return fmt.Errorf("bad result: %v", res)
		}

		if c.authState != 1 {
			return fmt.Errorf("authentication error")
		}

		return errAuth

	default:
		return fmt.Errorf("bad result: %v", res)
	}

	if !c.Publish {
		err = c.mrw.Write(&message.CommandAMF0{
			ChunkStreamID: 3,
			Name:          "createStream",
			CommandID:     2,
			Arguments: []any{
				nil,
			},
		})
		if err != nil {
			return err
		}

		res, err = readCommandResult(c.mrw, 2)
		if err != nil {
			return err
		}

		if res.Name != "_result" || !resultIsOK2(res) {
			return fmt.Errorf("bad result: %v", res)
		}

		err = c.mrw.Write(&message.UserControlSetBufferLength{
			BufferLength: 0x64,
		})
		if err != nil {
			return err
		}

		err = c.mrw.Write(&message.CommandAMF0{
			ChunkStreamID:   4,
			MessageStreamID: 0x1000000,
			Name:            "play",
			CommandID:       3,
			Arguments: []any{
				nil,
				streamKey,
			},
		})
		if err != nil {
			return err
		}

		res, err = waitOnStatus(c.mrw, 3)
		if err != nil {
			return err
		}

		if res.Name != "onStatus" || !resultIsOK1(res) {
			return fmt.Errorf("bad result: %v", res)
		}

		c.startReadPump()
	} else {
		err = c.mrw.Write(&message.CommandAMF0{
			ChunkStreamID: 3,
			Name:          "releaseStream",
			CommandID:     2,
			Arguments: []any{
				nil,
				streamKey,
			},
		})
		if err != nil {
			return err
		}

		err = c.mrw.Write(&message.CommandAMF0{
			ChunkStreamID: 3,
			Name:          "FCPublish",
			CommandID:     3,
			Arguments: []any{
				nil,
				streamKey,
			},
		})
		if err != nil {
			return err
		}

		err = c.mrw.Write(&message.CommandAMF0{
			ChunkStreamID: 3,
			Name:          "createStream",
			CommandID:     4,
			Arguments: []any{
				nil,
			},
		})
		if err != nil {
			return err
		}

		res, err = readCommandResult(c.mrw, 4)
		if err != nil {
			return err
		}

		if res.Name != "_result" || !resultIsOK2(res) {
			return fmt.Errorf("bad result: %v", res)
		}

		err = c.mrw.Write(&message.CommandAMF0{
			ChunkStreamID:   4,
			MessageStreamID: 0x1000000,
			Name:            "publish",
			CommandID:       5,
			Arguments: []any{
				nil,
				streamKey,
				"live",
			},
		})
		if err != nil {
			return err
		}

		res, err = waitOnStatus(c.mrw, 5)
		if err != nil {
			return err
		}

		if res.Name != "onStatus" || !resultIsOK1(res) {
			return fmt.Errorf("bad result: %v", res)
		}
	}

	return nil
}

// Close closes the connection.
func (c *Client) Close() {
	c.pauseMu.Lock()
	if c.closed {
		c.pauseMu.Unlock()
		return
	}
	c.closed = true
	c.pauseMu.Unlock()

	if c.done != nil {
		close(c.done)
	}

	c.nconn.Close()

	if c.readDone != nil {
		<-c.readDone
	}
}

// NetConn returns the underlying net.Conn.
func (c *Client) NetConn() net.Conn {
	return c.nconn
}

// BytesReceived returns the number of bytes received.
func (c *Client) BytesReceived() uint64 {
	return c.bc.Reader.Count()
}

// BytesSent returns the number of bytes sent.
func (c *Client) BytesSent() uint64 {
	return c.bc.Writer.Count()
}

// Read reads a message.
// In play mode, messages are delivered through an internal read pump:
// while the client is paused no media message is delivered, and after
// resuming only messages with non-decreasing timestamps are forwarded.
// It is safe to call Read from multiple goroutines; each message is
// delivered to exactly one caller.
func (c *Client) Read() (message.Message, error) {
	if c.messages == nil {
		return c.mrw.Read()
	}

	for {
		msg, ok := <-c.messages
		if !ok {
			return nil, c.readErr
		}

		// messages may have been queued before a pause took effect.
		// Drop media while paused, still advancing the timestamp so that
		// post-resume ordering stays correct.
		if ts, isMedia := mediaMessageDTS(msg); isMedia {
			c.pauseMu.Lock()
			paused := c.paused
			c.pauseMu.Unlock()

			if paused {
				c.advanceMediaTS(ts)
				continue
			}
		}

		return msg, nil
	}
}

// Write writes a message.
func (c *Client) Write(msg message.Message) error {
	return c.mrw.Write(msg)
}

// Paused returns whether the client is currently paused.
func (c *Client) Paused() bool {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	return c.paused
}

// Pause sends a NetStream "pause" command ("pause" flag set to true) to the
// server and waits for the corresponding onStatus. The pause position is the
// timestamp of the last delivered media message. It can only be used in play
// mode.
func (c *Client) Pause(ctx context.Context) error {
	return c.sendPause(ctx, true)
}

// Resume sends a NetStream "pause" command ("pause" flag set to false) to the
// server, resuming playback from the point at which Pause() was called, and
// waits for the corresponding onStatus. It can only be used in play mode.
func (c *Client) Resume(ctx context.Context) error {
	return c.sendPause(ctx, false)
}

func (c *Client) sendPause(ctx context.Context, pause bool) error {
	if c.messages == nil {
		return liberrors.ErrClientNotPlaying{}
	}

	c.pauseMu.Lock()
	if c.closed {
		c.pauseMu.Unlock()
		return liberrors.ErrClientClosed{}
	}

	if pause && c.paused {
		c.pauseMu.Unlock()
		return liberrors.ErrClientAlreadyPaused{}
	}

	if !pause && !c.paused {
		c.pauseMu.Unlock()
		return liberrors.ErrClientNotPaused{}
	}

	position := time.Duration(c.lastMediaTS.Load())
	if pause {
		c.pausePoint = position
	} else {
		position = c.pausePoint
	}
	c.paused = pause
	c.pauseMu.Unlock()

	err := c.sendPauseAndWait(ctx, pause, position)
	if err != nil {
		// roll back the local state, since the server did not acknowledge it
		c.pauseMu.Lock()
		c.paused = !pause
		c.pauseMu.Unlock()
		return err
	}

	// wake the pump so that a media delivery blocked before the pause
	// command can be re-evaluated against the new state.
	select {
	case c.pumpWake <- struct{}{}:
	default:
	}

	return nil
}

func (c *Client) sendPauseAndWait(ctx context.Context, pause bool, position time.Duration) error {
	c.cmdMu.Lock()
	c.nextCmdID++
	commandID := c.nextCmdID
	resCh := make(chan *message.CommandAMF0, 1)
	c.cmdPending[commandID] = resCh
	c.cmdMu.Unlock()

	defer func() {
		c.cmdMu.Lock()
		delete(c.cmdPending, commandID)
		c.cmdMu.Unlock()
	}()

	err := c.mrw.Write(&message.CommandAMF0{
		ChunkStreamID:   4,
		MessageStreamID: 0x1000000,
		Name:            "pause",
		CommandID:       commandID,
		Arguments: []any{
			nil,
			pause,
			float64(position / time.Millisecond),
		},
	})
	if err != nil {
		return err
	}

	select {
	case res := <-resCh:
		if res.Name == "onStatus" && resultIsOK1(res) {
			return nil
		}

		return fmt.Errorf("server rejected %s: %v", pauseCommandName(pause), res)

	case <-ctx.Done():
		return ctx.Err()

	case <-c.readDone:
		return liberrors.ErrClientClosed{}
	}
}

func pauseCommandName(pause bool) string {
	if pause {
		return "pause"
	}
	return "unpause"
}

func (c *Client) startReadPump() {
	c.messages = make(chan message.Message, clientMessageQueueSize)
	c.pumpWake = make(chan struct{}, 1)
	c.readDone = make(chan struct{})
	c.done = make(chan struct{})
	c.cmdPending = make(map[int]chan *message.CommandAMF0)
	c.nextCmdID = 5

	go c.readPump()
}

func (c *Client) readPump() {
	defer close(c.readDone)

	for {
		msg, err := c.mrw.Read()
		if err != nil {
			c.failReadPump(err)
			return
		}

		if cmd, ok := msg.(*message.CommandAMF0); ok {
			c.pumpHandleCommand(cmd)
			continue
		}

		if !c.pumpAcceptMessage(msg) {
			continue
		}

		if !c.pumpDeliver(msg) {
			return
		}
	}
}

func (c *Client) failReadPump(err error) {
	c.readErr = err

	c.cmdMu.Lock()
	c.cmdPending = nil
	c.cmdMu.Unlock()

	close(c.messages)
}

func (c *Client) pumpHandleCommand(cmd *message.CommandAMF0) {
	c.cmdMu.Lock()
	ch, ok := c.cmdPending[cmd.CommandID]

	// some servers (e.g. nginx-rtmp) answer with an onStatus carrying
	// command ID 0 instead of the request command ID; route it to the
	// only pending waiter.
	if !ok && cmd.Name == "onStatus" && len(c.cmdPending) == 1 {
		for _, pending := range c.cmdPending {
			ch = pending
			ok = true
		}
	}
	c.cmdMu.Unlock()

	if ok {
		select {
		case ch <- cmd:
		case <-c.done:
		}
		return
	}

	// unsolicited commands are delivered to the user like in the
	// non-pump path.
	c.pumpDeliver(cmd) //nolint:errcheck
}

// pumpAcceptMessage reports whether msg must be delivered to the user.
// Media messages are dropped while paused and when their timestamp is older
// than the last delivered one (the server restarts from a keyframe on resume).
func (c *Client) pumpAcceptMessage(msg message.Message) bool {
	ts, isMedia := mediaMessageDTS(msg)
	if !isMedia {
		return true
	}

	c.pauseMu.Lock()
	paused := c.paused
	c.pauseMu.Unlock()

	if paused {
		return false
	}

	return c.advanceMediaTS(ts)
}

// advanceMediaTS records ts as the latest observed media timestamp when it is
// not older than the current one. It returns false when the message must be
// dropped as out of order.
func (c *Client) advanceMediaTS(ts time.Duration) bool {
	for {
		cur := c.lastMediaTS.Load()

		if ts < time.Duration(cur) {
			return false
		}

		if c.lastMediaTS.CompareAndSwap(cur, int64(ts)) {
			return true
		}
	}
}

// pumpDeliver sends msg to the user queue. When the queue is full it waits,
// but keeps honoring pause state changes and connection termination so that
// Pause() cannot be blocked by a stalled reader.
func (c *Client) pumpDeliver(msg message.Message) bool {
	_, isMedia := mediaMessageDTS(msg)

	for {
		c.pauseMu.Lock()
		paused := c.paused
		c.pauseMu.Unlock()

		if paused && isMedia {
			return true
		}

		select {
		case c.messages <- msg:
			return true

		default:
		}

		select {
		case c.messages <- msg:
			return true

		case <-c.pumpWake:
			continue

		case <-c.done:
			return false
		}
	}
}

// mediaMessageDTS returns the DTS of a media (audio or video) message.
func mediaMessageDTS(msg message.Message) (time.Duration, bool) {
	switch msg := msg.(type) {
	case *message.Video:
		return msg.DTS, true

	case *message.VideoExCodedFrames:
		return msg.DTS, true

	case *message.VideoExFramesX:
		return msg.DTS, true

	case *message.Audio:
		if msg.Codec == 0 {
			return 0, false
		}
		return msg.DTS, true

	case *message.AudioExCodedFrames:
		return msg.DTS, true
	}

	return 0, false
}
