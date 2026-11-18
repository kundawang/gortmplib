package gortmplib

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/bluenviron/gortmplib/pkg/message"
)

// Publisher errors.
var (
	// ErrPublisherClosed is returned by write methods after Close has been called.
	ErrPublisherClosed = errors.New("publisher is closed")
	// ErrPublisherQueueFull is returned by write methods when the queue of
	// messages waiting to be sent is full. The queue size bounds the memory
	// used while the connection is down.
	ErrPublisherQueueFull = errors.New("publisher queue is full")
	// ErrReconnectsExhausted is returned by Wait when all reconnect attempts failed.
	ErrReconnectsExhausted = errors.New("reconnect attempts exhausted")
	// ErrNoKeyFrameAfterReconnect is returned by Wait when no key frame has
	// been received within ReconnectConfig.KeyFrameTimeout after a reconnection.
	ErrNoKeyFrameAfterReconnect = errors.New("no key frame received after reconnection")
)

const (
	defaultReconnectInitialBackoff  = 1 * time.Second
	defaultReconnectMaxBackoff      = 30 * time.Second
	defaultReconnectQueueSize       = 256
	defaultReconnectKeyFrameTimeout = 10 * time.Second
)

// ReconnectConfig configures the reconnection behavior of Publisher.
type ReconnectConfig struct {
	// MaxRetries is the maximum number of consecutive reconnect attempts
	// after a connection loss. Zero means unlimited attempts.
	MaxRetries int
	// InitialBackoff is the wait time after the first failed reconnect
	// attempt. It is doubled after each failure, up to MaxBackoff.
	// It defaults to 1 second.
	InitialBackoff time.Duration
	// MaxBackoff is the maximum wait time between reconnect attempts.
	// It defaults to 30 seconds.
	MaxBackoff time.Duration
	// QueueSize is the maximum number of messages that can be buffered
	// while the connection is down. When the queue is full, write methods
	// return ErrPublisherQueueFull. It defaults to 256.
	QueueSize int
	// KeyFrameTimeout is the maximum time to wait for a key frame after a
	// reconnection. After it expires, the publisher terminates with
	// ErrNoKeyFrameAfterReconnect. It defaults to 10 seconds.
	KeyFrameTimeout time.Duration
}

func (c *ReconnectConfig) initialBackoff() time.Duration {
	if c.InitialBackoff != 0 {
		return c.InitialBackoff
	}
	return defaultReconnectInitialBackoff
}

func (c *ReconnectConfig) maxBackoff() time.Duration {
	if c.MaxBackoff != 0 {
		return c.MaxBackoff
	}
	return defaultReconnectMaxBackoff
}

func (c *ReconnectConfig) queueSize() int {
	if c.QueueSize != 0 {
		return c.QueueSize
	}
	return defaultReconnectQueueSize
}

func (c *ReconnectConfig) keyFrameTimeout() time.Duration {
	if c.KeyFrameTimeout != 0 {
		return c.KeyFrameTimeout
	}
	return defaultReconnectKeyFrameTimeout
}

// isSetupMessage reports whether a message carries stream metadata or codec
// configurations, that must be sent again after a reconnection.
func isSetupMessage(msg message.Message) bool {
	switch m := msg.(type) {
	case *message.DataAMF0:
		return len(m.Payload) >= 1 && m.Payload[0] == "@setDataFrame"

	case *message.Video:
		return m.FrameType != message.VideoFrameTypeCommand &&
			m.PacketType == message.VideoPacketTypeConfig

	case *message.Audio:
		return m.Codec == message.CodecMPEG4Audio &&
			m.AACType == message.AudioAACTypeConfig

	case *message.VideoExSequenceStart, *message.AudioExSequenceStart:
		return true

	case *message.VideoExMultitrack:
		return isSetupMessage(m.Wrapped)

	case *message.AudioExMultitrack:
		return isSetupMessage(m.Wrapped)
	}

	return false
}

// videoFrameType returns the frame type of a video frame message.
func videoFrameType(msg message.Message) (message.VideoFrameType, bool) {
	switch m := msg.(type) {
	case *message.Video:
		if m.FrameType == message.VideoFrameTypeCommand ||
			m.PacketType != message.VideoPacketTypeAU {
			return 0, false
		}
		return m.FrameType, true

	case *message.VideoExFramesX:
		return m.FrameType, true

	case *message.VideoExCodedFrames:
		return m.FrameType, true

	case *message.VideoExMultitrack:
		return videoFrameType(m.Wrapped)
	}

	return 0, false
}

func messageDTS(msg message.Message) (time.Duration, bool) {
	switch m := msg.(type) {
	case *message.Video:
		return m.DTS, true
	case *message.Audio:
		return m.DTS, true
	case *message.VideoExFramesX:
		return m.DTS, true
	case *message.VideoExCodedFrames:
		return m.DTS, true
	case *message.AudioExCodedFrames:
		return m.DTS, true
	case *message.VideoExMultitrack:
		return messageDTS(m.Wrapped)
	case *message.AudioExMultitrack:
		return messageDTS(m.Wrapped)
	}

	return 0, false
}

func setMessageDTS(msg message.Message, dts time.Duration) {
	switch m := msg.(type) {
	case *message.Video:
		m.DTS = dts
	case *message.Audio:
		m.DTS = dts
	case *message.VideoExFramesX:
		m.DTS = dts
	case *message.VideoExCodedFrames:
		m.DTS = dts
	case *message.AudioExCodedFrames:
		m.DTS = dts
	case *message.VideoExMultitrack:
		setMessageDTS(m.Wrapped, dts)
	case *message.AudioExMultitrack:
		setMessageDTS(m.Wrapped, dts)
	}
}

// publisherConn is the Conn interface between Writer and Publisher.
type publisherConn struct {
	p *Publisher
}

func (pc *publisherConn) Write(msg message.Message) error {
	p := pc.p

	select {
	case <-p.ctx.Done():
		return ErrPublisherClosed
	default:
	}

	if isSetupMessage(msg) {
		p.mutex.Lock()
		p.setupMsgs = append(p.setupMsgs, msg)
		p.mutex.Unlock()
	}

	select {
	case p.queue <- msg:
		return nil
	case <-p.ctx.Done():
		return ErrPublisherClosed
	default:
		return ErrPublisherQueueFull
	}
}

func (pc *publisherConn) Read() (message.Message, error) {
	<-pc.p.done
	return nil, io.EOF
}

func (pc *publisherConn) BytesReceived() uint64 {
	return pc.p.BytesReceived()
}

func (pc *publisherConn) BytesSent() uint64 {
	return pc.p.BytesSent()
}

// Publisher publishes tracks to a RTMP server, with optional automatic
// reconnection.
//
// When Reconnect is set and the connection drops, the publisher connects
// again with configurable retries and backoff, re-handshakes, re-publishes
// the stream, re-sends metadata and codec configurations, and resumes video
// from the next key frame. Timestamps of messages sent after a reconnection
// are shifted forward when needed, in order to keep them monotonically
// increasing and audio/video aligned.
//
// When Reconnect is nil, a connection error terminates the publisher.
type Publisher struct {
	//
	// Target
	//
	// URL of the RTMP server to connect to.
	// Format is rtmp://user:pass@host:port/path#streamKey
	URL *url.URL

	//
	// Tracks to publish.
	//
	Tracks []*Track

	//
	// RTMP parameters (all optional)
	//
	// a TLS configuration to connect to RTMPS servers.
	// It defaults to nil.
	TLSConfig *tls.Config
	// reconnection configuration.
	// When nil, the publisher does not reconnect after a connection loss.
	// It defaults to nil.
	Reconnect *ReconnectConfig

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

	ctx       context.Context
	ctxCancel context.CancelFunc
	queue     chan message.Message
	done      chan struct{}

	mutex     sync.Mutex
	client    *Client
	setupMsgs []message.Message

	writer  *Writer
	connErr chan error

	// the following fields are owned by the run goroutine.
	err            error
	hasVideo       bool
	waitKeyFrame   bool
	kfTimer        *time.Timer
	tsOffset       time.Duration
	adjustTSOffset bool
	lastTS         time.Duration
}

// Initialize initializes Publisher.
// It connects to the server and starts publishing.
// Reconnect attempts, if enabled, apply to connection losses that happen
// after a successful initialization, not to this initial connection.
func (p *Publisher) Initialize(ctx context.Context) error {
	if p.URL == nil {
		return fmt.Errorf("URL is nil")
	}

	queueSize := defaultReconnectQueueSize
	if p.Reconnect != nil {
		queueSize = p.Reconnect.queueSize()
	}

	p.ctx, p.ctxCancel = context.WithCancel(context.Background())
	p.queue = make(chan message.Message, queueSize)
	p.done = make(chan struct{})

	for _, track := range p.Tracks {
		if track.Codec.IsVideo() {
			p.hasVideo = true
			break
		}
	}

	client, err := p.connect(ctx)
	if err != nil {
		p.ctxCancel()
		return err
	}

	p.setClient(client)
	p.connErr = make(chan error, 1)
	go p.drain(client, p.connErr)

	p.writer = &Writer{
		Conn:   &publisherConn{p: p},
		Tracks: p.Tracks,
	}
	err = p.writer.Initialize()
	if err != nil {
		p.closeClient()
		p.ctxCancel()
		return err
	}

	go p.run()

	return nil
}

// Writer returns the Writer that can be used to write tracks.
func (p *Publisher) Writer() *Writer {
	return p.writer
}

// Close closes the publisher. It immediately stops any ongoing reconnect
// attempt. It can be called multiple times.
func (p *Publisher) Close() {
	p.ctxCancel()
	p.closeClient()
}

// Wait waits until the publisher terminates and returns its exit error.
// It returns nil when the publisher has been closed with Close.
func (p *Publisher) Wait() error {
	<-p.done
	return p.err
}

// BytesReceived returns the number of bytes received by the current connection.
func (p *Publisher) BytesReceived() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.client == nil {
		return 0
	}
	return p.client.BytesReceived()
}

// BytesSent returns the number of bytes sent by the current connection.
func (p *Publisher) BytesSent() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.client == nil {
		return 0
	}
	return p.client.BytesSent()
}

func (p *Publisher) connect(ctx context.Context) (*Client, error) {
	c := &Client{
		URL:            p.URL,
		Publish:        true,
		TLSConfig:      p.TLSConfig,
		DialContext:    p.DialContext,
		DialTLSContext: p.DialTLSContext,
	}

	err := c.Initialize(ctx)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (p *Publisher) setClient(c *Client) {
	p.mutex.Lock()
	p.client = c
	p.mutex.Unlock()
}

func (p *Publisher) closeClient() {
	p.mutex.Lock()
	c := p.client
	p.client = nil
	p.mutex.Unlock()

	if c != nil {
		c.Close()
	}
}

// drain reads and discards incoming messages until the connection fails.
func (p *Publisher) drain(c *Client, errCh chan<- error) {
	for {
		_, err := c.Read()
		if err != nil {
			errCh <- err
			return
		}
	}
}

func (p *Publisher) run() {
	defer close(p.done)
	defer p.closeClient()

	for {
		var kfC <-chan time.Time
		if p.kfTimer != nil {
			kfC = p.kfTimer.C
		}

		select {
		case <-p.ctx.Done():
			return

		case err := <-p.connErr:
			if !p.handleConnError(err) {
				return
			}

		case msg := <-p.queue:
			if !p.handleMessage(msg) {
				return
			}

		case <-kfC:
			p.err = ErrNoKeyFrameAfterReconnect
			return
		}
	}
}

// handleConnError handles a connection error.
// It returns false when the publisher must terminate.
func (p *Publisher) handleConnError(err error) bool {
	if p.ctx.Err() != nil {
		return false
	}

	if p.Reconnect == nil {
		p.err = err
		return false
	}

	return p.reconnect()
}

// reconnect attempts to establish a new connection.
// It returns false when the publisher must terminate.
func (p *Publisher) reconnect() bool {
	p.closeClient()

	backoff := p.Reconnect.initialBackoff()
	attempt := 0

	for {
		client, err := p.connect(p.ctx)
		if err == nil {
			err = p.onConnected(client)
		}

		if err == nil {
			return true
		}

		attempt++

		if maxRetries := p.Reconnect.MaxRetries; maxRetries > 0 && attempt >= maxRetries {
			p.err = fmt.Errorf("%w: %w", ErrReconnectsExhausted, err)
			return false
		}

		timer := time.NewTimer(backoff)
		select {
		case <-p.ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}

		backoff = min(backoff*2, p.Reconnect.maxBackoff())
	}
}

// onConnected prepares a new connection for publishing.
func (p *Publisher) onConnected(client *Client) error {
	p.mutex.Lock()
	setupMsgs := append([]message.Message(nil), p.setupMsgs...)
	p.mutex.Unlock()

	// re-send metadata and codec configurations.
	for _, msg := range setupMsgs {
		err := client.Write(msg)
		if err != nil {
			client.Close()
			return err
		}
	}

	p.setClient(client)

	p.connErr = make(chan error, 1)
	go p.drain(client, p.connErr)

	// resume video from the next key frame.
	p.stopKeyFrameTimer()
	if p.hasVideo {
		p.waitKeyFrame = true
		p.kfTimer = time.NewTimer(p.Reconnect.keyFrameTimeout())
	}

	p.adjustTSOffset = true

	return nil
}

func (p *Publisher) stopKeyFrameTimer() {
	if p.kfTimer != nil {
		p.kfTimer.Stop()
		p.kfTimer = nil
	}
}

// handleMessage sends a queued message through the current connection.
// It returns false when the publisher must terminate.
func (p *Publisher) handleMessage(msg message.Message) bool {
	if p.waitKeyFrame {
		if frameType, ok := videoFrameType(msg); ok {
			if frameType != message.VideoFrameTypeKeyFrame {
				// drop inter frames until a key frame arrives.
				return true
			}
			p.waitKeyFrame = false
			p.stopKeyFrameTimer()
		}
	}

	if !isSetupMessage(msg) {
		if dts, ok := messageDTS(msg); ok {
			if p.adjustTSOffset {
				// keep timestamps monotonically increasing across
				// reconnections. The same offset is applied to both audio
				// and video, preserving their alignment.
				if dts+p.tsOffset <= p.lastTS {
					p.tsOffset = p.lastTS + time.Millisecond - dts
				}
				p.adjustTSOffset = false
			}

			ts := dts + p.tsOffset
			setMessageDTS(msg, ts)
			if ts > p.lastTS {
				p.lastTS = ts
			}
		}
	}

	p.mutex.Lock()
	client := p.client
	p.mutex.Unlock()

	if client == nil {
		return p.handleConnError(ErrPublisherClosed)
	}

	err := client.Write(msg)
	if err != nil {
		return p.handleConnError(err)
	}

	return true
}
