package miss

import (
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// session manages a single MISS client connection that can serve multiple
// streams (channels). For dual-channel cameras, both channel 0 and channel 1
// share the same underlying UDP/TCP session to avoid exhausting the camera's
// limited connection slots.
type session struct {
	client *Client
	key    string // cache key: host|did

	mu      sync.Mutex
	streams map[*stream]struct{}

	// startedMask tracks which channels have been started (bit 0 = ch0, bit 1 = ch1).
	startedMask uint8
	quality     [2]string // remembered quality per channel

	workerOnce sync.Once
	closeOnce  sync.Once

	speakerOnce sync.Once
	speakerErr  error
}

// stream represents a single channel's view of a session. It receives
// packets dispatched by the session worker and provides a ReadPacket
// interface compatible with what the Producer expects.
type stream struct {
	session *session
	channel uint8
	ch      chan *Packet

	closeOnce sync.Once
	deadline  atomic.Value // time.Time
	done      chan struct{}
}

var (
	sessionMu sync.Mutex
	sessions  = map[string]*session{}
)

// sessionKey builds a cache key from the URL. Two URLs pointing to the same
// camera (same host and did) will share a session.
func sessionKey(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	key := u.Host
	if did := u.Query().Get("did"); did != "" {
		key += "|" + did
	}
	return key, nil
}

// getOrCreateSession returns an existing session for the camera or creates a
// new one. Dafang-like models that don't support dual-channel always get a
// fresh client (no session sharing).
func getOrCreateSession(rawURL string) (*session, error) {
	key, err := sessionKey(rawURL)
	if err != nil {
		return nil, err
	}

	sessionMu.Lock()
	if s, ok := sessions[key]; ok {
		// Reuse existing session only if the underlying client supports
		// dual-channel (not dafang-like).
		if !s.client.IsDafangLike() {
			sessionMu.Unlock()
			return s, nil
		}
	}
	sessionMu.Unlock()

	client, err := NewClient(rawURL)
	if err != nil {
		return nil, err
	}

	// Dafang-like models: no session sharing, return standalone session.
	if client.IsDafangLike() {
		return &session{
			client:  client,
			key:     key,
			streams: make(map[*stream]struct{}),
		}, nil
	}

	s := &session{
		client:  client,
		key:     key,
		streams: make(map[*stream]struct{}),
	}

	sessionMu.Lock()
	if existing, ok := sessions[key]; ok {
		// Another goroutine created it between our check and now.
		sessionMu.Unlock()
		_ = client.Close()
		return existing, nil
	}
	sessions[key] = s
	sessionMu.Unlock()

	return s, nil
}

// openStream creates a new stream for the given channel on this session.
func (s *session) openStream(channel uint8) *stream {
	st := &stream{
		session: s,
		channel: channel,
		ch:      make(chan *Packet, 100),
		done:    make(chan struct{}),
	}
	st.deadline.Store(time.Time{})

	s.mu.Lock()
	s.streams[st] = struct{}{}
	s.mu.Unlock()

	s.workerOnce.Do(func() {
		go s.worker()
	})

	return st
}

// startMedia sends the appropriate VideoStart command. If only one channel is
// active, it sends a single-channel command. If both channels are active, it
// sends a dual-channel command.
func (s *session) startMedia(channel uint8, quality, audio string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch := int(channel)
	if ch > 1 {
		ch = 0
	}

	// Already started this channel.
	if s.startedMask&(1<<ch) != 0 {
		return nil
	}

	s.quality[ch] = quality
	other := ch ^ 1

	// If the other channel is already started, upgrade to dual-channel.
	if s.startedMask&(1<<other) != 0 {
		err := s.client.StartMediaDual(
			s.quality[0], s.quality[1], audio,
		)
		if err != nil {
			return err
		}
		s.startedMask |= 1 << ch
		return nil
	}

	// Single channel start.
	chStr := "0"
	if channel == 1 {
		chStr = "1"
	}
	if err := s.client.StartMedia(chStr, quality, audio); err != nil {
		return err
	}
	s.startedMask |= 1 << ch
	return nil
}

// worker is the read loop that dispatches packets to streams.
func (s *session) worker() {
	for {
		_ = s.client.SetDeadline(time.Now().Add(10 * time.Second))

		pkt, err := s.client.ReadPacket()
		if err != nil {
			s.shutdown()
			return
		}

		s.dispatch(pkt)
	}
}

// dispatch routes a packet to the appropriate stream(s).
func (s *session) dispatch(pkt *Packet) {
	s.mu.Lock()
	if len(s.streams) == 0 {
		s.mu.Unlock()
		return
	}

	streams := make([]*stream, 0, len(s.streams))
	for st := range s.streams {
		streams = append(streams, st)
	}
	s.mu.Unlock()

	// Audio packets are broadcast to all streams.
	if isAudioCodec(pkt.CodecID) {
		for _, st := range streams {
			st.push(pkt)
		}
		return
	}

	// If only one stream is active, send all video to it.
	if len(streams) == 1 {
		streams[0].push(pkt)
		return
	}

	// Multiple streams: route by channel.
	ch := s.pickChannel(pkt)
	for _, st := range streams {
		if st.channel == ch {
			st.push(pkt)
		}
	}
}

// pickChannel determines which channel a video packet belongs to.
func (s *session) pickChannel(pkt *Packet) uint8 {
	// Primary: use the channel field parsed from the packet header.
	if pkt.ChannelOK {
		return pkt.Channel
	}
	// Fallback: default to channel 0.
	return 0
}

// startSpeaker starts the speaker on the session (once).
func (s *session) startSpeaker() error {
	s.speakerOnce.Do(func() {
		s.speakerErr = s.client.StartSpeaker()
	})
	return s.speakerErr
}

// writeAudio sends audio data to the camera.
func (s *session) writeAudio(codecID uint32, payload []byte) error {
	return s.client.WriteAudio(codecID, payload)
}

// speakerCodec returns the speaker codec for the camera model.
func (s *session) speakerCodec() uint32 {
	return s.client.SpeakerCodec()
}

// removeStream removes a stream from the session. If no streams remain, the
// session is shut down.
func (s *session) removeStream(st *stream) {
	s.mu.Lock()
	if _, ok := s.streams[st]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.streams, st)
	empty := len(s.streams) == 0
	s.mu.Unlock()

	st.close()

	if empty {
		s.shutdown()
	}
}

// shutdown tears down the session, closing all streams and the client.
func (s *session) shutdown() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		streams := make([]*stream, 0, len(s.streams))
		for st := range s.streams {
			streams = append(streams, st)
		}
		s.streams = make(map[*stream]struct{})
		s.mu.Unlock()

		for _, st := range streams {
			st.close()
		}

		_ = s.client.Close()

		sessionMu.Lock()
		if sessions[s.key] == s {
			delete(sessions, s.key)
		}
		sessionMu.Unlock()
	})
}

// --- stream methods ---

// ReadPacket reads the next packet for this stream, respecting deadlines.
func (st *stream) ReadPacket() (*Packet, error) {
	deadline, _ := st.deadline.Load().(time.Time)
	if deadline.IsZero() {
		select {
		case pkt := <-st.ch:
			return pkt, nil
		case <-st.done:
			return nil, io.EOF
		}
	}

	d := time.Until(deadline)
	if d <= 0 {
		// Check if there's already a packet available.
		select {
		case pkt := <-st.ch:
			return pkt, nil
		default:
			return nil, &net.OpError{Op: "read", Err: &timeoutError{}}
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case pkt := <-st.ch:
		return pkt, nil
	case <-st.done:
		return nil, io.EOF
	case <-timer.C:
		return nil, &net.OpError{Op: "read", Err: &timeoutError{}}
	}
}

// SetDeadline sets a read deadline for this stream.
func (st *stream) SetDeadline(t time.Time) error {
	st.deadline.Store(t)
	return nil
}

// RemoteAddr returns the remote address of the underlying connection.
func (st *stream) RemoteAddr() net.Addr {
	return st.session.client.RemoteAddr()
}

// Close removes this stream from the session.
func (st *stream) Close() error {
	st.session.removeStream(st)
	return nil
}

func (st *stream) push(pkt *Packet) {
	select {
	case <-st.done:
		return
	default:
	}
	select {
	case st.ch <- pkt:
	default:
		// Drop packet if the buffer is full.
	}
}

func (st *stream) close() {
	st.closeOnce.Do(func() {
		close(st.done)
	})
}

// --- helpers ---

func isAudioCodec(codecID uint32) bool {
	switch codecID {
	case codecPCM, codecPCMU, codecPCMA, codecOPUS:
		return true
	}
	return false
}

// timeoutError implements the net.Error interface for deadline timeouts.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }
