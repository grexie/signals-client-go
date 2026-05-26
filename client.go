package signalsclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
)

const defaultWebSocketURL = "wss://signals.grexie.com/ws"

type clientConfig struct {
	url        string
	header     http.Header
	dialer     *websocket.Dialer
	bufferSize int
}

// ClientOption customizes a SignalsClient.
type ClientOption func(*clientConfig)

// WithURL sets the complete websocket URL. If not set, the public production
// endpoint is used.
func WithURL(rawURL string) ClientOption {
	return func(cfg *clientConfig) {
		cfg.url = rawURL
	}
}

// WithBaseURL converts an http(s) API base URL to the /ws websocket endpoint.
func WithBaseURL(rawURL string) ClientOption {
	return func(cfg *clientConfig) {
		u, err := url.Parse(rawURL)
		if err != nil {
			cfg.url = rawURL
			return
		}
		switch u.Scheme {
		case "http":
			u.Scheme = "ws"
		case "https":
			u.Scheme = "wss"
		}
		u.Path = "/ws"
		u.RawQuery = ""
		cfg.url = u.String()
	}
}

// WithHeader adds an extra websocket handshake header.
func WithHeader(key, value string) ClientOption {
	return func(cfg *clientConfig) {
		cfg.header.Set(key, value)
	}
}

// WithDialer overrides the websocket dialer.
func WithDialer(dialer *websocket.Dialer) ClientOption {
	return func(cfg *clientConfig) {
		if dialer != nil {
			cfg.dialer = dialer
		}
	}
}

// WithBufferSize changes the event and error channel buffer size.
func WithBufferSize(size int) ClientOption {
	return func(cfg *clientConfig) {
		if size > 0 {
			cfg.bufferSize = size
		}
	}
}

// SignalsClient manages an authenticated Grexie Signals websocket connection.
type SignalsClient struct {
	token SignalsWebSocketToken
	cfg   clientConfig

	mu            sync.Mutex
	writeMu       sync.Mutex
	subMu         sync.Mutex
	conn          *websocket.Conn
	done          chan struct{}
	subscriptions map[*eventSubscription]struct{}

	events chan Event
	errors chan error
}

type eventSubscription struct {
	events chan Event
	errors chan error
}

// NewSignalsClient creates a client. Call Connect before subscribing.
func NewSignalsClient(token SignalsWebSocketToken, opts ...ClientOption) *SignalsClient {
	cfg := clientConfig{
		url:        defaultWebSocketURL,
		header:     make(http.Header),
		dialer:     websocket.DefaultDialer,
		bufferSize: 128,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &SignalsClient{
		token:         token,
		cfg:           cfg,
		done:          make(chan struct{}),
		events:        make(chan Event, cfg.bufferSize),
		errors:        make(chan error, cfg.bufferSize),
		subscriptions: make(map[*eventSubscription]struct{}),
	}
}

// Connect opens the websocket and starts the reader loop.
func (c *SignalsClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return nil
	}
	header := c.cfg.header.Clone()
	if c.token != "" {
		header.Set("Authorization", "Bearer "+string(c.token))
	}
	conn, _, err := c.cfg.dialer.DialContext(ctx, c.cfg.url, header)
	if err != nil {
		return err
	}
	c.conn = conn
	c.done = make(chan struct{})
	go c.readLoop(conn, c.done)
	return nil
}

// Close closes the websocket connection.
func (c *SignalsClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	return err
}

// Events returns the stream of typed websocket events.
func (c *SignalsClient) Events() <-chan Event {
	return c.events
}

// Errors returns asynchronous read and protocol errors.
func (c *SignalsClient) Errors() <-chan error {
	return c.errors
}

// SubscribeEvents returns an independent fan-out stream of events for one
// consumer. Use this when several components, such as multiple PositionManager
// instances, share one SignalsClient.
func (c *SignalsClient) SubscribeEvents(ctx context.Context) (<-chan Event, <-chan error) {
	sub := &eventSubscription{
		events: make(chan Event, c.cfg.bufferSize),
		errors: make(chan error, c.cfg.bufferSize),
	}
	c.subMu.Lock()
	c.subscriptions[sub] = struct{}{}
	c.subMu.Unlock()
	go func() {
		<-ctx.Done()
		c.removeSubscription(sub)
	}()
	return sub.events, sub.errors
}

func (c *SignalsClient) removeSubscription(sub *eventSubscription) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if _, ok := c.subscriptions[sub]; !ok {
		return
	}
	delete(c.subscriptions, sub)
	close(sub.events)
	close(sub.errors)
}

// Receive waits for the next event or error.
func (c *SignalsClient) Receive(ctx context.Context) (Event, error) {
	select {
	case ev, ok := <-c.events:
		if !ok {
			return nil, errors.New("signalsclient: websocket event stream is closed")
		}
		return ev, nil
	case err, ok := <-c.errors:
		if !ok {
			return nil, errors.New("signalsclient: websocket error stream is closed")
		}
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Subscribe requests signal and lifecycle events for a venue/instrument pair.
// The server answers with a SubscribedEvent carrying the subscription id.
func (c *SignalsClient) Subscribe(ctx context.Context, venue, instrument string) error {
	return c.writeJSON(ctx, map[string]string{
		"type":       "subscribe",
		"venue":      venue,
		"instrument": instrument,
	})
}

// Unsubscribe removes a subscription by id.
func (c *SignalsClient) Unsubscribe(ctx context.Context, subscriptionID int64) error {
	return c.writeJSON(ctx, map[string]any{
		"type":           "unsubscribe",
		"subscriptionId": subscriptionID,
	})
}

// UnsubscribeInstrument removes a subscription by venue/instrument pair.
func (c *SignalsClient) UnsubscribeInstrument(ctx context.Context, venue, instrument string) error {
	return c.writeJSON(ctx, map[string]string{
		"type":       "unsubscribe",
		"venue":      venue,
		"instrument": instrument,
	})
}

func (c *SignalsClient) writeJSON(ctx context.Context, payload any) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return errors.New("signalsclient: websocket is not connected")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	done := make(chan error, 1)
	go func() {
		done <- conn.WriteMessage(websocket.TextMessage, data)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *SignalsClient) readLoop(conn *websocket.Conn, done <-chan struct{}) {
	defer c.closeStreams()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if c.broadcastError(err, done) {
				return
			}
			return
		}
		ev, err := ParseEvent(data)
		if err != nil {
			if c.broadcastError(err, done) {
				return
			}
			continue
		}
		if c.broadcastEvent(ev, done) {
			return
		}
	}
}

func (c *SignalsClient) broadcastEvent(ev Event, done <-chan struct{}) bool {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	select {
	case c.events <- ev:
	case <-done:
		return true
	}
	for sub := range c.subscriptions {
		select {
		case sub.events <- ev:
		case <-done:
			return true
		}
	}
	return false
}

func (c *SignalsClient) broadcastError(err error, done <-chan struct{}) bool {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	select {
	case c.errors <- err:
	case <-done:
		return true
	}
	for sub := range c.subscriptions {
		select {
		case sub.errors <- err:
		case <-done:
			return true
		}
	}
	return false
}

func (c *SignalsClient) closeStreams() {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	close(c.events)
	close(c.errors)
	for sub := range c.subscriptions {
		close(sub.events)
		close(sub.errors)
		delete(c.subscriptions, sub)
	}
}
