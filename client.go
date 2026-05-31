package signalsclient

import (
	"context"
	"encoding/json"
	"errors"
	"math"
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

// SignalsClient is the basket-router transport used by SignalsManager. The
// production websocket implementation reconnects and replays active basket
// subscriptions, while tests and the Signals server can provide an in-process
// implementation.
type SignalsClient interface {
	Subscribe(ctx context.Context, request SubscribeRequest) (int64, error)
	UpdateAsset(ctx context.Context, subscriptionID int64, asset AssetSnapshot) error
	UpdatePosition(ctx context.Context, subscriptionID int64, position Position) error
	AddInstrument(ctx context.Context, subscriptionID int64, instrument string) error
	RemoveInstrument(ctx context.Context, subscriptionID int64, instrument string) error
	UpdateConfig(ctx context.Context, subscriptionID int64, cfg RuntimeConfig) error
	ScheduleWithdrawal(ctx context.Context, subscriptionID int64, withdrawal WithdrawalRequest) error
	Unsubscribe(ctx context.Context, subscriptionID int64) error
	SubscribeEvents(ctx context.Context) (<-chan Event, <-chan error)
}

// WebSocketSignalsClient manages one authenticated Grexie Signals websocket
// connection. It is a low-level transport; use NewSignalsClient for a
// reconnecting multi-subscription client.
type WebSocketSignalsClient struct {
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

// NewWebSocketSignalsClient creates a single websocket client. Call Connect
// before sending requests.
func NewWebSocketSignalsClient(token SignalsWebSocketToken, opts ...ClientOption) *WebSocketSignalsClient {
	cfg := clientConfig{
		url:        defaultWebSocketURL,
		header:     make(http.Header),
		dialer:     websocket.DefaultDialer,
		bufferSize: 128,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &WebSocketSignalsClient{
		token:         token,
		cfg:           cfg,
		done:          make(chan struct{}),
		events:        make(chan Event, cfg.bufferSize),
		errors:        make(chan error, cfg.bufferSize),
		subscriptions: make(map[*eventSubscription]struct{}),
	}
}

// NewSignalsClient creates a reconnecting basket-router client.
func NewSignalsClient(token SignalsWebSocketToken, opts ...ClientOption) *ReconnectingSignalsClient {
	return NewReconnectingSignalsClient(token, opts...)
}

// Connect opens the websocket and starts the reader loop.
func (c *WebSocketSignalsClient) Connect(ctx context.Context) error {
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
func (c *WebSocketSignalsClient) Close() error {
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
func (c *WebSocketSignalsClient) Events() <-chan Event {
	return c.events
}

// Errors returns asynchronous read and protocol errors.
func (c *WebSocketSignalsClient) Errors() <-chan error {
	return c.errors
}

// SubscribeEvents returns an independent fan-out stream of events for one
// consumer. Use this when several components, such as multiple PositionManager
// instances, share one SignalsClient.
func (c *WebSocketSignalsClient) SubscribeEvents(ctx context.Context) (<-chan Event, <-chan error) {
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

func (c *WebSocketSignalsClient) removeSubscription(sub *eventSubscription) {
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
func (c *WebSocketSignalsClient) Receive(ctx context.Context) (Event, error) {
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

// Subscribe requests one Bollinger-router basket subscription. The server
// answers with a SubscribedEvent carrying the subscription id.
func (c *WebSocketSignalsClient) Subscribe(ctx context.Context, request SubscribeRequest) (int64, error) {
	request.Type = "subscribe"
	return 0, c.writeJSON(ctx, request.normalized())
}

// SubscribeInstrument requests the legacy single-instrument signal stream.
// Deprecated: new integrations should use SignalsManager and basket
// subscriptions.
func (c *WebSocketSignalsClient) SubscribeInstrument(ctx context.Context, venue, instrument string) error {
	return c.writeJSON(ctx, map[string]any{
		"type":       "subscribe",
		"venue":      venue,
		"instrument": instrument,
	})
}

// UpdateAsset publishes the current account state for one settlement currency.
func (c *WebSocketSignalsClient) UpdateAsset(ctx context.Context, subscriptionID int64, asset AssetSnapshot) error {
	return c.writeJSON(ctx, map[string]any{
		"type":           "update-asset",
		"subscriptionId": subscriptionID,
		"venue":          asset.Venue,
		"currency":       asset.Currency,
		"cash":           asset.Cash,
		"available":      asset.Available,
		"used":           asset.Used,
		"equity":         asset.Equity,
		"maxUsage":       asset.MaxUsage,
	})
}

// UpdatePosition publishes the current venue position for one instrument/side.
func (c *WebSocketSignalsClient) UpdatePosition(ctx context.Context, subscriptionID int64, position Position) error {
	return c.writeJSON(ctx, map[string]any{
		"type":            "update-position",
		"subscriptionId":  subscriptionID,
		"venue":           position.Venue,
		"instrument":      position.Instrument,
		"side":            position.Side(),
		"status":          position.Status,
		"size":            math.Abs(position.Size),
		"entryPrice":      position.EntryPrice,
		"markPrice":       position.LastPrice,
		"margin":          positionMargin(position),
		"leverage":        position.Leverage,
		"takeProfitPrice": positiveOr(position.TakeProfitPrice, priceFromRisk(position.EntryPrice, position.Side(), position.TakeProfit)),
		"stopLossPrice":   positiveOr(position.StopLossPrice, priceFromRisk(position.EntryPrice, oppositeSide(position.Side()), position.StopLoss)),
	})
}

// AddInstrument adds one instrument to an existing basket.
func (c *WebSocketSignalsClient) AddInstrument(ctx context.Context, subscriptionID int64, instrument string) error {
	return c.writeJSON(ctx, map[string]any{"type": "add-instrument", "subscriptionId": subscriptionID, "instrument": instrument})
}

// RemoveInstrument removes one instrument from an existing basket.
func (c *WebSocketSignalsClient) RemoveInstrument(ctx context.Context, subscriptionID int64, instrument string) error {
	return c.writeJSON(ctx, map[string]any{"type": "remove-instrument", "subscriptionId": subscriptionID, "instrument": instrument})
}

func (c *WebSocketSignalsClient) UpdateConfig(ctx context.Context, subscriptionID int64, cfg RuntimeConfig) error {
	return c.writeJSON(ctx, map[string]any{
		"type":                "update-config",
		"subscriptionId":      subscriptionID,
		"profitWithdrawRatio": cfg.ProfitWithdrawRatio,
	})
}

func (c *WebSocketSignalsClient) ScheduleWithdrawal(ctx context.Context, subscriptionID int64, withdrawal WithdrawalRequest) error {
	return c.writeJSON(ctx, map[string]any{
		"type":           "schedule-withdrawal",
		"subscriptionId": subscriptionID,
		"venue":          withdrawal.Venue,
		"currency":       withdrawal.Currency,
		"amount":         withdrawal.Amount,
		"reason":         withdrawal.Reason,
	})
}

// Unsubscribe removes a subscription by id.
func (c *WebSocketSignalsClient) Unsubscribe(ctx context.Context, subscriptionID int64) error {
	return c.writeJSON(ctx, map[string]any{
		"type":           "unsubscribe",
		"subscriptionId": subscriptionID,
	})
}

// UnsubscribeInstrument requests a legacy single-instrument unsubscribe.
// Deprecated: new integrations should use SignalsManager and basket
// subscriptions.
func (c *WebSocketSignalsClient) UnsubscribeInstrument(ctx context.Context, venue, instrument string) error {
	return c.writeJSON(ctx, map[string]any{
		"type":       "unsubscribe",
		"venue":      venue,
		"instrument": instrument,
	})
}

func (c *WebSocketSignalsClient) writeJSON(ctx context.Context, payload any) error {
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

func (c *WebSocketSignalsClient) readLoop(conn *websocket.Conn, done <-chan struct{}) {
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

func (c *WebSocketSignalsClient) broadcastEvent(ev Event, done <-chan struct{}) bool {
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

func (c *WebSocketSignalsClient) broadcastError(err error, done <-chan struct{}) bool {
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

func (c *WebSocketSignalsClient) closeStreams() {
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

func positionMargin(position Position) float64 {
	if position.Leverage <= 0 || position.EntryPrice <= 0 {
		return 0
	}
	return math.Abs(position.Size) * position.EntryPrice / position.Leverage
}

func oppositeSide(side Side) Side {
	switch side {
	case SideBuy:
		return SideSell
	case SideSell:
		return SideBuy
	default:
		return ""
	}
}

func priceFromRisk(entry float64, side Side, risk float64) float64 {
	if entry <= 0 || risk <= 0 {
		return 0
	}
	if side == SideSell {
		return entry * (1 - risk)
	}
	return entry * (1 + risk)
}
