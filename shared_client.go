package signalsclient

import (
	"context"
	"errors"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultSignalStaleAfter    = 2 * time.Minute
	DefaultReconnectMinBackoff = time.Second
	DefaultReconnectMaxBackoff = 30 * time.Second
)

var (
	ErrSignalsDisabled = errors.New("signals are disabled")
	ErrSignalsNoToken  = errors.New("signals websocket token is not configured")
)

// SharedClientConfig configures the process-wide websocket client.
type SharedClientConfig struct {
	Enabled             bool
	WebSocketURL        string
	Token               string
	StaleAfter          time.Duration
	ReconnectMinBackoff time.Duration
	ReconnectMaxBackoff time.Duration
	Now                 func() time.Time
}

// EnvSharedClientConfig reads standard SIGNALS_* environment variables.
func EnvSharedClientConfig() SharedClientConfig {
	return NormalizeSharedClientConfig(SharedClientConfig{
		Enabled:      envBool("SIGNALS_ENABLED", true),
		WebSocketURL: strings.TrimSpace(os.Getenv("SIGNALS_WS_URL")),
		Token:        strings.TrimSpace(os.Getenv("SIGNALS_WS_TOKEN")),
	})
}

// NormalizeSharedClientConfig applies production defaults.
func NormalizeSharedClientConfig(cfg SharedClientConfig) SharedClientConfig {
	if strings.TrimSpace(cfg.WebSocketURL) == "" {
		cfg.WebSocketURL = defaultWebSocketURL
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = DefaultSignalStaleAfter
	}
	if cfg.ReconnectMinBackoff <= 0 {
		cfg.ReconnectMinBackoff = DefaultReconnectMinBackoff
	}
	if cfg.ReconnectMaxBackoff <= 0 {
		cfg.ReconnectMaxBackoff = DefaultReconnectMaxBackoff
	}
	if cfg.ReconnectMaxBackoff < cfg.ReconnectMinBackoff {
		cfg.ReconnectMaxBackoff = cfg.ReconnectMinBackoff
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	cfg.WebSocketURL = strings.TrimSpace(cfg.WebSocketURL)
	cfg.Token = strings.TrimSpace(cfg.Token)
	return cfg
}

// SharedSignalState is the latest in-memory signal for one venue/instrument.
type SharedSignalState struct {
	Venue                  string
	Instrument             string
	Direction              string
	SignedScore            float64
	Confidence             float64
	TargetExposure         float64
	TakeProfit             float64
	StopLoss               float64
	TrailingStopActivation float64
	TrailingStopDistance   float64
	TrailingStopMinProfit  float64
	Price                  float64
	RiskMetadata           map[string]any
	TimeframeComponents    []SignalComponent
	UpdatedAt              time.Time
	Replay                 bool
	Stale                  bool
}

// SharedInstrumentState is the latest lifecycle/ticker state for one subscription.
type SharedInstrumentState struct {
	Venue             string
	Instrument        string
	LatestTickerPrice float64
	LastUpdate        time.Time
	Stage             string
	Message           string
	Disconnected      bool
	Stale             bool
}

type sharedSubscriptionState struct {
	venue          string
	instrument     string
	refCount       int
	subscriptionID int64
	active         bool
	requested      bool
}

// SharedSubscription is a retained interest in one venue/instrument stream.
type SharedSubscription struct {
	client     *SharedClient
	venue      string
	instrument string
	once       sync.Once
}

// Close releases this consumer's subscription reference. The shared websocket
// unsubscribes from the server only when the final local consumer closes.
func (s *SharedSubscription) Close() {
	if s == nil || s.client == nil {
		return
	}
	s.once.Do(func() { s.client.releaseSubscription(s.venue, s.instrument) })
}

type sharedEventSubscription struct {
	events chan Event
	errors chan error
}

// SharedClient owns one reconnecting websocket and a process-wide in-memory
// signal cache. It deduplicates subscribe/unsubscribe requests for all local
// consumers and exposes reconnect-safe event fan-out.
type SharedClient struct {
	cfg SharedClientConfig

	mu              sync.RWMutex
	client          *WebSocketSignalsClient
	started         bool
	connected       bool
	connectionCount int
	lastError       error
	subscriptions   map[string]*sharedSubscriptionState
	signals         map[string]SharedSignalState
	instruments     map[string]SharedInstrumentState
	watchers        map[chan struct{}]struct{}
	eventWatchers   map[*sharedEventSubscription]struct{}
}

// NewSharedClient creates a reconnecting shared websocket client.
func NewSharedClient(cfg SharedClientConfig) *SharedClient {
	cfg = NormalizeSharedClientConfig(cfg)
	return &SharedClient{
		cfg:           cfg,
		subscriptions: make(map[string]*sharedSubscriptionState),
		signals:       make(map[string]SharedSignalState),
		instruments:   make(map[string]SharedInstrumentState),
		watchers:      make(map[chan struct{}]struct{}),
		eventWatchers: make(map[*sharedEventSubscription]struct{}),
	}
}

var defaultSharedClient = struct {
	sync.Mutex
	client *SharedClient
}{}

// DefaultSharedClient returns the process-wide shared websocket client.
func DefaultSharedClient() *SharedClient {
	defaultSharedClient.Lock()
	defer defaultSharedClient.Unlock()
	if defaultSharedClient.client == nil {
		defaultSharedClient.client = NewSharedClient(EnvSharedClientConfig())
	}
	return defaultSharedClient.client
}

// SetDefaultSharedClientForTesting overrides the process singleton.
func SetDefaultSharedClientForTesting(client *SharedClient) func() {
	defaultSharedClient.Lock()
	previous := defaultSharedClient.client
	defaultSharedClient.client = client
	defaultSharedClient.Unlock()
	return func() {
		defaultSharedClient.Lock()
		defaultSharedClient.client = previous
		defaultSharedClient.Unlock()
	}
}

// Config returns a copy of the shared client config.
func (c *SharedClient) Config() SharedClientConfig {
	if c == nil {
		return NormalizeSharedClientConfig(SharedClientConfig{})
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

// Start begins the reconnect loop once.
func (c *SharedClient) Start(ctx context.Context) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.started || !c.cfg.Enabled {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.mu.Unlock()
	go c.run(ctx)
}

// Subscribe retains interest in a venue/instrument stream.
func (c *SharedClient) Subscribe(ctx context.Context, venue, instrument string) (*SharedSubscription, error) {
	if c == nil {
		return nil, ErrSignalsDisabled
	}
	venue = NormalizeVenue(venue)
	instrument = NormalizeInstrument(instrument)
	if venue == "" || instrument == "" {
		return nil, errors.New("signalsclient: subscription requires venue and instrument")
	}
	c.Start(ctx)
	key := positionKey(venue, instrument)

	var client *WebSocketSignalsClient
	c.mu.Lock()
	if !c.cfg.Enabled {
		c.mu.Unlock()
		return nil, ErrSignalsDisabled
	}
	sub := c.subscriptions[key]
	if sub == nil {
		sub = &sharedSubscriptionState{venue: venue, instrument: instrument}
		c.subscriptions[key] = sub
	}
	sub.refCount++
	client = c.client
	shouldSubscribe := c.connected && client != nil && !sub.active && !sub.requested
	if shouldSubscribe {
		sub.requested = true
	}
	c.mu.Unlock()

	if shouldSubscribe && client != nil {
		if err := client.SubscribeInstrument(ctx, venue, instrument); err != nil {
			c.clearSubscriptionRequested(venue, instrument)
			c.setLastError(err)
		}
	}
	c.notify()
	return &SharedSubscription{client: c, venue: venue, instrument: instrument}, nil
}

func (c *SharedClient) releaseSubscription(venue, instrument string) {
	venue = NormalizeVenue(venue)
	instrument = NormalizeInstrument(instrument)
	key := positionKey(venue, instrument)
	var client *WebSocketSignalsClient
	var subscriptionID int64
	var instrumentUnsubscribe bool

	c.mu.Lock()
	sub := c.subscriptions[key]
	if sub == nil {
		c.mu.Unlock()
		return
	}
	sub.refCount--
	if sub.refCount > 0 {
		c.mu.Unlock()
		return
	}
	if sub.subscriptionID > 0 {
		subscriptionID = sub.subscriptionID
	} else if sub.active || sub.requested {
		instrumentUnsubscribe = true
	}
	delete(c.subscriptions, key)
	client = c.client
	c.mu.Unlock()

	if client != nil && (subscriptionID > 0 || instrumentUnsubscribe) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var err error
		if subscriptionID > 0 {
			err = client.Unsubscribe(ctx, subscriptionID)
		} else {
			err = client.UnsubscribeInstrument(ctx, venue, instrument)
		}
		if err != nil {
			c.setLastError(err)
		}
	}
	c.notify()
}

// SignalState returns the latest signal state for one venue/instrument.
func (c *SharedClient) SignalState(venue, instrument string) (SharedSignalState, bool) {
	if c == nil {
		return SharedSignalState{}, false
	}
	venue = NormalizeVenue(venue)
	instrument = NormalizeInstrument(instrument)
	key := positionKey(venue, instrument)
	now := c.now()
	c.mu.RLock()
	state, ok := c.signals[key]
	connected := c.connected
	staleAfter := c.cfg.StaleAfter
	c.mu.RUnlock()
	if !ok {
		return SharedSignalState{}, false
	}
	state.Stale = !connected || state.UpdatedAt.IsZero() || now.Sub(state.UpdatedAt) > staleAfter
	return state, true
}

// InstrumentState returns the latest lifecycle state for one venue/instrument.
func (c *SharedClient) InstrumentState(venue, instrument string) (SharedInstrumentState, bool) {
	if c == nil {
		return SharedInstrumentState{}, false
	}
	venue = NormalizeVenue(venue)
	instrument = NormalizeInstrument(instrument)
	key := positionKey(venue, instrument)
	now := c.now()
	c.mu.RLock()
	state, ok := c.instruments[key]
	connected := c.connected
	staleAfter := c.cfg.StaleAfter
	c.mu.RUnlock()
	if !ok {
		return SharedInstrumentState{}, false
	}
	state.Disconnected = !connected
	state.Stale = !connected || state.LastUpdate.IsZero() || now.Sub(state.LastUpdate) > staleAfter
	return state, true
}

func (c *SharedClient) Connected() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *SharedClient) ConnectionCount() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connectionCount
}

func (c *SharedClient) LastError() error {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastError
}

// SubscribeUpdates returns a lightweight state-change pulse channel.
func (c *SharedClient) SubscribeUpdates(ctx context.Context) <-chan struct{} {
	out := make(chan struct{}, 1)
	if c == nil {
		close(out)
		return out
	}
	c.mu.Lock()
	c.watchers[out] = struct{}{}
	c.mu.Unlock()
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		delete(c.watchers, out)
		c.mu.Unlock()
		close(out)
	}()
	return out
}

// SubscribeEvents returns reconnect-safe event fan-out.
func (c *SharedClient) SubscribeEvents(ctx context.Context) (<-chan Event, <-chan error) {
	sub := &sharedEventSubscription{events: make(chan Event, 128), errors: make(chan error, 128)}
	if c == nil {
		close(sub.events)
		close(sub.errors)
		return sub.events, sub.errors
	}
	c.mu.Lock()
	c.eventWatchers[sub] = struct{}{}
	c.mu.Unlock()
	go func() {
		<-ctx.Done()
		c.removeEventSubscription(sub)
	}()
	return sub.events, sub.errors
}

func (c *SharedClient) removeEventSubscription(sub *sharedEventSubscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.eventWatchers[sub]; !ok {
		return
	}
	delete(c.eventWatchers, sub)
	close(sub.events)
	close(sub.errors)
}

// InjectSignal is a deterministic testing helper.
func (c *SharedClient) InjectSignal(signal Signal) {
	if c == nil {
		return
	}
	c.applySignal(SignalEvent{
		Venue:      NormalizeVenue(signal.Venue),
		Instrument: NormalizeInstrument(signal.Instrument),
		Signal:     signal,
		Timestamp:  firstTime(signal.Timestamp, c.now()),
	})
}

func (c *SharedClient) run(ctx context.Context) {
	backoff := c.cfg.ReconnectMinBackoff
	for {
		select {
		case <-ctx.Done():
			c.setConnected(false)
			return
		default:
		}
		if !c.cfg.Enabled {
			c.setLastError(ErrSignalsDisabled)
			c.sleep(ctx, backoff)
			continue
		}
		if strings.TrimSpace(c.cfg.Token) == "" {
			c.setConnected(false)
			c.setLastError(ErrSignalsNoToken)
			c.sleep(ctx, c.cfg.ReconnectMaxBackoff)
			continue
		}
		client := NewWebSocketSignalsClient(SignalsWebSocketToken(c.cfg.Token), WithURL(c.cfg.WebSocketURL))
		if err := client.Connect(ctx); err != nil {
			c.setConnected(false)
			c.broadcastSharedError(err)
			c.setLastError(err)
			c.sleep(ctx, backoff)
			backoff = minDuration(backoff*2, c.cfg.ReconnectMaxBackoff)
			continue
		}
		c.mu.Lock()
		c.client = client
		c.connected = true
		c.connectionCount++
		for _, sub := range c.subscriptions {
			sub.subscriptionID = 0
			sub.active = false
			sub.requested = false
		}
		c.lastError = nil
		c.mu.Unlock()
		c.notify()
		c.subscribeAll(ctx, client)
		backoff = c.cfg.ReconnectMinBackoff
		c.readUntilDisconnected(ctx, client)
		_ = client.Close()
		c.mu.Lock()
		if c.client == client {
			c.client = nil
			c.connected = false
			for _, sub := range c.subscriptions {
				sub.subscriptionID = 0
				sub.active = false
				sub.requested = false
			}
		}
		c.mu.Unlock()
		c.notify()
		c.sleep(ctx, backoff)
	}
}

func (c *SharedClient) subscribeAll(ctx context.Context, client *WebSocketSignalsClient) {
	c.mu.Lock()
	subs := make([]sharedSubscriptionState, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		if sub.active || sub.requested {
			continue
		}
		sub.requested = true
		subs = append(subs, *sub)
	}
	c.mu.Unlock()
	for _, sub := range subs {
		if err := client.SubscribeInstrument(ctx, sub.venue, sub.instrument); err != nil {
			c.clearSubscriptionRequested(sub.venue, sub.instrument)
			c.setLastError(err)
		}
	}
}

func (c *SharedClient) clearSubscriptionRequested(venue, instrument string) {
	c.mu.Lock()
	if sub := c.subscriptions[positionKey(venue, instrument)]; sub != nil {
		sub.requested = false
	}
	c.mu.Unlock()
}

func (c *SharedClient) readUntilDisconnected(ctx context.Context, client *WebSocketSignalsClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-client.Events():
			if !ok {
				return
			}
			c.applyEvent(ev)
		case err, ok := <-client.Errors():
			if !ok {
				return
			}
			if err != nil {
				c.broadcastSharedError(err)
				c.setLastError(err)
				return
			}
		}
	}
}

func (c *SharedClient) applyEvent(ev Event) {
	switch event := ev.(type) {
	case SubscribedEvent:
		c.mu.Lock()
		if sub := c.subscriptions[positionKey(NormalizeVenue(event.Venue), NormalizeInstrument(event.Instrument))]; sub != nil {
			sub.subscriptionID = event.SubscriptionID
			sub.active = true
			sub.requested = false
		}
		c.mu.Unlock()
	case UnsubscribedEvent:
		c.mu.Lock()
		if sub := c.subscriptions[positionKey(NormalizeVenue(event.Venue), NormalizeInstrument(event.Instrument))]; sub != nil && sub.subscriptionID == event.SubscriptionID {
			sub.subscriptionID = 0
			sub.active = false
			sub.requested = false
		}
		c.mu.Unlock()
	case InfoEvent:
		c.applyInfo(event)
	case SignalEvent:
		c.applySignal(event)
	case ErrorEvent:
		if c.applyIdempotentSubscriptionError(event) {
			break
		}
		c.setLastError(errors.New(strings.TrimSpace(event.Code + ": " + event.Message)))
	}
	c.broadcastSharedEvent(ev)
	c.notify()
}

func (c *SharedClient) applyIdempotentSubscriptionError(event ErrorEvent) bool {
	switch strings.ToLower(strings.TrimSpace(event.Code)) {
	case "duplicate_subscription", "subscription_not_found":
	default:
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sub := range c.subscriptions {
		if sub.requested && !sub.active {
			sub.active = true
			sub.requested = false
			return true
		}
	}
	return true
}

func (c *SharedClient) applyInfo(event InfoEvent) {
	venue := NormalizeVenue(event.Venue)
	instrument := NormalizeInstrument(event.Instrument)
	if venue == "" || instrument == "" {
		return
	}
	ts := firstTime(event.Timestamp, c.now())
	c.mu.Lock()
	state := c.instruments[positionKey(venue, instrument)]
	state.Venue = venue
	state.Instrument = instrument
	state.Stage = event.Stage
	state.Message = event.Message
	state.LastUpdate = ts
	state.Disconnected = !c.connected
	c.instruments[positionKey(venue, instrument)] = state
	c.mu.Unlock()
}

func (c *SharedClient) applySignal(event SignalEvent) {
	venue := NormalizeVenue(firstNonEmpty(event.Signal.Venue, event.Venue))
	instrument := NormalizeInstrument(firstNonEmpty(event.Signal.Instrument, event.Instrument))
	if venue == "" || instrument == "" {
		return
	}
	ts := firstTime(event.Signal.Timestamp, event.Timestamp, c.now())
	signal := event.Signal
	direction := strings.ToLower(strings.TrimSpace(string(signal.Side)))
	side := sideSign(signal.Side)
	confidence := clamp01(signal.Confidence)
	signedScore := signal.Score
	if signedScore == 0 {
		signedScore = side * confidence
	}
	state := SharedSignalState{
		Venue:                  venue,
		Instrument:             instrument,
		Direction:              direction,
		SignedScore:            signedScore,
		Confidence:             confidence,
		TargetExposure:         side * confidence,
		TakeProfit:             signal.TakeProfit,
		StopLoss:               signal.StopLoss,
		TrailingStopActivation: signal.TrailingStopActivation,
		TrailingStopDistance:   signal.TrailingStopDistance,
		TrailingStopMinProfit:  signal.TrailingStopMinProfit,
		Price:                  signal.Price,
		RiskMetadata: map[string]any{
			"score":                  signal.Score,
			"confidence":             confidence,
			"side":                   direction,
			"trailingStopActivation": signal.TrailingStopActivation,
			"trailingStopDistance":   signal.TrailingStopDistance,
			"trailingStopMinProfit":  signal.TrailingStopMinProfit,
		},
		TimeframeComponents: signal.Components,
		UpdatedAt:           ts,
		Replay:              event.Replay,
	}
	key := positionKey(venue, instrument)
	c.mu.Lock()
	c.signals[key] = state
	instrumentState := c.instruments[key]
	instrumentState.Venue = venue
	instrumentState.Instrument = instrument
	if signal.Price > 0 {
		instrumentState.LatestTickerPrice = signal.Price
	}
	instrumentState.LastUpdate = ts
	instrumentState.Disconnected = !c.connected
	c.instruments[key] = instrumentState
	c.mu.Unlock()
}

func (c *SharedClient) setConnected(connected bool) {
	c.mu.Lock()
	c.connected = connected
	c.mu.Unlock()
	c.notify()
}

func (c *SharedClient) setLastError(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	c.lastError = err
	c.mu.Unlock()
	log.Printf("signals websocket: %v", err)
	c.notify()
}

func (c *SharedClient) notify() {
	c.mu.RLock()
	watchers := make([]chan struct{}, 0, len(c.watchers))
	for ch := range c.watchers {
		watchers = append(watchers, ch)
	}
	c.mu.RUnlock()
	for _, ch := range watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (c *SharedClient) broadcastSharedEvent(ev Event) {
	c.mu.RLock()
	watchers := make([]*sharedEventSubscription, 0, len(c.eventWatchers))
	for sub := range c.eventWatchers {
		watchers = append(watchers, sub)
	}
	c.mu.RUnlock()
	for _, sub := range watchers {
		select {
		case sub.events <- ev:
		default:
		}
	}
}

func (c *SharedClient) broadcastSharedError(err error) {
	c.mu.RLock()
	watchers := make([]*sharedEventSubscription, 0, len(c.eventWatchers))
	for sub := range c.eventWatchers {
		watchers = append(watchers, sub)
	}
	c.mu.RUnlock()
	for _, sub := range watchers {
		select {
		case sub.errors <- err:
		default:
		}
	}
}

func (c *SharedClient) sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (c *SharedClient) now() time.Time {
	if c == nil || c.cfg.Now == nil {
		return time.Now().UTC()
	}
	return c.cfg.Now().UTC()
}

// NormalizeVenue returns the canonical lower-case venue id.
func NormalizeVenue(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// NormalizeInstrument returns the canonical upper-case instrument id.
func NormalizeInstrument(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	if strings.Contains(value, "-") {
		return value
	}
	if strings.HasSuffix(value, "USDT") && !strings.HasSuffix(value, "-SWAP") {
		return strings.TrimSuffix(value, "USDT") + "-USDT-SWAP"
	}
	return value
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	if parsed, err := strconv.ParseBool(value); err == nil {
		return parsed
	}
	switch strings.ToLower(value) {
	case "1", "yes", "y", "on", "enabled":
		return true
	case "0", "no", "n", "off", "disabled":
		return false
	default:
		return fallback
	}
}

func firstTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value.UTC()
		}
	}
	return time.Time{}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// WebSocketURLFromBaseURL converts an HTTP(S) base URL into its /ws endpoint.
func WebSocketURLFromBaseURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = "/ws"
	u.RawQuery = ""
	return u.String()
}
