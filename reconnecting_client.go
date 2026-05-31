package signalsclient

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ReconnectingSignalsClient shares one websocket across multiple
// SignalsManager instances and replays active basket subscriptions after a
// reconnect.
type ReconnectingSignalsClient struct {
	token SignalsWebSocketToken
	opts  []ClientOption
	cfg   clientConfig

	mu            sync.RWMutex
	started       bool
	closed        bool
	transport     *WebSocketSignalsClient
	nextLocalID   int64
	subscriptions map[int64]SubscribeRequest
	localToServer map[int64]int64
	serverToLocal map[int64]int64
	eventWatchers map[*eventSubscription]struct{}
}

func NewReconnectingSignalsClient(token SignalsWebSocketToken, opts ...ClientOption) *ReconnectingSignalsClient {
	cfg := clientConfig{
		url:        defaultWebSocketURL,
		header:     make(http.Header),
		dialer:     websocket.DefaultDialer,
		bufferSize: 128,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &ReconnectingSignalsClient{
		token:         token,
		opts:          append([]ClientOption(nil), opts...),
		cfg:           cfg,
		subscriptions: make(map[int64]SubscribeRequest),
		localToServer: make(map[int64]int64),
		serverToLocal: make(map[int64]int64),
		eventWatchers: make(map[*eventSubscription]struct{}),
	}
}

func (c *ReconnectingSignalsClient) Subscribe(ctx context.Context, request SubscribeRequest) (int64, error) {
	if c == nil {
		return 0, errors.New("signalsclient: nil client")
	}
	request = request.normalized()
	c.mu.Lock()
	c.nextLocalID++
	localID := c.nextLocalID
	c.subscriptions[localID] = request
	transport := c.transport
	if !c.started {
		c.started = true
		go c.run(context.Background())
	}
	c.mu.Unlock()
	if transport != nil {
		if _, err := transport.Subscribe(ctx, request); err != nil {
			c.mu.Lock()
			delete(c.subscriptions, localID)
			delete(c.localToServer, localID)
			c.mu.Unlock()
			return 0, err
		}
	}
	return localID, nil
}

func (c *ReconnectingSignalsClient) UpdateAsset(ctx context.Context, subscriptionID int64, asset AssetSnapshot) error {
	if transport, serverID := c.transportFor(subscriptionID); transport != nil && serverID > 0 {
		return transport.UpdateAsset(ctx, serverID, asset)
	}
	return nil
}

func (c *ReconnectingSignalsClient) UpdatePosition(ctx context.Context, subscriptionID int64, position Position) error {
	if transport, serverID := c.transportFor(subscriptionID); transport != nil && serverID > 0 {
		return transport.UpdatePosition(ctx, serverID, position)
	}
	return nil
}

func (c *ReconnectingSignalsClient) AddInstrument(ctx context.Context, subscriptionID int64, instrument string) error {
	c.mu.Lock()
	if req, ok := c.subscriptions[subscriptionID]; ok {
		req.Instruments = normalizeInstrumentList(append(req.Instruments, instrument))
		c.subscriptions[subscriptionID] = req
	}
	transport := c.transport
	serverID := c.localToServer[subscriptionID]
	c.mu.Unlock()
	if transport != nil && serverID > 0 {
		return transport.AddInstrument(ctx, serverID, instrument)
	}
	return nil
}

func (c *ReconnectingSignalsClient) RemoveInstrument(ctx context.Context, subscriptionID int64, instrument string) error {
	instrument = NormalizeInstrument(instrument)
	c.mu.Lock()
	if req, ok := c.subscriptions[subscriptionID]; ok {
		next := make([]string, 0, len(req.Instruments))
		for _, current := range req.Instruments {
			if current != instrument {
				next = append(next, current)
			}
		}
		req.Instruments = next
		c.subscriptions[subscriptionID] = req
	}
	transport := c.transport
	serverID := c.localToServer[subscriptionID]
	c.mu.Unlock()
	if transport != nil && serverID > 0 {
		return transport.RemoveInstrument(ctx, serverID, instrument)
	}
	return nil
}

func (c *ReconnectingSignalsClient) UpdateConfig(ctx context.Context, subscriptionID int64, cfg RuntimeConfig) error {
	cfg.ProfitWithdrawRatio = clamp01(cfg.ProfitWithdrawRatio)
	c.mu.Lock()
	if req, ok := c.subscriptions[subscriptionID]; ok {
		req.ProfitWithdrawRatio = cfg.ProfitWithdrawRatio
		req.Risk.ProfitWithdrawRatio = cfg.ProfitWithdrawRatio
		c.subscriptions[subscriptionID] = req
	}
	transport := c.transport
	serverID := c.localToServer[subscriptionID]
	c.mu.Unlock()
	if transport != nil && serverID > 0 {
		return transport.UpdateConfig(ctx, serverID, cfg)
	}
	return nil
}

func (c *ReconnectingSignalsClient) ScheduleWithdrawal(ctx context.Context, subscriptionID int64, withdrawal WithdrawalRequest) error {
	if transport, serverID := c.transportFor(subscriptionID); transport != nil && serverID > 0 {
		return transport.ScheduleWithdrawal(ctx, serverID, withdrawal)
	}
	return nil
}

func (c *ReconnectingSignalsClient) Unsubscribe(ctx context.Context, subscriptionID int64) error {
	c.mu.Lock()
	delete(c.subscriptions, subscriptionID)
	transport := c.transport
	serverID := c.localToServer[subscriptionID]
	delete(c.localToServer, subscriptionID)
	if serverID > 0 {
		delete(c.serverToLocal, serverID)
	}
	c.mu.Unlock()
	if transport != nil && serverID > 0 {
		return transport.Unsubscribe(ctx, serverID)
	}
	return nil
}

func (c *ReconnectingSignalsClient) SubscribeEvents(ctx context.Context) (<-chan Event, <-chan error) {
	sub := &eventSubscription{events: make(chan Event, c.cfg.bufferSize), errors: make(chan error, c.cfg.bufferSize)}
	c.mu.Lock()
	c.eventWatchers[sub] = struct{}{}
	if !c.started {
		c.started = true
		go c.run(context.Background())
	}
	c.mu.Unlock()
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		if _, ok := c.eventWatchers[sub]; ok {
			delete(c.eventWatchers, sub)
			close(sub.events)
			close(sub.errors)
		}
		c.mu.Unlock()
	}()
	return sub.events, sub.errors
}

func (c *ReconnectingSignalsClient) Close() {
	c.mu.Lock()
	c.closed = true
	transport := c.transport
	c.transport = nil
	c.mu.Unlock()
	if transport != nil {
		_ = transport.Close()
	}
}

func (c *ReconnectingSignalsClient) transportFor(localID int64) (*WebSocketSignalsClient, int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.transport, c.localToServer[localID]
}

func (c *ReconnectingSignalsClient) run(ctx context.Context) {
	backoff := time.Second
	for {
		c.mu.RLock()
		closed := c.closed
		c.mu.RUnlock()
		if closed {
			return
		}
		transport := NewWebSocketSignalsClient(c.token, c.opts...)
		if err := transport.Connect(ctx); err != nil {
			c.broadcastError(err)
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}
		c.mu.Lock()
		c.transport = transport
		c.localToServer = make(map[int64]int64)
		c.serverToLocal = make(map[int64]int64)
		requests := make(map[int64]SubscribeRequest, len(c.subscriptions))
		for id, req := range c.subscriptions {
			requests[id] = req
		}
		c.mu.Unlock()
		for _, req := range requests {
			if _, err := transport.Subscribe(ctx, req); err != nil {
				c.broadcastError(err)
			}
		}
		backoff = time.Second
		c.readTransport(ctx, transport)
		_ = transport.Close()
		c.mu.Lock()
		if c.transport == transport {
			c.transport = nil
			c.localToServer = make(map[int64]int64)
			c.serverToLocal = make(map[int64]int64)
		}
		c.mu.Unlock()
		time.Sleep(backoff)
	}
}

func (c *ReconnectingSignalsClient) readTransport(ctx context.Context, transport *WebSocketSignalsClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-transport.Events():
			if !ok {
				return
			}
			c.applyTransportEvent(event)
		case err, ok := <-transport.Errors():
			if !ok {
				return
			}
			if err != nil {
				c.broadcastError(err)
				log.Printf("signals websocket: %v", err)
				return
			}
		}
	}
}

func (c *ReconnectingSignalsClient) applyTransportEvent(event Event) {
	switch ev := event.(type) {
	case SubscribedEvent:
		c.mu.Lock()
		localID := int64(0)
		for id, req := range c.subscriptions {
			if requestMatchesSubscribeEvent(req, ev) && c.localToServer[id] == 0 {
				localID = id
				break
			}
		}
		if localID > 0 {
			c.localToServer[localID] = ev.SubscriptionID
			c.serverToLocal[ev.SubscriptionID] = localID
			ev.SubscriptionID = localID
			event = ev
		}
		c.mu.Unlock()
	case UnsubscribedEvent:
		c.mu.Lock()
		if localID := c.serverToLocal[ev.SubscriptionID]; localID > 0 {
			delete(c.localToServer, localID)
			delete(c.serverToLocal, ev.SubscriptionID)
			ev.SubscriptionID = localID
			event = ev
		}
		c.mu.Unlock()
	case CreateMarketOrderEvent:
		c.mu.RLock()
		localID := c.serverToLocal[ev.SubscriptionID]
		c.mu.RUnlock()
		if localID > 0 {
			ev.SubscriptionID = localID
			event = ev
		}
	case UpdateTPSLEvent:
		c.mu.RLock()
		localID := c.serverToLocal[ev.SubscriptionID]
		c.mu.RUnlock()
		if localID > 0 {
			ev.SubscriptionID = localID
			event = ev
		}
	case WithdrawEvent:
		c.mu.RLock()
		localID := c.serverToLocal[ev.SubscriptionID]
		c.mu.RUnlock()
		if localID > 0 {
			ev.SubscriptionID = localID
			event = ev
		}
	}
	c.broadcastEvent(event)
}

func requestMatchesSubscribeEvent(req SubscribeRequest, ev SubscribedEvent) bool {
	if NormalizeVenue(ev.Venue) != NormalizeVenue(req.Venue) {
		return false
	}
	if ev.Instrument != "" {
		target := NormalizeInstrument(ev.Instrument)
		for _, instrument := range req.Instruments {
			if instrument == target {
				return true
			}
		}
		return false
	}
	return true
}

func (c *ReconnectingSignalsClient) broadcastEvent(event Event) {
	c.mu.RLock()
	watchers := make([]*eventSubscription, 0, len(c.eventWatchers))
	for watcher := range c.eventWatchers {
		watchers = append(watchers, watcher)
	}
	c.mu.RUnlock()
	for _, watcher := range watchers {
		select {
		case watcher.events <- event:
		default:
		}
	}
}

func (c *ReconnectingSignalsClient) broadcastError(err error) {
	if err == nil {
		return
	}
	c.mu.RLock()
	watchers := make([]*eventSubscription, 0, len(c.eventWatchers))
	for watcher := range c.eventWatchers {
		watchers = append(watchers, watcher)
	}
	c.mu.RUnlock()
	for _, watcher := range watchers {
		select {
		case watcher.errors <- err:
		default:
		}
	}
}
