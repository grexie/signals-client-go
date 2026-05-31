package signalsclient

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultSignalsManagerBuffer = 128
)

// SubscribeRequest is the public websocket basket subscription request for the
// Bollinger HTF router. Legacy per-instrument signal subscriptions are not
// exposed through SignalsManager.
type SubscribeRequest struct {
	Type                string          `json:"type,omitempty"`
	Venue               string          `json:"venue"`
	Instruments         []string        `json:"instruments"`
	Mode                string          `json:"mode,omitempty"`
	Risk                RiskConfig      `json:"risk,omitempty"`
	ProfitWithdrawRatio float64         `json:"profitWithdrawRatio,omitempty"`
	Assets              []AssetSnapshot `json:"assets,omitempty"`
	Positions           []Position      `json:"positions,omitempty"`
}

func (r SubscribeRequest) normalized() SubscribeRequest {
	r.Type = "subscribe"
	r.Venue = NormalizeVenue(firstNonEmpty(r.Venue, "okx"))
	r.Instruments = normalizeInstrumentList(r.Instruments)
	r.Mode = strings.TrimSpace(r.Mode)
	for i := range r.Assets {
		r.Assets[i].Venue = firstNonEmpty(r.Assets[i].Venue, r.Venue)
	}
	for i := range r.Positions {
		r.Positions[i].Venue = firstNonEmpty(r.Positions[i].Venue, r.Venue)
	}
	return r
}

// RiskConfig configures basket-level capital and switching constraints.
type RiskConfig struct {
	MaxMarginRatio         float64 `json:"maxMarginRatio,omitempty"`
	MaxConcurrentPositions int     `json:"maxConcurrentPositions,omitempty"`
	MaxDrawdown            float64 `json:"maxDrawdown,omitempty"`
	SwitchBuffer           float64 `json:"switchBuffer,omitempty"`
	MinLeverage            float64 `json:"minLeverage,omitempty"`
	MaxLeverage            float64 `json:"maxLeverage,omitempty"`
	ProfitWithdrawRatio    float64 `json:"profitWithdrawRatio,omitempty"`
}

type RuntimeConfig struct {
	ProfitWithdrawRatio float64 `json:"profitWithdrawRatio,omitempty"`
}

type WithdrawalRequest struct {
	Venue    string  `json:"venue,omitempty"`
	Currency string  `json:"currency"`
	Amount   float64 `json:"amount"`
	Reason   string  `json:"reason,omitempty"`
}

// SignalsManagerConfig configures one Bollinger-router basket subscription.
type SignalsManagerConfig struct {
	Venue               string
	Instruments         []string
	Mode                string
	Risk                RiskConfig
	ProfitWithdrawRatio float64
	BufferSize          int
}

// SignalsManagerState is durable state passed into NewSignalsManager.
type SignalsManagerState struct {
	Assets    []AssetSnapshot `json:"assets,omitempty"`
	Positions []Position      `json:"positions,omitempty"`
}

// Intent is a server-directed order intent for the client venue executor.
type Intent struct {
	SubscriptionID  int64
	IntentID        string
	Action          string
	Reason          string
	Venue           string
	Instrument      string
	Side            Side
	OrderType       string
	ContractSize    float64
	Leverage        float64
	ReduceOnly      bool
	TakeProfit      float64
	StopLoss        float64
	TakeProfitPrice float64
	StopLossPrice   float64
	Timestamp       time.Time
}

// SignalsManager owns one Bollinger-router basket subscription. Multiple
// managers can share one SignalsClient transport; each keeps its own
// subscription id and state.
type SignalsManager struct {
	client SignalsClient
	cfg    SignalsManagerConfig

	mu             sync.RWMutex
	subscriptionID int64
	buffer         int
	assets         map[string]AssetSnapshot
	positions      map[string]Position
	intents        chan Intent
	protections    chan UpdateTPSLEvent
	withdrawals    chan WithdrawEvent
	backtests      chan BacktestEvent
	messages       chan InfoEvent
	events         chan Event
	errors         chan error

	intentSubscribers     map[chan Intent]struct{}
	protectionSubscribers map[chan UpdateTPSLEvent]struct{}
	withdrawalSubscribers map[chan WithdrawEvent]struct{}
	backtestSubscribers   map[chan BacktestEvent]struct{}
	messageSubscribers    map[chan InfoEvent]struct{}
	eventSubscribers      map[chan Event]struct{}
	errorSubscribers      map[chan error]struct{}
}

func NewSignalsManager(client SignalsClient, state SignalsManagerState, cfg SignalsManagerConfig) *SignalsManager {
	cfg = normalizeSignalsManagerConfig(cfg)
	buffer := cfg.BufferSize
	if buffer <= 0 {
		buffer = DefaultSignalsManagerBuffer
	}
	manager := &SignalsManager{
		client:      client,
		cfg:         cfg,
		buffer:      buffer,
		assets:      make(map[string]AssetSnapshot),
		positions:   make(map[string]Position),
		intents:     make(chan Intent, buffer),
		protections: make(chan UpdateTPSLEvent, buffer),
		withdrawals: make(chan WithdrawEvent, buffer),
		backtests:   make(chan BacktestEvent, buffer),
		messages:    make(chan InfoEvent, buffer),
		events:      make(chan Event, buffer),
		errors:      make(chan error, buffer),

		intentSubscribers:     make(map[chan Intent]struct{}),
		protectionSubscribers: make(map[chan UpdateTPSLEvent]struct{}),
		withdrawalSubscribers: make(map[chan WithdrawEvent]struct{}),
		backtestSubscribers:   make(map[chan BacktestEvent]struct{}),
		messageSubscribers:    make(map[chan InfoEvent]struct{}),
		eventSubscribers:      make(map[chan Event]struct{}),
		errorSubscribers:      make(map[chan error]struct{}),
	}
	for _, asset := range state.Assets {
		manager.UpdateAsset(asset)
	}
	for _, position := range state.Positions {
		manager.UpdatePosition(position)
	}
	return manager
}

func normalizeSignalsManagerConfig(cfg SignalsManagerConfig) SignalsManagerConfig {
	cfg.Venue = NormalizeVenue(firstNonEmpty(cfg.Venue, "okx"))
	cfg.Instruments = normalizeInstrumentList(cfg.Instruments)
	cfg.Mode = strings.TrimSpace(cfg.Mode)
	if cfg.Risk.MinLeverage < 0 {
		cfg.Risk.MinLeverage = 0
	}
	if cfg.Risk.MaxLeverage < 0 {
		cfg.Risk.MaxLeverage = 0
	}
	return cfg
}

func (m *SignalsManager) Run(ctx context.Context) error {
	if m == nil || m.client == nil {
		return errors.New("signalsclient: SignalsManager requires a SignalsClient")
	}
	events, errs := m.client.SubscribeEvents(ctx)
	if err := m.subscribe(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			m.unsubscribeOnStop(ctx)
			return ctx.Err()
		case err, ok := <-errs:
			if !ok {
				if ctx.Err() != nil {
					m.unsubscribeOnStop(ctx)
					return ctx.Err()
				}
				return errors.New("signalsclient: event error stream closed")
			}
			if err != nil {
				m.sendError(err)
			}
		case event, ok := <-events:
			if !ok {
				if ctx.Err() != nil {
					m.unsubscribeOnStop(ctx)
					return ctx.Err()
				}
				return errors.New("signalsclient: event stream closed")
			}
			m.handleEvent(ctx, event)
		}
	}
}

func (m *SignalsManager) Subscribe(ctx context.Context) error {
	return m.subscribe(ctx)
}

func (m *SignalsManager) subscribe(ctx context.Context) error {
	request := SubscribeRequest{
		Venue:               m.cfg.Venue,
		Instruments:         m.cfg.Instruments,
		Mode:                m.cfg.Mode,
		Risk:                m.cfg.Risk,
		ProfitWithdrawRatio: m.cfg.ProfitWithdrawRatio,
		Assets:              m.Assets(),
		Positions:           m.Positions(),
	}
	subscriptionID, err := m.client.Subscribe(ctx, request)
	if err != nil {
		return err
	}
	if subscriptionID > 0 {
		m.mu.Lock()
		m.subscriptionID = subscriptionID
		m.mu.Unlock()
	}
	return nil
}

func (m *SignalsManager) unsubscribe(ctx context.Context) {
	m.mu.RLock()
	id := m.subscriptionID
	m.mu.RUnlock()
	if id > 0 {
		_ = m.client.Unsubscribe(ctx, id)
	}
}

func (m *SignalsManager) unsubscribeOnStop(parent context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer cancel()
	m.unsubscribe(ctx)
}

// Intents returns the legacy single-consumer intent stream. Prefer
// SubscribeIntents for fan-out listeners.
func (m *SignalsManager) Intents() <-chan Intent { return m.intents }

// ProtectionUpdates returns the legacy single-consumer TP/SL update stream.
// Prefer SubscribeProtectionUpdates for fan-out listeners.
func (m *SignalsManager) ProtectionUpdates() <-chan UpdateTPSLEvent { return m.protections }

// Withdrawals returns the legacy single-consumer withdrawal stream. Prefer
// SubscribeWithdrawals for fan-out listeners.
func (m *SignalsManager) Withdrawals() <-chan WithdrawEvent { return m.withdrawals }

// Backtests returns the legacy single-consumer backtest stream. Prefer
// SubscribeBacktests for fan-out listeners.
func (m *SignalsManager) Backtests() <-chan BacktestEvent { return m.backtests }

// Messages returns the legacy single-consumer info-message stream. Prefer
// SubscribeMessages for fan-out listeners.
func (m *SignalsManager) Messages() <-chan InfoEvent { return m.messages }

// Events returns the legacy single-consumer manager event stream. Prefer
// SubscribeManagerEvents for fan-out listeners.
func (m *SignalsManager) Events() <-chan Event { return m.events }

// Errors returns the legacy single-consumer manager error stream. Prefer
// SubscribeErrors for fan-out listeners.
func (m *SignalsManager) Errors() <-chan error { return m.errors }

func (m *SignalsManager) SubscribeIntents(ctx context.Context) <-chan Intent {
	ch := make(chan Intent, m.subscriberBuffer())
	m.mu.Lock()
	m.intentSubscribers[ch] = struct{}{}
	m.mu.Unlock()
	go m.removeIntentSubscriber(ctx, ch)
	return ch
}

func (m *SignalsManager) SubscribeProtectionUpdates(ctx context.Context) <-chan UpdateTPSLEvent {
	ch := make(chan UpdateTPSLEvent, m.subscriberBuffer())
	m.mu.Lock()
	m.protectionSubscribers[ch] = struct{}{}
	m.mu.Unlock()
	go m.removeProtectionSubscriber(ctx, ch)
	return ch
}

func (m *SignalsManager) SubscribeWithdrawals(ctx context.Context) <-chan WithdrawEvent {
	ch := make(chan WithdrawEvent, m.subscriberBuffer())
	m.mu.Lock()
	m.withdrawalSubscribers[ch] = struct{}{}
	m.mu.Unlock()
	go m.removeWithdrawalSubscriber(ctx, ch)
	return ch
}

func (m *SignalsManager) SubscribeBacktests(ctx context.Context) <-chan BacktestEvent {
	ch := make(chan BacktestEvent, m.subscriberBuffer())
	m.mu.Lock()
	m.backtestSubscribers[ch] = struct{}{}
	m.mu.Unlock()
	go m.removeBacktestSubscriber(ctx, ch)
	return ch
}

func (m *SignalsManager) SubscribeMessages(ctx context.Context) <-chan InfoEvent {
	ch := make(chan InfoEvent, m.subscriberBuffer())
	m.mu.Lock()
	m.messageSubscribers[ch] = struct{}{}
	m.mu.Unlock()
	go m.removeMessageSubscriber(ctx, ch)
	return ch
}

func (m *SignalsManager) SubscribeManagerEvents(ctx context.Context) <-chan Event {
	ch := make(chan Event, m.subscriberBuffer())
	m.mu.Lock()
	m.eventSubscribers[ch] = struct{}{}
	m.mu.Unlock()
	go m.removeEventSubscriber(ctx, ch)
	return ch
}

func (m *SignalsManager) SubscribeErrors(ctx context.Context) <-chan error {
	ch := make(chan error, m.subscriberBuffer())
	m.mu.Lock()
	m.errorSubscribers[ch] = struct{}{}
	m.mu.Unlock()
	go m.removeErrorSubscriber(ctx, ch)
	return ch
}

func (m *SignalsManager) SubscriptionID() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.subscriptionID
}

func (m *SignalsManager) UpdateAsset(asset AssetSnapshot) {
	if m == nil {
		return
	}
	asset.Venue = NormalizeVenue(firstNonEmpty(asset.Venue, m.cfg.Venue))
	asset.Currency = strings.ToUpper(strings.TrimSpace(asset.Currency))
	asset.MaxUsage = clamp01(positiveOr(asset.MaxUsage, 1))
	if asset.UpdatedAt.IsZero() {
		asset.UpdatedAt = time.Now().UTC()
	}
	if asset.Currency == "" {
		return
	}
	m.mu.Lock()
	m.assets[asset.Currency] = asset
	subscriptionID := m.subscriptionID
	m.mu.Unlock()
	if subscriptionID > 0 && m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := m.client.UpdateAsset(ctx, subscriptionID, asset); err != nil {
			m.sendError(err)
		}
	}
}

func (m *SignalsManager) UpdatePosition(position Position) {
	if m == nil {
		return
	}
	position.Venue = NormalizeVenue(firstNonEmpty(position.Venue, m.cfg.Venue))
	position.Instrument = NormalizeInstrument(position.Instrument)
	position.Status = strings.ToLower(strings.TrimSpace(position.Status))
	if position.Status == "" && math.Abs(position.Size) > floatTolerance {
		position.Status = "open"
	}
	if position.LastPrice <= 0 {
		position.LastPrice = position.EntryPrice
	}
	if position.Instrument == "" {
		return
	}
	key := positionKey(position.Venue, position.Instrument)
	m.mu.Lock()
	if position.Status == "closed" || math.Abs(position.Size) <= floatTolerance {
		if position.Status == "" {
			position.Status = "closed"
		}
		delete(m.positions, key)
	} else {
		m.positions[key] = position
	}
	subscriptionID := m.subscriptionID
	m.mu.Unlock()
	if subscriptionID > 0 && m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := m.client.UpdatePosition(ctx, subscriptionID, position); err != nil {
			m.sendError(err)
		}
	}
}

func (m *SignalsManager) AddInstrument(ctx context.Context, instrument string) error {
	instrument = NormalizeInstrument(instrument)
	if instrument == "" {
		return nil
	}
	m.mu.Lock()
	m.cfg.Instruments = normalizeInstrumentList(append(m.cfg.Instruments, instrument))
	subscriptionID := m.subscriptionID
	m.mu.Unlock()
	if subscriptionID > 0 {
		return m.client.AddInstrument(ctx, subscriptionID, instrument)
	}
	return nil
}

func (m *SignalsManager) RemoveInstrument(ctx context.Context, instrument string) error {
	instrument = NormalizeInstrument(instrument)
	if instrument == "" {
		return nil
	}
	m.mu.Lock()
	next := make([]string, 0, len(m.cfg.Instruments))
	for _, current := range m.cfg.Instruments {
		if current != instrument {
			next = append(next, current)
		}
	}
	m.cfg.Instruments = next
	subscriptionID := m.subscriptionID
	m.mu.Unlock()
	if subscriptionID > 0 {
		return m.client.RemoveInstrument(ctx, subscriptionID, instrument)
	}
	return nil
}

func (m *SignalsManager) UpdateConfig(ctx context.Context, cfg RuntimeConfig) error {
	if m == nil {
		return nil
	}
	cfg.ProfitWithdrawRatio = clamp01(cfg.ProfitWithdrawRatio)
	m.mu.Lock()
	m.cfg.ProfitWithdrawRatio = cfg.ProfitWithdrawRatio
	subscriptionID := m.subscriptionID
	m.mu.Unlock()
	if subscriptionID > 0 {
		return m.client.UpdateConfig(ctx, subscriptionID, cfg)
	}
	return nil
}

func (m *SignalsManager) ScheduleWithdrawal(ctx context.Context, withdrawal WithdrawalRequest) error {
	if m == nil {
		return nil
	}
	withdrawal.Venue = NormalizeVenue(firstNonEmpty(withdrawal.Venue, m.cfg.Venue))
	withdrawal.Currency = strings.ToUpper(strings.TrimSpace(withdrawal.Currency))
	if withdrawal.Currency == "" || withdrawal.Amount <= 0 {
		return errors.New("signalsclient: withdrawal requires currency and positive amount")
	}
	m.mu.RLock()
	subscriptionID := m.subscriptionID
	m.mu.RUnlock()
	if subscriptionID <= 0 {
		return errors.New("signalsclient: basket is not subscribed")
	}
	return m.client.ScheduleWithdrawal(ctx, subscriptionID, withdrawal)
}

func (m *SignalsManager) Assets() []AssetSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]AssetSnapshot, 0, len(m.assets))
	for _, asset := range m.assets {
		out = append(out, asset)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Currency < out[j].Currency })
	return out
}

func (m *SignalsManager) Positions() []Position {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Position, 0, len(m.positions))
	for _, position := range m.positions {
		out = append(out, position)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Venue == out[j].Venue {
			return out[i].Instrument < out[j].Instrument
		}
		return out[i].Venue < out[j].Venue
	})
	return out
}

func (m *SignalsManager) AvailableOrderCash(currency string) float64 {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	m.mu.RLock()
	asset := m.assets[currency]
	m.mu.RUnlock()
	return math.Max(0, asset.Available) * clamp01(positiveOr(asset.MaxUsage, 1))
}

func (m *SignalsManager) State() SignalsManagerState {
	return SignalsManagerState{Assets: m.Assets(), Positions: m.Positions()}
}

func (m *SignalsManager) handleEvent(ctx context.Context, event Event) {
	if !m.acceptsEvent(event) {
		return
	}
	switch ev := event.(type) {
	case SubscribedEvent:
		if ev.SubscriptionID <= 0 {
			break
		}
		m.mu.Lock()
		m.subscriptionID = ev.SubscriptionID
		m.mu.Unlock()
		for _, asset := range m.Assets() {
			if err := m.client.UpdateAsset(ctx, ev.SubscriptionID, asset); err != nil {
				m.sendError(err)
			}
		}
		for _, position := range m.Positions() {
			if err := m.client.UpdatePosition(ctx, ev.SubscriptionID, position); err != nil {
				m.sendError(err)
			}
		}
	case UnsubscribedEvent:
		m.mu.Lock()
		if m.subscriptionID == ev.SubscriptionID {
			m.subscriptionID = 0
		}
		m.mu.Unlock()
	case CreateMarketOrderEvent:
		m.sendIntent(Intent{
			SubscriptionID:  ev.SubscriptionID,
			IntentID:        ev.IntentID,
			Action:          ev.Action,
			Reason:          ev.Reason,
			Venue:           ev.Venue,
			Instrument:      ev.Instrument,
			Side:            ev.Side,
			OrderType:       ev.OrderType,
			ContractSize:    ev.ContractSize,
			Leverage:        ev.Leverage,
			ReduceOnly:      ev.ReduceOnly,
			TakeProfit:      ev.TakeProfit,
			StopLoss:        ev.StopLoss,
			TakeProfitPrice: ev.TakeProfitPrice,
			StopLossPrice:   ev.StopLossPrice,
			Timestamp:       ev.Timestamp,
		})
	case UpdateTPSLEvent:
		m.applyTPSLUpdate(ev)
		m.sendProtectionUpdate(ev)
	case WithdrawEvent:
		m.sendWithdrawal(ev)
	case BacktestEvent:
		m.sendBacktest(ev)
	case InfoEvent:
		m.sendMessage(ev)
	}
	m.sendEvent(event)
}

func (m *SignalsManager) acceptsEvent(event Event) bool {
	if m == nil || event == nil {
		return false
	}
	subscriptionID := eventSubscriptionID(event)
	m.mu.RLock()
	currentSubscriptionID := m.subscriptionID
	cfg := m.cfg
	m.mu.RUnlock()
	if currentSubscriptionID > 0 && subscriptionID > 0 {
		return subscriptionID == currentSubscriptionID
	}
	switch ev := event.(type) {
	case SubscribedEvent:
		return subscribeEventMatchesConfig(cfg, ev)
	case InfoEvent:
		return instrumentInConfig(cfg, ev.Venue, ev.Instrument)
	case BacktestEvent:
		return ev.Venue == "" || NormalizeVenue(ev.Venue) == NormalizeVenue(cfg.Venue)
	case SignalEvent:
		return instrumentInConfig(cfg, ev.Venue, ev.Instrument)
	case CreateMarketOrderEvent:
		return instrumentInConfig(cfg, ev.Venue, ev.Instrument)
	case UpdateTPSLEvent:
		return instrumentInConfig(cfg, ev.Venue, ev.Instrument)
	case WithdrawEvent:
		return true
	default:
		return true
	}
}

func eventSubscriptionID(event Event) int64 {
	switch ev := event.(type) {
	case SubscribedEvent:
		return ev.SubscriptionID
	case UnsubscribedEvent:
		return ev.SubscriptionID
	case InfoEvent:
		return ev.SubscriptionID
	case BacktestEvent:
		return ev.SubscriptionID
	case SignalEvent:
		return ev.SubscriptionID
	case CreateMarketOrderEvent:
		return ev.SubscriptionID
	case UpdateTPSLEvent:
		return ev.SubscriptionID
	case WithdrawEvent:
		return ev.SubscriptionID
	default:
		return 0
	}
}

func subscribeEventMatchesConfig(cfg SignalsManagerConfig, event SubscribedEvent) bool {
	if NormalizeVenue(event.Venue) != NormalizeVenue(cfg.Venue) {
		return false
	}
	if event.Instrument == "" {
		return true
	}
	return instrumentInConfig(cfg, event.Venue, event.Instrument)
}

func instrumentInConfig(cfg SignalsManagerConfig, venue, instrument string) bool {
	if NormalizeVenue(venue) != NormalizeVenue(cfg.Venue) {
		return false
	}
	instrument = NormalizeInstrument(instrument)
	if instrument == "" {
		return true
	}
	for _, configured := range cfg.Instruments {
		if NormalizeInstrument(configured) == instrument {
			return true
		}
	}
	return false
}

func (m *SignalsManager) applyTPSLUpdate(event UpdateTPSLEvent) {
	venue := NormalizeVenue(firstNonEmpty(event.Venue, m.cfg.Venue))
	instrument := NormalizeInstrument(event.Instrument)
	if venue == "" || instrument == "" {
		return
	}
	key := positionKey(venue, instrument)
	m.mu.Lock()
	position := m.positions[key]
	if position.Instrument == "" {
		m.mu.Unlock()
		return
	}
	if event.Side != "" && position.Side() != event.Side {
		m.mu.Unlock()
		return
	}
	if event.TakeProfitPrice > 0 {
		position.TakeProfitPrice = event.TakeProfitPrice
	}
	if event.StopLossPrice > 0 {
		position.StopLossPrice = event.StopLossPrice
	}
	if event.TakeProfit > 0 {
		position.TakeProfit = event.TakeProfit
	}
	if event.StopLoss > 0 {
		position.StopLoss = event.StopLoss
	}
	m.positions[key] = position
	m.mu.Unlock()
}

func (m *SignalsManager) sendIntent(intent Intent) {
	select {
	case m.intents <- intent:
	default:
	}
	m.mu.RLock()
	subscribers := make([]chan Intent, 0, len(m.intentSubscribers))
	for ch := range m.intentSubscribers {
		subscribers = append(subscribers, ch)
	}
	m.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- intent:
		default:
		}
	}
}

func (m *SignalsManager) sendProtectionUpdate(event UpdateTPSLEvent) {
	select {
	case m.protections <- event:
	default:
	}
	m.mu.RLock()
	subscribers := make([]chan UpdateTPSLEvent, 0, len(m.protectionSubscribers))
	for ch := range m.protectionSubscribers {
		subscribers = append(subscribers, ch)
	}
	m.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (m *SignalsManager) sendWithdrawal(event WithdrawEvent) {
	select {
	case m.withdrawals <- event:
	default:
	}
	m.mu.RLock()
	subscribers := make([]chan WithdrawEvent, 0, len(m.withdrawalSubscribers))
	for ch := range m.withdrawalSubscribers {
		subscribers = append(subscribers, ch)
	}
	m.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (m *SignalsManager) sendBacktest(event BacktestEvent) {
	select {
	case m.backtests <- event:
	default:
	}
	m.mu.RLock()
	subscribers := make([]chan BacktestEvent, 0, len(m.backtestSubscribers))
	for ch := range m.backtestSubscribers {
		subscribers = append(subscribers, ch)
	}
	m.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (m *SignalsManager) sendMessage(event InfoEvent) {
	select {
	case m.messages <- event:
	default:
	}
	m.mu.RLock()
	subscribers := make([]chan InfoEvent, 0, len(m.messageSubscribers))
	for ch := range m.messageSubscribers {
		subscribers = append(subscribers, ch)
	}
	m.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (m *SignalsManager) sendEvent(event Event) {
	select {
	case m.events <- event:
	default:
	}
	m.mu.RLock()
	subscribers := make([]chan Event, 0, len(m.eventSubscribers))
	for ch := range m.eventSubscribers {
		subscribers = append(subscribers, ch)
	}
	m.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (m *SignalsManager) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case m.errors <- err:
	default:
	}
	m.mu.RLock()
	subscribers := make([]chan error, 0, len(m.errorSubscribers))
	for ch := range m.errorSubscribers {
		subscribers = append(subscribers, ch)
	}
	m.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- err:
		default:
		}
	}
}

func (m *SignalsManager) subscriberBuffer() int {
	if m == nil || m.buffer <= 0 {
		return DefaultSignalsManagerBuffer
	}
	return m.buffer
}

func (m *SignalsManager) removeIntentSubscriber(ctx context.Context, ch chan Intent) {
	<-ctx.Done()
	m.mu.Lock()
	if _, ok := m.intentSubscribers[ch]; ok {
		delete(m.intentSubscribers, ch)
		close(ch)
	}
	m.mu.Unlock()
}

func (m *SignalsManager) removeProtectionSubscriber(ctx context.Context, ch chan UpdateTPSLEvent) {
	<-ctx.Done()
	m.mu.Lock()
	if _, ok := m.protectionSubscribers[ch]; ok {
		delete(m.protectionSubscribers, ch)
		close(ch)
	}
	m.mu.Unlock()
}

func (m *SignalsManager) removeWithdrawalSubscriber(ctx context.Context, ch chan WithdrawEvent) {
	<-ctx.Done()
	m.mu.Lock()
	if _, ok := m.withdrawalSubscribers[ch]; ok {
		delete(m.withdrawalSubscribers, ch)
		close(ch)
	}
	m.mu.Unlock()
}

func (m *SignalsManager) removeBacktestSubscriber(ctx context.Context, ch chan BacktestEvent) {
	<-ctx.Done()
	m.mu.Lock()
	if _, ok := m.backtestSubscribers[ch]; ok {
		delete(m.backtestSubscribers, ch)
		close(ch)
	}
	m.mu.Unlock()
}

func (m *SignalsManager) removeMessageSubscriber(ctx context.Context, ch chan InfoEvent) {
	<-ctx.Done()
	m.mu.Lock()
	if _, ok := m.messageSubscribers[ch]; ok {
		delete(m.messageSubscribers, ch)
		close(ch)
	}
	m.mu.Unlock()
}

func (m *SignalsManager) removeEventSubscriber(ctx context.Context, ch chan Event) {
	<-ctx.Done()
	m.mu.Lock()
	if _, ok := m.eventSubscribers[ch]; ok {
		delete(m.eventSubscribers, ch)
		close(ch)
	}
	m.mu.Unlock()
}

func (m *SignalsManager) removeErrorSubscriber(ctx context.Context, ch chan error) {
	<-ctx.Done()
	m.mu.Lock()
	if _, ok := m.errorSubscribers[ch]; ok {
		delete(m.errorSubscribers, ch)
		close(ch)
	}
	m.mu.Unlock()
}

func normalizeInstrumentList(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = NormalizeInstrument(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
