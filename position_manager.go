package signalsclient

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	DefaultMakerFeeRate         = 0.0002
	DefaultTakerFeeRate         = 0.0005
	DefaultPositionSize         = 1.0
	DefaultMinExpectedEdge      = 0.0045
	DefaultMinOrderDelta        = 0.20
	DefaultRebalanceInterval    = 6 * time.Hour
	DefaultMinimumLeverage      = 1.0
	DefaultMaximumLeverage      = 1.0
	portfolioPositionBudget     = 1.0
	floatTolerance              = 1e-9
	defaultPositionOrderChannel = 128
)

// InstrumentConfig overrides fees and leverage limits for one instrument.
type InstrumentConfig struct {
	MakerFeeRate float64
	TakerFeeRate float64
	MinLeverage  float64
	MaxLeverage  float64
}

// PositionManagerConfig controls fee-aware sizing and leverage selection.
type PositionManagerConfig struct {
	PositionSize      float64
	MinExpectedEdge   float64
	MinOrderDelta     float64
	RebalanceInterval time.Duration
	MakerFeeRate      float64
	TakerFeeRate      float64
	MinLeverage       float64
	MaxLeverage       float64
	Instruments       map[string]InstrumentConfig
	AssetManager      *AssetManager
	InstrumentManager *InstrumentManager
}

// ProductionPositionManagerConfig returns the same execution-policy defaults
// used by the Grexie Signals server.
func ProductionPositionManagerConfig() PositionManagerConfig {
	return PositionManagerConfig{
		PositionSize:      DefaultPositionSize,
		MinExpectedEdge:   DefaultMinExpectedEdge,
		MinOrderDelta:     DefaultMinOrderDelta,
		RebalanceInterval: DefaultRebalanceInterval,
		MakerFeeRate:      DefaultMakerFeeRate,
		TakerFeeRate:      DefaultTakerFeeRate,
		MinLeverage:       DefaultMinimumLeverage,
		MaxLeverage:       DefaultMaximumLeverage,
		Instruments:       map[string]InstrumentConfig{},
	}
}

// Position is the in-memory state tracked by PositionManager.
type Position struct {
	Venue         string
	Instrument    string
	Size          float64
	Confidence    float64
	EntryPrice    float64
	LastPrice     float64
	TakeProfit    float64
	StopLoss      float64
	Leverage      float64
	RealizedGross float64
	Fees          float64
	RealizedPnL   float64
	OpenedAt      time.Time
	LastSignalAt  time.Time
}

// Side returns the current position direction.
func (p Position) Side() Side {
	if p.Size < 0 {
		return SideSell
	}
	if p.Size > 0 {
		return SideBuy
	}
	return ""
}

// UnrealizedPnL returns the unlevered percentage PnL contribution of the
// position using its current size.
func (p Position) UnrealizedPnL() float64 {
	return p.move() * math.Abs(p.Size)
}

// Order is a target order recommendation emitted by PositionManager.
type Order struct {
	Venue              string
	Instrument         string
	Side               Side
	Reason             string
	SizeDelta          float64
	PreviousSize       float64
	TargetSize         float64
	Price              float64
	Confidence         float64
	Score              float64
	ExpectedEdge       float64
	FeeRate            float64
	EstimatedFee       float64
	EstimatedFeeValue  float64
	Quantity           float64
	Notional           float64
	SettlementCurrency string
	MinSize            float64
	LotSize            float64
	TickSize           float64
	Leverage           float64
	TakeProfit         float64
	StopLoss           float64
	Timestamp          time.Time
	Subscription       int64
	Replay             bool
}

// ClosedTrade records realized PnL after a position is closed or flipped.
type ClosedTrade struct {
	Venue         string
	Instrument    string
	Side          Side
	Size          float64
	EntryPrice    float64
	ExitPrice     float64
	RealizedGross float64
	Fees          float64
	RealizedPnL   float64
	OpenedAt      time.Time
	ClosedAt      time.Time
}

// PositionStats summarizes realized and unrealized performance across the
// in-memory runtime.
type PositionStats struct {
	Equity               float64
	Available            float64
	Used                 float64
	RealizedPnL          float64
	UnrealizedPnL        float64
	Fees                 float64
	RealizedPnLPercent   float64
	UnrealizedPnLPercent float64
	TotalPnLPercent      float64
	ByInstrument         map[string]InstrumentPositionStats
	ByCurrency           map[string]CurrencyPositionStats
}

// InstrumentPositionStats reports PnL and settlement currency per instrument.
type InstrumentPositionStats struct {
	Venue                string
	Instrument           string
	SettlementCurrency   string
	Side                 Side
	Size                 float64
	Quantity             float64
	Notional             float64
	RealizedPnL          float64
	UnrealizedPnL        float64
	Fees                 float64
	RealizedPnLPercent   float64
	UnrealizedPnLPercent float64
	TotalPnLPercent      float64
	Leverage             float64
}

// CurrencyPositionStats aggregates stats by settlement currency.
type CurrencyPositionStats struct {
	SettlementCurrency   string
	Equity               float64
	Available            float64
	Used                 float64
	RealizedPnL          float64
	UnrealizedPnL        float64
	Fees                 float64
	RealizedPnLPercent   float64
	UnrealizedPnLPercent float64
	TotalPnLPercent      float64
}

// PositionManager consumes signal events and maintains an in-memory portfolio.
type PositionManager struct {
	client      *SignalsClient
	cfg         PositionManagerConfig
	assets      *AssetManager
	instruments *InstrumentManager

	mu           sync.RWMutex
	positions    map[string]*Position
	closedTrades []ClosedTrade
	orders       chan Order
}

// NewPositionManager creates an in-memory manager. Pass nil as client when
// feeding signals manually via HandleSignal.
func NewPositionManager(client *SignalsClient, cfg PositionManagerConfig) *PositionManager {
	cfg = normalizePositionManagerConfig(cfg)
	assets := cfg.AssetManager
	if assets == nil {
		assets = NewAssetManager()
	}
	instruments := cfg.InstrumentManager
	if instruments == nil {
		instruments = NewInstrumentManager()
	}
	return &PositionManager{
		client:      client,
		cfg:         cfg,
		assets:      assets,
		instruments: instruments,
		positions:   make(map[string]*Position),
		orders:      make(chan Order, defaultPositionOrderChannel),
	}
}

// AssetManager returns the mutable asset manager used by PositionManager.
func (pm *PositionManager) AssetManager() *AssetManager { return pm.assets }

// InstrumentManager returns the mutable instrument manager used by PositionManager.
func (pm *PositionManager) InstrumentManager() *InstrumentManager { return pm.instruments }

// Orders returns asynchronous order recommendations generated by Run.
func (pm *PositionManager) Orders() <-chan Order {
	return pm.orders
}

// Run consumes events from the associated SignalsClient until the context ends.
func (pm *PositionManager) Run(ctx context.Context) error {
	if pm.client == nil {
		return errors.New("signalsclient: PositionManager has no SignalsClient")
	}
	events, errs := pm.client.SubscribeEvents(ctx)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			orders, err := pm.HandleEvent(ev)
			if err != nil {
				return err
			}
			for _, order := range orders {
				select {
				case pm.orders <- order:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		case err, ok := <-errs:
			if !ok {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// AddPosition inserts a position into the in-memory runtime.
func (pm *PositionManager) AddPosition(position Position) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	key := positionKey(position.Venue, position.Instrument)
	copy := position
	if copy.Leverage <= 0 {
		copy.Leverage = pm.minLeverage(key)
	}
	pm.positions[key] = &copy
}

// UpdatePosition upserts a position in the in-memory runtime.
func (pm *PositionManager) UpdatePosition(position Position) {
	pm.AddPosition(position)
}

// ClosePosition closes one position at its last known price and returns the
// resulting order recommendation.
func (pm *PositionManager) ClosePosition(venue, instrument string) ([]Order, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	key := positionKey(venue, instrument)
	pos := pm.positions[key]
	if pos == nil || math.Abs(pos.Size) <= floatTolerance {
		return nil, nil
	}
	now := time.Now().UTC()
	delta := -pos.Size
	price := pos.LastPrice
	if price <= 0 {
		price = pos.EntryPrice
	}
	order := pm.orderForDeltaLocked(key, pos, delta, 0, 0, "closing", now, 0, false)
	if !pm.orderMeetsInstrumentMinimum(order) {
		return nil, nil
	}
	pm.applyPositionDeltaLocked(key, pos, delta, price, pm.takerFeeRate(key), now)
	return []Order{order}, nil
}

// Positions returns a stable snapshot of open positions.
func (pm *PositionManager) Positions() []Position {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	positions := make([]Position, 0, len(pm.positions))
	for _, pos := range pm.positions {
		positions = append(positions, *pos)
	}
	sort.Slice(positions, func(i, j int) bool {
		if positions[i].Venue == positions[j].Venue {
			return positions[i].Instrument < positions[j].Instrument
		}
		return positions[i].Venue < positions[j].Venue
	})
	return positions
}

// ClosedTrades returns realized closed trades produced by closes and flips.
func (pm *PositionManager) ClosedTrades() []ClosedTrade {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	trades := make([]ClosedTrade, len(pm.closedTrades))
	copy(trades, pm.closedTrades)
	return trades
}

// Stats returns current realized and unrealized PnL percentages plus
// instrument settlement currency details.
func (pm *PositionManager) Stats() PositionStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	stats := PositionStats{
		ByInstrument: make(map[string]InstrumentPositionStats),
		ByCurrency:   make(map[string]CurrencyPositionStats),
	}
	for _, asset := range pm.assets.Assets() {
		stats.Equity += asset.Equity
		stats.Available += asset.Available
		stats.Used += asset.Used
		stats.ByCurrency[asset.Currency] = CurrencyPositionStats{
			SettlementCurrency: asset.Currency,
			Equity:             asset.Equity,
			Available:          asset.Available,
			Used:               asset.Used,
		}
	}
	for key, pos := range pm.positions {
		if pos == nil {
			continue
		}
		metadata := pm.instrumentMetadataForKey(key, pos.Venue, pos.Instrument)
		asset, _ := pm.assets.Asset(metadata.SettlementCurrency)
		equity := positiveOr(asset.Equity, asset.Cash+asset.Used, 1)
		price := roundToTick(positiveOr(pos.LastPrice, pos.EntryPrice), metadata.TickSize)
		quantity := 0.0
		notional := 0.0
		if price > 0 {
			notional = math.Abs(pos.Size) * equity * positiveOr(pos.Leverage, pm.minLeverage(key), 1)
			quantity = roundDownToStep(notional/price, metadata.LotSize)
			notional = quantity * price
		}
		realizedValue := pos.RealizedPnL * equity
		unrealizedValue := pos.UnrealizedPnL() * equity
		feesValue := pos.Fees * equity
		stats.ByInstrument[key] = InstrumentPositionStats{
			Venue:                pos.Venue,
			Instrument:           pos.Instrument,
			SettlementCurrency:   metadata.SettlementCurrency,
			Side:                 pos.Side(),
			Size:                 pos.Size,
			Quantity:             quantity,
			Notional:             notional,
			RealizedPnL:          realizedValue,
			UnrealizedPnL:        unrealizedValue,
			Fees:                 feesValue,
			RealizedPnLPercent:   pos.RealizedPnL,
			UnrealizedPnLPercent: pos.UnrealizedPnL(),
			TotalPnLPercent:      pos.RealizedPnL + pos.UnrealizedPnL(),
			Leverage:             pos.Leverage,
		}
		stats.RealizedPnL += realizedValue
		stats.UnrealizedPnL += unrealizedValue
		stats.Fees += feesValue
		currencyStats := stats.ByCurrency[metadata.SettlementCurrency]
		currencyStats.SettlementCurrency = metadata.SettlementCurrency
		if currencyStats.Equity <= 0 {
			currencyStats.Equity = equity
		}
		currencyStats.RealizedPnL += realizedValue
		currencyStats.UnrealizedPnL += unrealizedValue
		currencyStats.Fees += feesValue
		if currencyStats.Equity > 0 {
			currencyStats.RealizedPnLPercent = currencyStats.RealizedPnL / currencyStats.Equity
			currencyStats.UnrealizedPnLPercent = currencyStats.UnrealizedPnL / currencyStats.Equity
			currencyStats.TotalPnLPercent = (currencyStats.RealizedPnL + currencyStats.UnrealizedPnL) / currencyStats.Equity
		}
		stats.ByCurrency[metadata.SettlementCurrency] = currencyStats
	}
	if stats.Equity <= 0 {
		stats.Equity = 1
	}
	stats.RealizedPnLPercent = stats.RealizedPnL / stats.Equity
	stats.UnrealizedPnLPercent = stats.UnrealizedPnL / stats.Equity
	stats.TotalPnLPercent = (stats.RealizedPnL + stats.UnrealizedPnL) / stats.Equity
	return stats
}

// UpdatePrice updates the last price and evaluates take-profit/stop-loss exits.
func (pm *PositionManager) UpdatePrice(venue, instrument string, price float64, timestamp time.Time) ([]Order, error) {
	if price <= 0 {
		return nil, errors.New("signalsclient: price must be positive")
	}
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	key := positionKey(venue, instrument)
	pos := pm.positions[key]
	if pos == nil {
		return nil, nil
	}
	pos.LastPrice = price
	if !pos.exitTriggered(price) {
		return nil, nil
	}
	reason := "stop_loss"
	if pos.takeProfitTriggered(price) {
		reason = "take_profit"
	}
	feeRate := pm.takerFeeRate(key)
	if reason == "take_profit" {
		feeRate = pm.makerFeeRate(key)
	}
	delta := -pos.Size
	order := pm.orderForDeltaLocked(key, pos, delta, 0, 0, reason, timestamp, 0, false)
	order.FeeRate = feeRate
	order.EstimatedFee = math.Abs(delta) * feeRate
	order.EstimatedFeeValue = order.Notional * feeRate
	if !pm.orderMeetsInstrumentMinimum(order) {
		return nil, nil
	}
	pm.applyPositionDeltaLocked(key, pos, delta, price, feeRate, timestamp)
	return []Order{order}, nil
}

// HandleEvent applies SignalEvent values and ignores non-signal events.
func (pm *PositionManager) HandleEvent(ev Event) ([]Order, error) {
	signalEvent, ok := ev.(SignalEvent)
	if !ok {
		return nil, nil
	}
	signal := signalEvent.Signal
	if signal.Venue == "" {
		signal.Venue = signalEvent.Venue
	}
	if signal.Instrument == "" {
		signal.Instrument = signalEvent.Instrument
	}
	if signal.Timestamp.IsZero() {
		signal.Timestamp = signalEvent.Timestamp
	}
	if signalEvent.Replay {
		return nil, nil
	}
	orders, err := pm.HandleSignal(signal)
	for i := range orders {
		orders[i].Subscription = signalEvent.SubscriptionID
		orders[i].Replay = signalEvent.Replay
	}
	return orders, err
}

// HandleSignal applies a signal to the in-memory portfolio and returns order
// recommendations required to reach the new target sizing.
func (pm *PositionManager) HandleSignal(signal Signal) ([]Order, error) {
	if signal.Venue == "" || signal.Instrument == "" {
		return nil, errors.New("signalsclient: signal venue and instrument are required")
	}
	if _, ok := pm.instruments.Instrument(signal.Venue, signal.Instrument); !ok {
		return nil, nil
	}
	now := signal.Timestamp
	if now.IsZero() {
		now = time.Now().UTC()
	}
	key := positionKey(signal.Venue, signal.Instrument)
	targetSign := sideSign(signal.Side)
	targetConfidence := clamp01(signal.Confidence)
	if targetSign == 0 || targetConfidence <= 0 {
		return nil, nil
	}
	edge := feeAdjustedExpectedEdge(signal, pm.takerFeeRate(key))
	if pm.cfg.MinExpectedEdge > 0 && edge < pm.cfg.MinExpectedEdge {
		return nil, nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()
	pos := pm.positions[key]
	targetSize := targetSign * pm.cfg.PositionSize * targetConfidence
	minOrderDelta := pm.effectiveMinOrderDelta()
	if pos == nil || math.Abs(pos.Size) <= floatTolerance {
		if math.Abs(targetSize) < minOrderDelta {
			return nil, nil
		}
		pos = &Position{
			Venue:        signal.Venue,
			Instrument:   signal.Instrument,
			EntryPrice:   signal.Price,
			LastPrice:    signal.Price,
			OpenedAt:     now,
			LastSignalAt: now,
		}
		pm.positions[key] = pos
	} else {
		isFlip := sign(pos.Size) != 0 && sign(pos.Size) != targetSign
		if !isFlip && pm.cfg.RebalanceInterval > 0 && !pos.LastSignalAt.IsZero() && now.Before(pos.LastSignalAt.Add(pm.cfg.RebalanceInterval)) {
			return nil, nil
		}
		if !isFlip && minOrderDelta > 0 && math.Abs(targetSize-pos.Size) < minOrderDelta {
			return nil, nil
		}
	}

	if signal.Price > 0 {
		pos.LastPrice = signal.Price
		if pos.EntryPrice <= 0 {
			pos.EntryPrice = signal.Price
		}
	}
	pos.Confidence = targetConfidence
	pos.LastSignalAt = now
	if pos.TakeProfit <= 0 || pos.StopLoss <= 0 || pos.Side() != signal.Side {
		pos.TakeProfit = signal.TakeProfit
		pos.StopLoss = signal.StopLoss
	} else {
		pos.TakeProfit = blendRisk(pos.TakeProfit, signal.TakeProfit, 0.5)
		pos.StopLoss = blendRisk(pos.StopLoss, signal.StopLoss, 0.5)
	}
	pos.Leverage = pm.selectLeverage(key, targetConfidence, edge, signal.Score)

	return pm.rebalanceLocked(now, map[string]float64{key: targetSign}, map[string]signalContext{
		key: {
			confidence:   targetConfidence,
			score:        signal.Score,
			expectedEdge: edge,
			takeProfit:   signal.TakeProfit,
			stopLoss:     signal.StopLoss,
		},
	}), nil
}

func (pm *PositionManager) rebalanceLocked(now time.Time, sideOverrides map[string]float64, signalContexts map[string]signalContext) []Order {
	if pm.cfg.PositionSize <= 0 || len(pm.positions) == 0 {
		return nil
	}
	weights := make(map[string]float64, len(pm.positions))
	sides := make(map[string]float64, len(pm.positions))
	totalWeight := 0.0
	for key, pos := range pm.positions {
		if pos == nil || math.Abs(pos.Size) <= floatTolerance && pos.Confidence <= 0 {
			continue
		}
		_, hasOverride := sideOverrides[key]
		weight := clamp01(pos.Confidence)
		if !hasOverride && weight <= 0 {
			weight = pos.confidenceFromSize(pm.cfg.PositionSize)
		}
		side := sign(pos.Size)
		if override, ok := sideOverrides[key]; ok {
			side = override
		}
		if weight <= floatTolerance || side == 0 {
			weights[key] = 0
			sides[key] = side
			continue
		}
		weights[key] = weight
		sides[key] = side
		totalWeight += weight
	}

	usedBudget := math.Min(pm.cfg.PositionSize, totalWeight)
	orders := make([]Order, 0)
	for key, pos := range pm.positions {
		if pos == nil {
			continue
		}
		targetSize := 0.0
		if totalWeight > 0 {
			targetSize = sides[key] * usedBudget * weights[key] / totalWeight
		}
		delta := targetSize - pos.Size
		if math.Abs(delta) <= floatTolerance {
			pos.Confidence = weights[key]
			continue
		}
		_, hasOverride := sideOverrides[key]
		if pm.shouldSkipRebalanceDelta(pos, targetSize, delta, now, hasOverride) {
			pos.Confidence = weights[key]
			continue
		}
		ctx := signalContexts[key]
		reason := orderReason(pos, targetSize)
		order := pm.orderForDeltaLocked(key, pos, delta, ctx.expectedEdge, ctx.score, reason, now, ctx.confidence, false)
		order.TakeProfit = ctx.takeProfit
		order.StopLoss = ctx.stopLoss
		if !pm.orderMeetsInstrumentMinimum(order) {
			pos.Confidence = weights[key]
			continue
		}
		orders = append(orders, order)
		price := pos.LastPrice
		if price <= 0 {
			price = pos.EntryPrice
		}
		pm.applyPositionDeltaLocked(key, pos, delta, price, pm.takerFeeRate(key), now)
		if current := pm.positions[key]; current != nil {
			current.Confidence = weights[key]
		}
	}
	return orders
}

func (pm *PositionManager) shouldSkipRebalanceDelta(pos *Position, targetSize, delta float64, now time.Time, hasOverride bool) bool {
	isClosing := math.Abs(targetSize) <= floatTolerance && math.Abs(pos.Size) > floatTolerance
	isOpening := math.Abs(pos.Size) <= floatTolerance && math.Abs(targetSize) > floatTolerance
	isFlip := math.Abs(pos.Size) > floatTolerance && math.Abs(targetSize) > floatTolerance && !sameSign(pos.Size, targetSize)
	if isClosing || isOpening || isFlip {
		return false
	}
	if minOrderDelta := pm.effectiveMinOrderDelta(); minOrderDelta > 0 && math.Abs(delta) < minOrderDelta {
		return true
	}
	if !hasOverride && pm.cfg.RebalanceInterval > 0 && !pos.LastSignalAt.IsZero() && now.Before(pos.LastSignalAt.Add(pm.cfg.RebalanceInterval)) {
		return true
	}
	return false
}

func (pm *PositionManager) orderForDeltaLocked(key string, pos *Position, delta, edge, score float64, reason string, now time.Time, confidence float64, replay bool) Order {
	feeRate := pm.takerFeeRate(key)
	side := SideBuy
	if delta < 0 {
		side = SideSell
	}
	if confidence <= 0 {
		confidence = pos.Confidence
	}
	leverage := pm.selectLeverage(key, confidence, edge, score)
	pos.Leverage = leverage
	metadata := pm.instrumentMetadataForKey(key, pos.Venue, pos.Instrument)
	price := roundToTick(positiveOr(pos.LastPrice, pos.EntryPrice), metadata.TickSize)
	asset, _ := pm.assets.Asset(metadata.SettlementCurrency)
	equity := positiveOr(asset.Equity, asset.Cash+asset.Used, 1)
	notional := math.Abs(delta) * equity * leverage
	quantity := 0.0
	if price > 0 {
		quantity = roundDownToStep(notional/price, metadata.LotSize)
		notional = quantity * price
	}
	return Order{
		Venue:              pos.Venue,
		Instrument:         pos.Instrument,
		Side:               side,
		Reason:             reason,
		SizeDelta:          delta,
		PreviousSize:       pos.Size,
		TargetSize:         pos.Size + delta,
		Price:              price,
		Confidence:         confidence,
		Score:              score,
		ExpectedEdge:       edge,
		FeeRate:            feeRate,
		EstimatedFee:       math.Abs(delta) * feeRate,
		EstimatedFeeValue:  notional * feeRate,
		Quantity:           quantity,
		Notional:           notional,
		SettlementCurrency: metadata.SettlementCurrency,
		MinSize:            metadata.MinSize,
		LotSize:            metadata.LotSize,
		TickSize:           metadata.TickSize,
		Leverage:           leverage,
		Timestamp:          now,
		Replay:             replay,
	}
}

func (pm *PositionManager) applyPositionDeltaLocked(key string, pos *Position, delta, price, feeRate float64, now time.Time) {
	if feeRate < 0 {
		feeRate = 0
	}
	if math.Abs(delta) <= floatTolerance {
		return
	}
	if pos.Size == 0 || sameSign(pos.Size, delta) {
		opened := math.Abs(pos.Size) <= floatTolerance
		nextAbs := math.Abs(pos.Size) + math.Abs(delta)
		if price > 0 {
			if nextAbs > 0 && math.Abs(pos.Size) > floatTolerance && pos.EntryPrice > 0 {
				pos.EntryPrice = (pos.EntryPrice*math.Abs(pos.Size) + price*math.Abs(delta)) / nextAbs
			} else {
				pos.EntryPrice = price
			}
			pos.LastPrice = price
		}
		if opened {
			pos.OpenedAt = now
		}
		fee := math.Abs(delta) * feeRate
		pos.Fees += fee
		pos.RealizedPnL -= fee
		pos.Size += delta
		return
	}

	if price > 0 {
		pos.LastPrice = price
	}
	closing := math.Min(math.Abs(pos.Size), math.Abs(delta))
	gross := pos.move() * closing
	fee := closing * feeRate
	pos.RealizedGross += gross
	pos.Fees += fee
	pos.RealizedPnL += gross - fee
	remaining := math.Abs(delta) - closing
	closed := ClosedTrade{
		Venue:         pos.Venue,
		Instrument:    pos.Instrument,
		Side:          pos.Side(),
		Size:          closing,
		EntryPrice:    pos.EntryPrice,
		ExitPrice:     price,
		RealizedGross: pos.RealizedGross,
		Fees:          pos.Fees,
		RealizedPnL:   pos.RealizedPnL,
		OpenedAt:      pos.OpenedAt,
		ClosedAt:      now,
	}
	if remaining <= floatTolerance {
		pos.Size += delta
		if math.Abs(pos.Size) <= floatTolerance {
			delete(pm.positions, key)
			pm.closedTrades = append(pm.closedTrades, closed)
		}
		return
	}

	pm.closedTrades = append(pm.closedTrades, closed)
	pos.Size = sign(delta) * remaining
	pos.EntryPrice = price
	pos.LastPrice = price
	pos.OpenedAt = now
	pos.Confidence = 0
	pos.RealizedGross = 0
	pos.Fees = remaining * pm.takerFeeRate(key)
	pos.RealizedPnL = -pos.Fees
}

func (pm *PositionManager) effectiveMinOrderDelta() float64 {
	if pm.cfg.MinOrderDelta <= 0 {
		return 0
	}
	if pm.cfg.PositionSize <= 0 {
		return pm.cfg.MinOrderDelta
	}
	return pm.cfg.MinOrderDelta * pm.cfg.PositionSize
}

func (pm *PositionManager) selectLeverage(key string, confidence, edge, score float64) float64 {
	minLev := pm.minLeverage(key)
	maxLev := pm.maxLeverage(key)
	if maxLev < minLev {
		maxLev = minLev
	}
	if maxLev == minLev {
		return minLev
	}
	confidence = clamp01(confidence)
	edgeScore := clamp01(edge / math.Max(pm.cfg.MinExpectedEdge*3, 0.001))
	scoreBoost := clamp01(math.Abs(score))
	quality := clamp01(confidence*0.65 + edgeScore*0.25 + scoreBoost*0.10)
	return minLev + (maxLev-minLev)*quality
}

func (pm *PositionManager) makerFeeRate(key string) float64 {
	if override, ok := pm.cfg.Instruments[key]; ok && override.MakerFeeRate > 0 {
		return override.MakerFeeRate
	}
	return pm.cfg.MakerFeeRate
}

func (pm *PositionManager) takerFeeRate(key string) float64 {
	if override, ok := pm.cfg.Instruments[key]; ok && override.TakerFeeRate > 0 {
		return override.TakerFeeRate
	}
	return pm.cfg.TakerFeeRate
}

func (pm *PositionManager) minLeverage(key string) float64 {
	if override, ok := pm.cfg.Instruments[key]; ok && override.MinLeverage > 0 {
		return override.MinLeverage
	}
	return pm.cfg.MinLeverage
}

func (pm *PositionManager) maxLeverage(key string) float64 {
	maxLeverage := pm.cfg.MaxLeverage
	if override, ok := pm.cfg.Instruments[key]; ok && override.MaxLeverage > 0 {
		maxLeverage = override.MaxLeverage
	}
	if metadata := pm.instrumentMetadataForKey(key, "", ""); metadata.MaxLeverage > 0 && (maxLeverage <= 0 || metadata.MaxLeverage < maxLeverage) {
		maxLeverage = metadata.MaxLeverage
	}
	return maxLeverage
}

func (pm *PositionManager) instrumentMetadataForKey(key, venue, instrument string) InstrumentMetadata {
	if venue == "" || instrument == "" {
		for _, pos := range pm.positions {
			if pos != nil && positionKey(pos.Venue, pos.Instrument) == key {
				venue, instrument = pos.Venue, pos.Instrument
				break
			}
		}
	}
	metadata, ok := pm.instruments.Instrument(venue, instrument)
	if !ok {
		metadata = InstrumentMetadata{Venue: venue, Instrument: instrument}
	}
	if metadata.SettlementCurrency == "" {
		metadata.SettlementCurrency = "USDT"
	}
	return metadata
}

func (pm *PositionManager) orderMeetsInstrumentMinimum(order Order) bool {
	if order.Reason == "closing" || order.Reason == "flip" {
		return true
	}
	if order.MinSize > 0 && order.Quantity > 0 && order.Quantity < order.MinSize {
		return false
	}
	if order.MinSize > 0 && order.Quantity <= 0 {
		return false
	}
	return true
}

type signalContext struct {
	confidence   float64
	score        float64
	expectedEdge float64
	takeProfit   float64
	stopLoss     float64
}

func normalizePositionManagerConfig(cfg PositionManagerConfig) PositionManagerConfig {
	if cfg.PositionSize <= 0 {
		cfg.PositionSize = DefaultPositionSize
	}
	if cfg.PositionSize > portfolioPositionBudget {
		cfg.PositionSize = portfolioPositionBudget
	}
	if cfg.MinExpectedEdge < 0 {
		cfg.MinExpectedEdge = 0
	}
	if cfg.MinOrderDelta < 0 {
		cfg.MinOrderDelta = 0
	}
	if cfg.MinOrderDelta > portfolioPositionBudget {
		cfg.MinOrderDelta = portfolioPositionBudget
	}
	if cfg.RebalanceInterval < 0 {
		cfg.RebalanceInterval = 0
	}
	if cfg.MakerFeeRate <= 0 {
		cfg.MakerFeeRate = DefaultMakerFeeRate
	}
	if cfg.TakerFeeRate <= 0 {
		cfg.TakerFeeRate = DefaultTakerFeeRate
	}
	if cfg.MinLeverage <= 0 {
		cfg.MinLeverage = DefaultMinimumLeverage
	}
	if cfg.MaxLeverage <= 0 {
		cfg.MaxLeverage = DefaultMaximumLeverage
	}
	if cfg.Instruments == nil {
		cfg.Instruments = map[string]InstrumentConfig{}
	}
	return cfg
}

func feeAdjustedExpectedEdge(signal Signal, takerFee float64) float64 {
	return expectedEdge(signal) - 2*takerFee
}

func expectedEdge(signal Signal) float64 {
	confidence := clamp01(signal.Confidence)
	takeProfit := math.Max(signal.TakeProfit, 0)
	stopLoss := math.Max(signal.StopLoss, 0)
	return confidence*takeProfit - (1-confidence)*stopLoss
}

func (p Position) move() float64 {
	if p.EntryPrice <= 0 || p.LastPrice <= 0 {
		return 0
	}
	if p.Size < 0 {
		return (p.EntryPrice - p.LastPrice) / p.EntryPrice
	}
	return (p.LastPrice - p.EntryPrice) / p.EntryPrice
}

func (p Position) confidenceFromSize(positionSize float64) float64 {
	if positionSize <= 0 {
		return clamp01(math.Abs(p.Size))
	}
	return clamp01(math.Abs(p.Size) / positionSize)
}

func (p Position) takeProfitPrice() float64 {
	if p.EntryPrice <= 0 || p.TakeProfit <= 0 {
		return 0
	}
	if p.Size < 0 {
		return p.EntryPrice * (1 - p.TakeProfit)
	}
	return p.EntryPrice * (1 + p.TakeProfit)
}

func (p Position) stopLossPrice() float64 {
	if p.EntryPrice <= 0 || p.StopLoss <= 0 {
		return 0
	}
	if p.Size < 0 {
		return p.EntryPrice * (1 + p.StopLoss)
	}
	return p.EntryPrice * (1 - p.StopLoss)
}

func (p Position) takeProfitTriggered(price float64) bool {
	target := p.takeProfitPrice()
	if target <= 0 {
		return false
	}
	if p.Size < 0 {
		return price <= target
	}
	return price >= target
}

func (p Position) stopLossTriggered(price float64) bool {
	target := p.stopLossPrice()
	if target <= 0 {
		return false
	}
	if p.Size < 0 {
		return price >= target
	}
	return price <= target
}

func (p Position) exitTriggered(price float64) bool {
	return p.takeProfitTriggered(price) || p.stopLossTriggered(price)
}

func orderReason(pos *Position, targetSize float64) string {
	if math.Abs(pos.Size) <= floatTolerance {
		return "opening"
	}
	if math.Abs(targetSize) <= floatTolerance {
		return "closing"
	}
	if !sameSign(pos.Size, targetSize) {
		return "flip"
	}
	return "rebalance"
}

func positionKey(venue, instrument string) string {
	return venue + ":" + instrument
}

func sideSign(side Side) float64 {
	switch side {
	case SideBuy:
		return 1
	case SideSell:
		return -1
	default:
		return 0
	}
}

func sign(value float64) float64 {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}

func sameSign(a, b float64) bool {
	return sign(a) == sign(b)
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func positiveOr(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func blendRisk(current, incoming, gate float64) float64 {
	if current <= 0 {
		return incoming
	}
	if incoming <= 0 {
		return current
	}
	gate = clamp01(gate)
	return current*(1-gate) + incoming*gate
}
