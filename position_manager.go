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
	DefaultMakerFeeRate            = 0.0002
	DefaultTakerFeeRate            = 0.0005
	DefaultMaxMarginRatio          = 1.0
	DefaultMinExpectedEdge         = 0.0045
	DefaultMinOrderDelta           = 0.20
	DefaultRebalanceInterval       = 6 * time.Hour
	DefaultMinimumLeverage         = 1.0
	DefaultMaximumLeverage         = 1.0
	DefaultMinPositionSizeRatio    = 0.01
	DefaultAvailableMarginBuffer   = 0.10
	DefaultExecutableMarginBuffer  = 0.001
	floatTolerance                 = 1e-9
	defaultPositionOrderChannel    = 128
	maxExecutableLotBufferFraction = 0.45
)

// InstrumentConfig overrides fees and leverage limits for one instrument.
type InstrumentConfig struct {
	MakerFeeRate float64
	TakerFeeRate float64
	MinLeverage  float64
	MaxLeverage  float64
}

// EventSource is the narrow event stream dependency consumed by
// PositionManager. SignalsClient implements it, and servers/tests can provide
// their own in-process signal distributor.
type EventSource interface {
	SubscribeEvents(ctx context.Context) (<-chan Event, <-chan error)
}

// PositionManagerConfig controls fee-aware sizing and leverage selection.
// MaxMarginRatio is the fraction of AssetManager capital that may be allocated
// as margin across all positions in the portfolio. Position.Size and
// Order.SizeDelta are signed executable quantities/lots; Margin carries the
// settlement-currency margin required by those lots.
type PositionManagerConfig struct {
	MaxMarginRatio         float64
	PositionSize           float64 // Deprecated: use MaxMarginRatio.
	MinExpectedEdge        float64
	MinOrderDelta          float64
	MinPositionSizeRatio   float64
	RebalanceInterval      time.Duration
	MakerFeeRate           float64
	TakerFeeRate           float64
	MinLeverage            float64
	MaxLeverage            float64
	AvailableMarginBuffer  float64
	ExecutableMarginBuffer float64
	Instruments            map[string]InstrumentConfig
	AssetManager           *AssetManager
	InstrumentManager      *InstrumentManager
}

// ProductionPositionManagerConfig returns the same execution-policy defaults
// used by the Grexie Signals server.
func ProductionPositionManagerConfig() PositionManagerConfig {
	return PositionManagerConfig{
		MaxMarginRatio:         DefaultMaxMarginRatio,
		MinExpectedEdge:        DefaultMinExpectedEdge,
		MinOrderDelta:          DefaultMinOrderDelta,
		MinPositionSizeRatio:   DefaultMinPositionSizeRatio,
		RebalanceInterval:      DefaultRebalanceInterval,
		MakerFeeRate:           DefaultMakerFeeRate,
		TakerFeeRate:           DefaultTakerFeeRate,
		MinLeverage:            DefaultMinimumLeverage,
		MaxLeverage:            DefaultMaximumLeverage,
		AvailableMarginBuffer:  DefaultAvailableMarginBuffer,
		ExecutableMarginBuffer: DefaultExecutableMarginBuffer,
		Instruments:            map[string]InstrumentConfig{},
	}
}

// Position is the in-memory state tracked by PositionManager. Size is signed
// executable quantity/lots, not margin or a portfolio percentage.
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

// UnrealizedPnL returns approximate settlement-currency PnL for linear
// instruments that have a contract value of 1. PositionManager.Stats uses
// instrument metadata for exact contract-value-aware PnL.
func (p Position) UnrealizedPnL() float64 {
	return p.move() * math.Abs(p.Size) * positiveOr(p.EntryPrice, 1)
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
	Margin             float64
	Quantity           float64
	Notional           float64
	SettlementCurrency string
	MinSize            float64
	LotSize            float64
	TickSize           float64
	Leverage           float64
	TakeProfit         float64
	StopLoss           float64
	ReduceOnly         bool
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
	client      EventSource
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
func NewPositionManager(client EventSource, cfg PositionManagerConfig) *PositionManager {
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

// UpdateInstrumentConfig upserts fee and leverage overrides for one
// venue/instrument pair. This lets execution adapters keep dynamic venue
// constraints in sync without rebuilding the manager.
func (pm *PositionManager) UpdateInstrumentConfig(venue, instrument string, cfg InstrumentConfig) {
	if pm == nil || venue == "" || instrument == "" {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.cfg.Instruments == nil {
		pm.cfg.Instruments = map[string]InstrumentConfig{}
	}
	pm.cfg.Instruments[positionKey(venue, instrument)] = cfg
}

// Orders returns asynchronous order recommendations generated by Run.
func (pm *PositionManager) Orders() <-chan Order {
	return pm.orders
}

// Run consumes events from the associated SignalsClient until the context ends.
func (pm *PositionManager) Run(ctx context.Context) error {
	if pm.client == nil {
		return errors.New("signalsclient: PositionManager has no event source")
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

// ReplacePositions replaces the open-position snapshot for the manager. This
// is intended for execution adapters that reconcile from an exchange or paper
// ledger before planning the next capital-basis order phase.
func (pm *PositionManager) ReplacePositions(positions []Position) {
	if pm == nil {
		return
	}
	next := make(map[string]*Position, len(positions))
	for _, position := range positions {
		if position.Venue == "" || position.Instrument == "" || math.Abs(position.Size) <= floatTolerance {
			continue
		}
		key := positionKey(position.Venue, position.Instrument)
		copy := position
		if copy.Leverage <= 0 {
			copy.Leverage = pm.minLeverage(key)
		}
		next[key] = &copy
	}
	pm.mu.Lock()
	pm.positions = next
	pm.mu.Unlock()
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
	pm.applyPositionDeltaLocked(key, pos, order.SizeDelta, price, pm.takerFeeRate(key), now)
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
		equity := positiveOr(asset.Equity, asset.Cash+asset.Used, asset.Cash, 1)
		price := roundToTick(positiveOr(pos.LastPrice, pos.EntryPrice), metadata.TickSize)
		contractNotional := instrumentContractNotional(price, metadata)
		quantity := math.Abs(pos.Size)
		notional := 0.0
		if contractNotional > 0 {
			quantity = roundDownToStep(quantity, metadata.LotSize)
			notional = quantity * contractNotional
		}
		realizedValue := pos.RealizedPnL
		unrealizedValue := pm.positionUnrealizedPnLLocked(key, pos)
		feesValue := pos.Fees
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
			RealizedPnLPercent:   ratioOrZero(pos.RealizedPnL, equity),
			UnrealizedPnLPercent: ratioOrZero(pos.UnrealizedPnL(), equity),
			TotalPnLPercent:      ratioOrZero(pos.RealizedPnL+pos.UnrealizedPnL(), equity),
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
	order.EstimatedFee = feeValueForNotional(order.Notional, feeRate)
	order.EstimatedFeeValue = order.Notional * feeRate
	if !pm.orderMeetsInstrumentMinimum(order) {
		return nil, nil
	}
	pm.applyPositionDeltaLocked(key, pos, order.SizeDelta, price, feeRate, timestamp)
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
	now := signal.Timestamp
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return pm.PlanSignals([]Signal{signal}, now)
}

// PlanSignals applies a coherent snapshot of live signals and returns the next
// executable order phase needed to move the portfolio toward the resulting
// allocation. Reductions are emitted before new/increased exposure, and every
// emitted order has already passed lot-size and minimum-size checks.
func (pm *PositionManager) PlanSignals(signals []Signal, now time.Time) ([]Order, error) {
	if len(signals) == 0 {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	type preparedSignal struct {
		signal Signal
		key    string
		side   float64
		ctx    signalContext
	}
	prepared := make([]preparedSignal, 0, len(signals))
	for _, signal := range signals {
		if signal.Venue == "" || signal.Instrument == "" {
			return nil, errors.New("signalsclient: signal venue and instrument are required")
		}
		if _, ok := pm.instruments.Instrument(signal.Venue, signal.Instrument); !ok {
			continue
		}
		if signal.Timestamp.IsZero() {
			signal.Timestamp = now
		}
		key := positionKey(signal.Venue, signal.Instrument)
		targetSign := sideSign(signal.Side)
		targetConfidence := clamp01(signal.Confidence)
		if targetSign == 0 || targetConfidence <= 0 {
			continue
		}
		edge := feeAdjustedExpectedEdge(signal, pm.takerFeeRate(key))
		if pm.cfg.MinExpectedEdge > 0 && edge < pm.cfg.MinExpectedEdge {
			continue
		}
		prepared = append(prepared, preparedSignal{
			signal: signal,
			key:    key,
			side:   targetSign,
			ctx: signalContext{
				confidence:   targetConfidence,
				score:        signal.Score,
				expectedEdge: edge,
				takeProfit:   signal.TakeProfit,
				stopLoss:     signal.StopLoss,
			},
		})
	}
	if len(prepared) == 0 {
		return nil, nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()
	sideOverrides := make(map[string]float64, len(prepared))
	signalContexts := make(map[string]signalContext, len(prepared))
	portfolioBudget := pm.maxPortfolioMarginBudgetLocked()
	minOrderDelta := pm.effectiveMinOrderDelta()
	for _, item := range prepared {
		signal := item.signal
		pos := pm.positions[item.key]
		if pos == nil || math.Abs(pos.Size) <= floatTolerance {
			if portfolioBudget < minOrderDelta || !pm.meetsMinimumPositionSize(portfolioBudget) {
				continue
			}
			pos = &Position{
				Venue:        signal.Venue,
				Instrument:   signal.Instrument,
				EntryPrice:   signal.Price,
				LastPrice:    signal.Price,
				OpenedAt:     now,
				LastSignalAt: signal.Timestamp,
			}
			pm.positions[item.key] = pos
		} else {
			isFlip := sign(pos.Size) != 0 && sign(pos.Size) != item.side
			belowMinimum := !pm.meetsMinimumPositionSize(pm.positionMarginLocked(item.key, pos))
			if !isFlip && !belowMinimum && pm.cfg.RebalanceInterval > 0 && !pos.LastSignalAt.IsZero() && signal.Timestamp.Before(pos.LastSignalAt.Add(pm.cfg.RebalanceInterval)) {
				continue
			}
		}

		if signal.Price > 0 {
			pos.LastPrice = signal.Price
			if pos.EntryPrice <= 0 {
				pos.EntryPrice = signal.Price
			}
		}
		pos.Confidence = item.ctx.confidence
		pos.LastSignalAt = signal.Timestamp
		if pos.TakeProfit <= 0 || pos.StopLoss <= 0 || pos.Side() != signal.Side {
			pos.TakeProfit = signal.TakeProfit
			pos.StopLoss = signal.StopLoss
		} else {
			pos.TakeProfit = blendRisk(pos.TakeProfit, signal.TakeProfit, 0.5)
			pos.StopLoss = blendRisk(pos.StopLoss, signal.StopLoss, 0.5)
		}
		pos.Leverage = pm.selectLeverage(item.key, item.ctx.confidence, item.ctx.expectedEdge, item.ctx.score)
		sideOverrides[item.key] = item.side
		signalContexts[item.key] = item.ctx
	}
	if len(sideOverrides) == 0 {
		return nil, nil
	}
	return pm.rebalanceLocked(now, sideOverrides, signalContexts), nil
}

func (pm *PositionManager) rebalanceLocked(now time.Time, sideOverrides map[string]float64, signalContexts map[string]signalContext) []Order {
	portfolioBudget := pm.maxPortfolioMarginBudgetLocked()
	if portfolioBudget <= 0 || len(pm.positions) == 0 {
		return nil
	}
	weights := make(map[string]float64, len(pm.positions))
	sides := make(map[string]float64, len(pm.positions))
	for key, pos := range pm.positions {
		if pos == nil || math.Abs(pos.Size) <= floatTolerance && pos.Confidence <= 0 {
			continue
		}
		_, hasOverride := sideOverrides[key]
		weight := clamp01(pos.Confidence)
		if !hasOverride && weight <= 0 {
			weight = clamp01(pm.positionMarginLocked(key, pos) / portfolioBudget)
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
	}

	keys := make([]string, 0, len(pm.positions))
	for key := range pm.positions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	targets := pm.allocateTargetSizesLocked(keys, weights, sides, signalContexts)
	reductions := make([]rebalanceCandidate, 0)
	openings := make([]rebalanceCandidate, 0)
	for _, key := range keys {
		pos := pm.positions[key]
		if pos == nil {
			continue
		}
		targetSize := targets[key]
		if math.Abs(pos.Size) > floatTolerance && !pm.meetsMinimumPositionSize(pm.positionMarginLocked(key, pos)) {
			targetSize = 0
		} else if targetSize != 0 && !pm.meetsMinimumPositionSize(pm.marginForQuantityLocked(key, pos, targetSize)) {
			if math.Abs(pos.Size) <= floatTolerance {
				pos.Confidence = weights[key]
				continue
			}
			targetSize = 0
		}
		delta := targetSize - pos.Size
		if isFlipTarget(pos.Size, targetSize) {
			delta = -pos.Size
		}
		if math.Abs(delta) <= floatTolerance {
			pos.Confidence = weights[key]
			continue
		}
		_, hasOverride := sideOverrides[key]
		if pm.shouldSkipRebalanceDelta(key, pos, targetSize, delta, now, hasOverride) {
			pos.Confidence = weights[key]
			continue
		}
		ctx := signalContexts[key]
		reason := orderReason(pos, targetSize)
		candidate := rebalanceCandidate{
			key:     key,
			pos:     pos,
			delta:   delta,
			weight:  weights[key],
			context: ctx,
			reason:  reason,
		}
		if isExposureReduction(pos.Size, pos.Size+delta) {
			reductions = append(reductions, candidate)
		} else {
			openings = append(openings, candidate)
		}
	}
	if len(reductions) > 0 {
		return pm.materializeRebalanceOrdersLocked(reductions, now, nil)
	}
	return pm.materializeRebalanceOrdersLocked(openings, now, map[string]float64{})
}

func (pm *PositionManager) allocateTargetSizesLocked(keys []string, weights map[string]float64, sides map[string]float64, signalContexts map[string]signalContext) map[string]float64 {
	targets := make(map[string]float64, len(keys))
	portfolioBudget := pm.maxPortfolioMarginBudgetLocked()
	if portfolioBudget <= 0 {
		return targets
	}
	active := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if weights[key] > floatTolerance && sides[key] != 0 {
			active[key] = struct{}{}
		}
	}
	for len(active) > 0 {
		totalWeight := 0.0
		for key := range active {
			totalWeight += weights[key]
		}
		if totalWeight <= floatTolerance {
			break
		}
		dropped := ""
		droppedWeight := math.Inf(1)
		for _, key := range keys {
			if _, ok := active[key]; !ok {
				continue
			}
			pos := pm.positions[key]
			if pos == nil {
				continue
			}
			desiredBudget := portfolioBudget * weights[key] / totalWeight
			if executable := pm.executableAllocationForBudget(key, pos, desiredBudget, signalContexts[key]); executable.margin > floatTolerance {
				continue
			}
			if weights[key] < droppedWeight || (math.Abs(weights[key]-droppedWeight) <= floatTolerance && (dropped == "" || key < dropped)) {
				dropped = key
				droppedWeight = weights[key]
			}
		}
		if dropped == "" {
			break
		}
		delete(active, dropped)
	}
	if len(active) == 0 {
		return targets
	}

	totalWeight := 0.0
	for key := range active {
		totalWeight += weights[key]
	}
	if totalWeight <= floatTolerance {
		return targets
	}
	allocated := 0.0
	for _, key := range keys {
		if _, ok := active[key]; !ok {
			continue
		}
		pos := pm.positions[key]
		if pos == nil {
			continue
		}
		desiredBudget := portfolioBudget * weights[key] / totalWeight
		executable := pm.executableAllocationForBudget(key, pos, desiredBudget, signalContexts[key])
		if executable.margin <= floatTolerance {
			continue
		}
		if !pm.meetsMinimumPositionSize(executable.margin) {
			continue
		}
		targets[key] = sides[key] * executable.quantity
		allocated += executable.margin + executable.fee
	}

	free := portfolioBudget - allocated
	if free <= floatTolerance {
		return targets
	}
	priority := append([]string(nil), keys...)
	sort.Slice(priority, func(i, j int) bool {
		wi := weights[priority[i]]
		wj := weights[priority[j]]
		if math.Abs(wi-wj) <= floatTolerance {
			return priority[i] < priority[j]
		}
		return wi > wj
	})
	for _, key := range priority {
		if _, ok := active[key]; !ok || free <= floatTolerance {
			continue
		}
		pos := pm.positions[key]
		if pos == nil {
			continue
		}
		quantityStep, marginStep, feeStep := pm.executableLotStepCost(key, pos, signalContexts[key])
		stepCost := marginStep + feeStep
		if stepCost <= floatTolerance {
			executable := pm.executableAllocationForBudget(key, pos, free, signalContexts[key])
			if executable.quantity > floatTolerance && pm.meetsMinimumPositionSize(executable.margin) {
				targets[key] += sides[key] * executable.quantity
			}
			free = 0
			break
		}
		steps := math.Floor((free + floatTolerance) / stepCost)
		if steps <= 0 {
			continue
		}
		nextQuantity := targets[key] + sides[key]*steps*quantityStep
		nextMargin := math.Abs(nextQuantity) * marginStep / quantityStep
		if !pm.meetsMinimumPositionSize(nextMargin) {
			continue
		}
		targets[key] = nextQuantity
		free -= steps * stepCost
	}
	return targets
}

type executableAllocation struct {
	quantity float64
	margin   float64
	fee      float64
}

type rebalanceCandidate struct {
	key     string
	pos     *Position
	delta   float64
	weight  float64
	context signalContext
	reason  string
}

func (pm *PositionManager) materializeRebalanceOrdersLocked(candidates []rebalanceCandidate, now time.Time, openingExposureByCurrency map[string]float64) []Order {
	orders := make([]Order, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.pos == nil || math.Abs(candidate.delta) <= floatTolerance {
			continue
		}
		delta := candidate.delta
		if openingExposureByCurrency != nil && !isExposureReduction(candidate.pos.Size, candidate.pos.Size+delta) {
			metadata := pm.instrumentMetadataForKey(candidate.key, candidate.pos.Venue, candidate.pos.Instrument)
			currency := metadata.SettlementCurrency
			available := pm.availableExposureBudget(currency) - openingExposureByCurrency[currency]
			if available <= floatTolerance {
				candidate.pos.Confidence = candidate.weight
				continue
			}
			delta = pm.capOpeningDeltaToBudget(candidate.key, candidate.pos, delta, candidate.context, available)
			if math.Abs(delta) <= floatTolerance {
				candidate.pos.Confidence = candidate.weight
				continue
			}
		}
		order := pm.orderForDeltaLocked(candidate.key, candidate.pos, delta, candidate.context.expectedEdge, candidate.context.score, candidate.reason, now, candidate.context.confidence, false)
		order.TakeProfit = candidate.context.takeProfit
		order.StopLoss = candidate.context.stopLoss
		if !pm.orderMeetsInstrumentMinimum(order) {
			candidate.pos.Confidence = candidate.weight
			continue
		}
		orders = append(orders, order)
		if openingExposureByCurrency != nil && !isExposureReduction(order.PreviousSize, order.TargetSize) {
			openingExposureByCurrency[order.SettlementCurrency] += orderBudgetCost(order)
		}
		price := candidate.pos.LastPrice
		if price <= 0 {
			price = candidate.pos.EntryPrice
		}
		pm.applyPositionDeltaLocked(candidate.key, candidate.pos, order.SizeDelta, price, pm.takerFeeRate(candidate.key), now)
		if current := pm.positions[candidate.key]; current != nil {
			current.Confidence = candidate.weight
		}
	}
	return orders
}

func (pm *PositionManager) availableExposureBudget(currency string) float64 {
	portfolioBudget := pm.availablePortfolioBudgetLocked()
	asset, ok := pm.assets.Asset(currency)
	if !ok {
		return portfolioBudget
	}
	if asset.Available <= 0 {
		return 0
	}
	budget := math.Max(0, asset.Available)
	if pm.cfg.AvailableMarginBuffer > 0 {
		budget *= 1 - pm.cfg.AvailableMarginBuffer
	}
	return math.Min(budget, portfolioBudget)
}

func (pm *PositionManager) availablePortfolioBudgetLocked() float64 {
	maxBudget := pm.maxPortfolioMarginBudgetLocked()
	used := 0.0
	for key, pos := range pm.positions {
		if pos == nil {
			continue
		}
		used += pm.positionMarginLocked(key, pos)
	}
	return math.Max(0, maxBudget-used)
}

func (pm *PositionManager) maxPortfolioMarginBudgetLocked() float64 {
	capital := pm.portfolioCapitalLocked()
	if capital <= 0 || pm.cfg.MaxMarginRatio <= 0 {
		return 0
	}
	return capital * pm.cfg.MaxMarginRatio
}

func (pm *PositionManager) portfolioCapitalLocked() float64 {
	capital := 0.0
	for _, asset := range pm.assets.Assets() {
		capital += positiveOr(asset.Equity, asset.Cash+asset.Used, asset.Cash)
	}
	if capital <= 0 {
		return 1
	}
	return capital
}

func (pm *PositionManager) positionMarginLocked(key string, pos *Position) float64 {
	if pos == nil || math.Abs(pos.Size) <= floatTolerance {
		return 0
	}
	return pm.marginForQuantityLocked(key, pos, pos.Size)
}

func (pm *PositionManager) marginForQuantityLocked(key string, pos *Position, quantity float64) float64 {
	if pos == nil || math.Abs(quantity) <= floatTolerance {
		return 0
	}
	metadata := pm.instrumentMetadataForKey(key, pos.Venue, pos.Instrument)
	price := roundToTick(positiveOr(pos.LastPrice, pos.EntryPrice), metadata.TickSize)
	contractNotional := instrumentContractNotional(price, metadata)
	leverage := positiveOr(pos.Leverage, pm.minLeverage(key), 1)
	if contractNotional <= 0 || leverage <= 0 {
		return 0
	}
	return math.Abs(quantity) * contractNotional / leverage
}

func (pm *PositionManager) positionUnrealizedPnLLocked(key string, pos *Position) float64 {
	if pos == nil || math.Abs(pos.Size) <= floatTolerance || pos.EntryPrice <= 0 || pos.LastPrice <= 0 {
		return 0
	}
	return pm.realizedGrossForQuantityLocked(key, pos, math.Abs(pos.Size), pos.LastPrice)
}

func (pm *PositionManager) realizedGrossForQuantityLocked(key string, pos *Position, quantity, exitPrice float64) float64 {
	if pos == nil || quantity <= floatTolerance || pos.EntryPrice <= 0 || exitPrice <= 0 {
		return 0
	}
	metadata := pm.instrumentMetadataForKey(key, pos.Venue, pos.Instrument)
	contractValue := metadata.ContractValue
	if contractValue <= 0 {
		contractValue = 1
	}
	contractMultiplier := metadata.ContractMultiplier
	if contractMultiplier <= 0 {
		contractMultiplier = 1
	}
	move := exitPrice - pos.EntryPrice
	if pos.Size < 0 {
		move = pos.EntryPrice - exitPrice
	}
	return move * quantity * contractValue * contractMultiplier
}

func (pm *PositionManager) feeForQuantityLocked(key string, pos *Position, quantity, price, feeRate float64) float64 {
	if pos == nil || quantity <= floatTolerance || price <= 0 || feeRate <= 0 {
		return 0
	}
	metadata := pm.instrumentMetadataForKey(key, pos.Venue, pos.Instrument)
	notional := quantity * instrumentContractNotional(price, metadata)
	return notional * feeRate
}

func (pm *PositionManager) executableAllocationForBudget(key string, pos *Position, budget float64, context signalContext) executableAllocation {
	if pos == nil || budget <= floatTolerance {
		return executableAllocation{}
	}
	metadata := pm.instrumentMetadataForKey(key, pos.Venue, pos.Instrument)
	price := roundToTick(positiveOr(pos.LastPrice, pos.EntryPrice), metadata.TickSize)
	leverage := pm.selectLeverage(key, context.confidence, context.expectedEdge, context.score)
	contractNotional := instrumentContractNotional(price, metadata)
	if contractNotional <= 0 || leverage <= 0 {
		return executableAllocation{}
	}
	feeRate := pm.takerFeeRate(key)
	maxMargin := budget
	if metadata.LotSize <= 0 {
		feeMultiplier := 1 + leverage*feeRate
		if feeMultiplier > 0 {
			maxMargin = budget / feeMultiplier
		}
	}
	quantity := roundDownToStep(maxMargin*leverage/contractNotional, metadata.LotSize)
	for quantity > floatTolerance {
		if metadata.MinSize > 0 && quantity < metadata.MinSize {
			return executableAllocation{}
		}
		margin := quantity * contractNotional / leverage
		fee := quantity * contractNotional * feeRate
		if margin+fee <= budget+floatTolerance {
			return executableAllocation{quantity: quantity, margin: margin, fee: fee}
		}
		if metadata.LotSize <= 0 {
			return executableAllocation{}
		}
		quantity = roundDownToStep(quantity-metadata.LotSize, metadata.LotSize)
	}
	return executableAllocation{}
}

func (pm *PositionManager) executableLotStepCost(key string, pos *Position, context signalContext) (float64, float64, float64) {
	if pos == nil {
		return 0, 0, 0
	}
	metadata := pm.instrumentMetadataForKey(key, pos.Venue, pos.Instrument)
	if metadata.LotSize <= 0 {
		return 0, 0, 0
	}
	price := roundToTick(positiveOr(pos.LastPrice, pos.EntryPrice), metadata.TickSize)
	leverage := pm.selectLeverage(key, context.confidence, context.expectedEdge, context.score)
	contractNotional := instrumentContractNotional(price, metadata)
	if contractNotional <= 0 || leverage <= 0 {
		return 0, 0, 0
	}
	margin := metadata.LotSize * contractNotional / leverage
	fee := metadata.LotSize * contractNotional * pm.takerFeeRate(key)
	return metadata.LotSize, margin, fee
}

func (pm *PositionManager) capOpeningDeltaToBudget(key string, pos *Position, delta float64, context signalContext, budget float64) float64 {
	if pos == nil || math.Abs(delta) <= floatTolerance || budget <= floatTolerance {
		return 0
	}
	absDelta := math.Abs(delta)
	executable := pm.executableAllocationForBudget(key, pos, budget, context)
	if executable.margin <= floatTolerance {
		return 0
	}
	if !pm.meetsMinimumPositionSize(executable.margin) {
		return 0
	}
	if executable.quantity < absDelta {
		return pm.capExecutableDeltaWithBufferedCost(key, pos, sign(delta)*executable.quantity, context, budget)
	}
	order := pm.orderForDeltaLocked(key, pos, delta, context.expectedEdge, context.score, "budget-check", time.Now().UTC(), context.confidence, false)
	if orderBudgetCost(order) > budget+floatTolerance {
		return pm.capExecutableDeltaWithBufferedCost(key, pos, sign(delta)*executable.quantity, context, budget)
	}
	return delta
}

func (pm *PositionManager) capExecutableDeltaWithBufferedCost(key string, pos *Position, delta float64, context signalContext, budget float64) float64 {
	if pos == nil || math.Abs(delta) <= floatTolerance || budget <= floatTolerance {
		return 0
	}
	metadata := pm.instrumentMetadataForKey(key, pos.Venue, pos.Instrument)
	price := roundToTick(positiveOr(pos.LastPrice, pos.EntryPrice), metadata.TickSize)
	leverage := pm.selectLeverage(key, context.confidence, context.expectedEdge, context.score)
	quantityStep := 0.0
	if metadata.LotSize > 0 && price > 0 && leverage > 0 {
		quantityStep = metadata.LotSize
	}
	candidate := math.Abs(delta)
	for candidate > floatTolerance {
		order := pm.orderForDeltaLocked(key, pos, sign(delta)*candidate, context.expectedEdge, context.score, "budget-check", time.Now().UTC(), context.confidence, false)
		if orderBudgetCost(order) <= budget+floatTolerance {
			return sign(delta) * candidate
		}
		if quantityStep <= floatTolerance {
			return pm.capContinuousOpeningDeltaToBudget(key, pos, delta, context, budget)
		}
		candidate -= quantityStep
	}
	return 0
}

func (pm *PositionManager) capContinuousOpeningDeltaToBudget(key string, pos *Position, delta float64, context signalContext, budget float64) float64 {
	if pos == nil || math.Abs(delta) <= floatTolerance || budget <= floatTolerance {
		return 0
	}
	low := 0.0
	high := math.Abs(delta)
	for i := 0; i < 64; i++ {
		mid := (low + high) / 2
		if mid <= floatTolerance {
			break
		}
		order := pm.orderForDeltaLocked(key, pos, sign(delta)*mid, context.expectedEdge, context.score, "budget-check", time.Now().UTC(), context.confidence, false)
		if orderBudgetCost(order) <= budget+floatTolerance {
			low = mid
		} else {
			high = mid
		}
	}
	if low <= floatTolerance {
		return 0
	}
	return sign(delta) * low
}

func (pm *PositionManager) shouldSkipRebalanceDelta(key string, pos *Position, targetSize, delta float64, now time.Time, hasOverride bool) bool {
	isClosing := math.Abs(targetSize) <= floatTolerance && math.Abs(pos.Size) > floatTolerance
	isOpening := math.Abs(pos.Size) <= floatTolerance && math.Abs(targetSize) > floatTolerance
	isFlip := math.Abs(pos.Size) > floatTolerance && math.Abs(targetSize) > floatTolerance && !sameSign(pos.Size, targetSize)
	if isClosing || isOpening || isFlip {
		return false
	}
	if minOrderDelta := pm.effectiveMinOrderDelta(); minOrderDelta > 0 && pm.marginForQuantityLocked(key, pos, delta) < minOrderDelta {
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
	requestedAbsDelta := math.Abs(delta)
	contractNotional := instrumentContractNotional(price, metadata)
	quantity := requestedAbsDelta
	reduceOnly := isExposureReduction(pos.Size, pos.Size+delta)
	closesToZero := math.Abs(pos.Size) > floatTolerance && math.Abs(pos.Size+delta) <= floatTolerance
	if contractNotional > 0 && !closesToZero {
		quantity = roundDownToStep(quantity, metadata.LotSize)
	}
	notional := quantity * contractNotional
	margin := 0.0
	if leverage > 0 {
		margin = notional / leverage
	}
	executableDelta := sign(delta) * quantity
	return Order{
		Venue:              pos.Venue,
		Instrument:         pos.Instrument,
		Side:               side,
		Reason:             reason,
		SizeDelta:          executableDelta,
		PreviousSize:       pos.Size,
		TargetSize:         pos.Size + executableDelta,
		Price:              price,
		Confidence:         confidence,
		Score:              score,
		ExpectedEdge:       edge,
		FeeRate:            feeRate,
		EstimatedFee:       feeValueForNotional(notional, feeRate),
		EstimatedFeeValue:  notional * feeRate,
		Margin:             margin,
		Quantity:           quantity,
		Notional:           notional,
		SettlementCurrency: metadata.SettlementCurrency,
		MinSize:            metadata.MinSize,
		LotSize:            metadata.LotSize,
		TickSize:           metadata.TickSize,
		Leverage:           leverage,
		ReduceOnly:         reduceOnly,
		Timestamp:          now,
		Replay:             replay,
	}
}

func orderBudgetCost(order Order) float64 {
	return math.Max(0, order.Margin) + math.Max(0, order.EstimatedFee)
}

func feeValueForNotional(notional, feeRate float64) float64 {
	if notional <= 0 || feeRate <= 0 {
		return 0
	}
	return notional * feeRate
}

func instrumentContractNotional(price float64, metadata InstrumentMetadata) float64 {
	if price <= 0 {
		return 0
	}
	contractValue := metadata.ContractValue
	if contractValue <= 0 {
		contractValue = 1
	}
	contractMultiplier := metadata.ContractMultiplier
	if contractMultiplier <= 0 {
		contractMultiplier = 1
	}
	return price * contractValue * contractMultiplier
}

func feeExposureForMargin(margin, leverage, feeRate float64) float64 {
	if margin <= 0 || leverage <= 0 || feeRate <= 0 {
		return 0
	}
	return margin * leverage * feeRate
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
		fee := pm.feeForQuantityLocked(key, pos, math.Abs(delta), price, feeRate)
		pos.Fees += fee
		pos.RealizedPnL -= fee
		pos.Size += delta
		return
	}

	if price > 0 {
		pos.LastPrice = price
	}
	closing := math.Min(math.Abs(pos.Size), math.Abs(delta))
	gross := pm.realizedGrossForQuantityLocked(key, pos, closing, price)
	fee := pm.feeForQuantityLocked(key, pos, closing, price, feeRate)
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
	pos.Fees = pm.feeForQuantityLocked(key, pos, remaining, price, pm.takerFeeRate(key))
	pos.RealizedPnL = -pos.Fees
}

func (pm *PositionManager) effectiveMinOrderDelta() float64 {
	if pm.cfg.MinOrderDelta <= 0 {
		return 0
	}
	return pm.cfg.MinOrderDelta * pm.maxPortfolioMarginBudgetLocked()
}

func (pm *PositionManager) minimumPositionSize() float64 {
	if pm == nil || pm.cfg.MinPositionSizeRatio <= 0 {
		return 0
	}
	return pm.cfg.MinPositionSizeRatio * pm.portfolioCapitalLocked()
}

func (pm *PositionManager) meetsMinimumPositionSize(size float64) bool {
	minimum := pm.minimumPositionSize()
	return minimum <= 0 || math.Abs(size)+floatTolerance >= minimum
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
	if order.Quantity <= 0 {
		return false
	}
	if order.Reason == "closing" || order.Reason == "flip" {
		return true
	}
	if order.MinSize > 0 && order.Quantity > 0 && order.Quantity < order.MinSize {
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
	if cfg.MaxMarginRatio <= 0 {
		if cfg.PositionSize > 0 && cfg.PositionSize <= 1 {
			cfg.MaxMarginRatio = cfg.PositionSize
		} else {
			cfg.MaxMarginRatio = DefaultMaxMarginRatio
		}
	}
	if cfg.MaxMarginRatio > 1 {
		cfg.MaxMarginRatio = 1
	}
	if cfg.MinExpectedEdge < 0 {
		cfg.MinExpectedEdge = 0
	}
	if cfg.MinOrderDelta < 0 {
		cfg.MinOrderDelta = 0
	}
	if cfg.MinOrderDelta > 1 {
		cfg.MinOrderDelta = 1
	}
	if cfg.MinPositionSizeRatio <= 0 {
		cfg.MinPositionSizeRatio = DefaultMinPositionSizeRatio
	}
	if cfg.MinPositionSizeRatio > 1 {
		cfg.MinPositionSizeRatio = 1
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
	if cfg.AvailableMarginBuffer < 0 {
		cfg.AvailableMarginBuffer = 0
	}
	if cfg.AvailableMarginBuffer > 0.95 {
		cfg.AvailableMarginBuffer = 0.95
	}
	if cfg.ExecutableMarginBuffer < 0 {
		cfg.ExecutableMarginBuffer = 0
	}
	if cfg.ExecutableMarginBuffer > 0.05 {
		cfg.ExecutableMarginBuffer = 0.05
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

func isFlipTarget(previousSize, targetSize float64) bool {
	return math.Abs(previousSize) > floatTolerance &&
		math.Abs(targetSize) > floatTolerance &&
		!sameSign(previousSize, targetSize)
}

func isExposureReduction(previousSize, targetSize float64) bool {
	if math.Abs(previousSize) <= floatTolerance {
		return false
	}
	if math.Abs(targetSize) <= floatTolerance {
		return true
	}
	if !sameSign(previousSize, targetSize) {
		return true
	}
	return math.Abs(targetSize) < math.Abs(previousSize)-floatTolerance
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

func ratioOrZero(numerator, denominator float64) float64 {
	if denominator <= 0 {
		return 0
	}
	return numerator / denominator
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
