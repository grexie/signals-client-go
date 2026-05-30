package signalsclient

import (
	"math"
	"testing"
	"time"
)

func TestPositionManagerOpensAndFlips(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.10,
		MinExpectedEdge:   0,
		MinOrderDelta:     0.20,
		RebalanceInterval: time.Hour,
		FlipFlopWindow:    0,
		MinLeverage:       1,
		MaxLeverage:       5,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	buyOrders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.8,
		TakeProfit: 0.02, StopLoss: 0.004, Score: 0.5, Price: 100, Timestamp: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(buyOrders) != 1 || buyOrders[0].Side != SideBuy || buyOrders[0].Reason != "opening" {
		t.Fatalf("unexpected buy orders: %+v", buyOrders)
	}
	if math.Abs(orderBudgetCost(buyOrders[0])-0.10) > 1e-9 {
		t.Fatalf("expected target plus fees to use 0.10 budget, got %+v", buyOrders[0])
	}
	sellOrders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 0.9,
		TakeProfit: 0.02, StopLoss: 0.004, Score: -0.6, Price: 99, Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sellOrders) != 1 || sellOrders[0].Side != SideSell || sellOrders[0].Reason != "flip" {
		t.Fatalf("expected flip close order, got %+v", sellOrders)
	}
	if math.Abs(sellOrders[0].TargetSize) > 1e-9 || math.Abs(sellOrders[0].SizeDelta+buyOrders[0].TargetSize) > 1e-9 {
		t.Fatalf("expected first flip phase to close the long only, got %+v", sellOrders[0])
	}
	openShort, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 0.9,
		TakeProfit: 0.02, StopLoss: 0.004, Score: -0.6, Price: 99, Timestamp: now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(openShort) != 1 || openShort[0].Side != SideSell || openShort[0].Reason != "opening" {
		t.Fatalf("expected second flip phase to open short, got %+v", openShort)
	}
}

func TestPositionManagerSuppressesFlipFlopByDefault(t *testing.T) {
	pm := NewPositionManager(nil, ProductionPositionManagerConfig())
	pm.cfg.MaxMarginRatio = 0.10
	pm.cfg.MinExpectedEdge = 0
	pm.cfg.MinOrderDelta = 0
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	open, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.8,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 1 {
		t.Fatalf("open orders = %+v, want one opening", open)
	}
	strongFlip, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 0.99,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 99.95, Timestamp: now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(strongFlip) != 0 {
		t.Fatalf("default flip orders = %+v, want suppressed hard hysteresis", strongFlip)
	}
	outsideWindow, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 0.99,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 99.95, Timestamp: now.Add(31 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(outsideWindow) != 1 || outsideWindow[0].Reason != "flip" {
		t.Fatalf("outside-window flip orders = %+v, want flip", outsideWindow)
	}
}

func TestPositionManagerIgnoresSignalsAfterInstrumentRemoved(t *testing.T) {
	pm := NewPositionManager(nil, ProductionPositionManagerConfig())
	pm.AssetManager().UpdateAsset(AssetSnapshot{Currency: "USDT", Available: 1000, Equity: 1000})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	pm.UpdateInstrumentConfig("okx", "BTC-USDT-SWAP", InstrumentConfig{TakerFeeRate: 0.001, MinLeverage: 2, MaxLeverage: 2})
	pm.InstrumentManager().RemoveInstrument("okx", "BTC-USDT-SWAP")
	pm.RemoveInstrumentConfig("okx", "BTC-USDT-SWAP")

	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.03, StopLoss: 0.01, Score: 1, Price: 100, Timestamp: time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 {
		t.Fatalf("removed instrument should ignore signals, got %+v", orders)
	}
	if _, ok := pm.InstrumentManager().Instrument("okx", "BTC-USDT-SWAP"); ok {
		t.Fatalf("expected instrument metadata to be removed")
	}
}

func TestPositionManagerAllowsExplicitHighConfidenceFlipThreshold(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:          0.10,
		MinExpectedEdge:         0,
		MinOrderDelta:           0,
		RebalanceInterval:       time.Hour,
		FlipFlopWindow:          30 * time.Minute,
		SignalFlipMinConfidence: 0.72,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	open, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.8,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 1 {
		t.Fatalf("open orders = %+v, want one opening", open)
	}
	weakFlip, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 0.70,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 99.95, Timestamp: now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(weakFlip) != 0 {
		t.Fatalf("weak flip orders = %+v, want suppressed", weakFlip)
	}
	strongFlip, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 0.72,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 99.95, Timestamp: now.Add(6 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(strongFlip) != 1 || strongFlip[0].Reason != "flip" {
		t.Fatalf("strong flip orders = %+v, want flip", strongFlip)
	}
}

func TestPositionManagerUsesConfidenceAsAllocationWeight(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:  0.10,
		MinExpectedEdge: 0,
		MinOrderDelta:   0.20,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "ETH-USDT-SWAP"})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	accepted, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "ETH-USDT-SWAP", Side: SideBuy, Confidence: 0.15,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 1 || math.Abs(orderBudgetCost(accepted[0])-0.10) > 1e-9 {
		t.Fatalf("expected low-confidence sole signal to use full configured budget, got %+v", accepted)
	}
}

func TestPositionManagerManagePositionsOnlyDoesNotOpenOrIncrease(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:  0.10,
		MinExpectedEdge: 0.01,
		MinOrderDelta:   0,
		MinLeverage:     1,
		MaxLeverage:     5,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)

	noOpen, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.9,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now, ManagePositionsOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(noOpen) != 0 || len(pm.Positions()) != 0 {
		t.Fatalf("managePositionsOnly should not open exposure, orders=%+v positions=%+v", noOpen, pm.Positions())
	}

	opened, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.7,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(opened) != 1 || opened[0].Reason != "opening" {
		t.Fatalf("expected normal opening order, got %+v", opened)
	}

	sameSide, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now.Add(2 * time.Minute), ManagePositionsOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sameSide) != 0 {
		t.Fatalf("managePositionsOnly should not increase same-side exposure, got %+v", sameSide)
	}

	closed, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 0.51,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 99, Timestamp: now.Add(3 * time.Minute), ManagePositionsOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(closed) != 1 || math.Abs(closed[0].TargetSize) > 1e-9 || closed[0].Reason != "closing" {
		t.Fatalf("managePositionsOnly opposite signal should close only, got %+v", closed)
	}

	noShort, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 0.51,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 99, Timestamp: now.Add(4 * time.Minute), ManagePositionsOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(noShort) != 0 || len(pm.Positions()) != 0 {
		t.Fatalf("managePositionsOnly should not open after close, orders=%+v positions=%+v", noShort, pm.Positions())
	}
}

func TestPositionManagerTrailingStopClosesAfterFavorableGiveback(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:  0.10,
		MinExpectedEdge: 0,
		MinOrderDelta:   0,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.8,
		TakeProfit: 0.50, StopLoss: 0.20, Price: 100, Timestamp: now,
		TrailingStopActivation: 0.02, TrailingStopDistance: 0.01, TrailingStopMinProfit: 0.001,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 || orders[0].TrailingStopActivation != 0.02 || orders[0].TrailingStopDistance != 0.01 {
		t.Fatalf("expected opening order with trailing settings, got %+v", orders)
	}
	if closed, err := pm.UpdatePrice("okx", "BTC-USDT-SWAP", 103, now.Add(time.Minute)); err != nil || len(closed) != 0 {
		t.Fatalf("expected no close while trail is above floor, orders=%+v err=%v", closed, err)
	}
	closed, err := pm.UpdatePrice("okx", "BTC-USDT-SWAP", 101.8, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(closed) != 1 || closed[0].Reason != "trailing_stop" {
		t.Fatalf("expected trailing_stop close, got %+v", closed)
	}
	trades := pm.ClosedTrades()
	if len(trades) != 1 || trades[0].ExitReason != "trailing_stop" {
		t.Fatalf("closed trades = %+v, want trailing_stop", trades)
	}
	if trades[0].MFE < 0.029 || trades[0].ExitMove < 0.017 || trades[0].RealizedPnL <= 0 {
		t.Fatalf("expected profitable trailing close with tracked MFE, got %+v", trades[0])
	}
}

func TestPositionManagerPersistsAndHydratesTrailingState(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	var snapshots []PositionManagerState
	pm := NewPositionManager(nil, PositionManagerConfig{
		MinExpectedEdge: 0,
		MinOrderDelta:   0,
		Persist: func(state PositionManagerState) {
			snapshots = append(snapshots, state)
		},
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT"})
	pm.AssetManager().UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: 1000, Available: 1000, Equity: 1000})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.9,
		TakeProfit: 0.50, StopLoss: 0.20, Price: 100, Timestamp: now,
		TrailingStopActivation: 0.02, TrailingStopDistance: 0.01, TrailingStopMinProfit: 0.001,
	})
	if err != nil {
		t.Fatalf("HandleSignal: %v", err)
	}
	if len(orders) == 0 {
		t.Fatal("expected opening order")
	}
	if _, err := pm.UpdatePrice("okx", "BTC-USDT-SWAP", 104, now.Add(time.Minute)); err != nil {
		t.Fatalf("UpdatePrice: %v", err)
	}
	if len(snapshots) < 2 {
		t.Fatalf("expected persistence snapshots, got %d", len(snapshots))
	}
	latest := snapshots[len(snapshots)-1]
	if len(latest.Positions) != 1 {
		t.Fatalf("persisted positions = %d, want 1", len(latest.Positions))
	}
	if latest.Positions[0].TrailingStopActivation != 0.02 || latest.Positions[0].TrailingStopDistance != 0.01 {
		t.Fatalf("trailing settings not persisted: %+v", latest.Positions[0])
	}
	if latest.Positions[0].MFE < 0.039 {
		t.Fatalf("MFE was not persisted after price update: %+v", latest.Positions[0])
	}

	rehydrated := NewPositionManager(nil, PositionManagerConfig{
		InitialState: latest,
	})
	positions := rehydrated.Positions()
	if len(positions) != 1 {
		t.Fatalf("hydrated positions = %d, want 1", len(positions))
	}
	if positions[0].TrailingStopActivation != latest.Positions[0].TrailingStopActivation || positions[0].MFE != latest.Positions[0].MFE {
		t.Fatalf("hydrated state mismatch got %+v want %+v", positions[0], latest.Positions[0])
	}
}

func TestPositionManagerTrailingActivationIsAtLeastBreakeven(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:  0.10,
		MinExpectedEdge: 0,
		MinOrderDelta:   0,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.8,
		TakeProfit: 0.50, StopLoss: 0.20, Price: 100, Timestamp: now,
		TrailingStopActivation: 0.0001, TrailingStopDistance: 0.0005,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected opening order, got %+v", orders)
	}
	if orders[0].TrailingStopMinProfit < 0.001 || orders[0].TrailingStopActivation <= orders[0].TrailingStopMinProfit {
		t.Fatalf("expected breakeven-safe trailing settings, got %+v", orders[0])
	}
}

func TestPositionManagerQuantizesOrderTargetSize(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Equity: 1000, Available: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 1, MinSize: 1, TickSize: 0.1,
	})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.50,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		AssetManager:      assets,
		InstrumentManager: instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.15,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 333,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected quantized executable order, got %+v", orders)
	}
	if orders[0].Quantity != 1 || math.Abs(orders[0].SizeDelta-1) > 1e-9 || math.Abs(orders[0].TargetSize-1) > 1e-9 {
		t.Fatalf("expected size to reflect one executable lot, got %+v", orders[0])
	}
}

func TestPositionManagerDropsLowConfidenceInstrumentToFundExecutableHighConfidenceLots(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Equity: 1000, Available: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "LOW-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 1, MinSize: 1, TickSize: 0.1,
	})
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "HIGH-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 1, MinSize: 1, TickSize: 0.1,
	})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.70,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		AssetManager:      assets,
		InstrumentManager: instruments,
	})
	pm.AddPosition(Position{
		Venue: "okx", Instrument: "LOW-USDT-SWAP", Size: 1, Confidence: 0.1,
		EntryPrice: 333, LastPrice: 333,
	})

	reductions, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "HIGH-USDT-SWAP", Side: SideBuy, Confidence: 0.9,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 333,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reductions) != 1 || reductions[0].Instrument != "LOW-USDT-SWAP" || math.Abs(reductions[0].TargetSize) > 1e-9 {
		t.Fatalf("expected low-confidence instrument to be reduced first, got %+v", reductions)
	}
	openings, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "HIGH-USDT-SWAP", Side: SideBuy, Confidence: 0.9,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 333,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(openings) != 1 || openings[0].Instrument != "HIGH-USDT-SWAP" || openings[0].Quantity != 2 || math.Abs(openings[0].TargetSize-2) > 1e-9 {
		t.Fatalf("expected freed budget to fund two high-confidence lots, got %+v", openings)
	}
}

func TestPositionManagerFeeAwareEdgeGate(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:  1,
		MinExpectedEdge: 0.0045,
		TakerFeeRate:    0.0005,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "DOGE-USDT-SWAP"})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "DOGE-USDT-SWAP", Side: SideBuy, Confidence: 0.67,
		TakeProfit: 0.006, StopLoss: 0.004, Price: 0.2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 {
		t.Fatalf("expected fee-aware edge gate to reject marginal signal: %+v", orders)
	}
}

func TestPositionManagerIgnoresUnconfiguredSignals(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:  0.10,
		MinExpectedEdge: 0,
		MinOrderDelta:   0,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "SOL-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 || len(pm.Positions()) != 0 {
		t.Fatalf("expected unconfigured signal to be ignored, orders=%+v positions=%+v", orders, pm.Positions())
	}
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "SOL-USDT-SWAP"})
	orders, err = pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "SOL-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected configured signal to create an order, got %+v", orders)
	}
}

func TestPositionManagerIgnoresReplayEvents(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:  0.10,
		MinExpectedEdge: 0,
		MinOrderDelta:   0,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	ev := SignalEvent{
		SubscriptionID: 3,
		Venue:          "okx",
		Instrument:     "BTC-USDT-SWAP",
		Replay:         true,
		Signal: Signal{
			Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
			TakeProfit: 0.02, StopLoss: 0.004, Price: 100,
		},
	}
	orders, err := pm.HandleEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 || len(pm.Positions()) != 0 {
		t.Fatalf("expected replay event to be ignored, orders=%+v positions=%+v", orders, pm.Positions())
	}
	ev.Replay = false
	orders, err = pm.HandleEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected live event to create an order, got %+v", orders)
	}
}

func TestPositionManagerLeverageAdaptsWithinConfiguredCaps(t *testing.T) {
	leverageFor := func(instrument string, confidence, takeProfit, score float64) float64 {
		pm := NewPositionManager(nil, PositionManagerConfig{
			MaxMarginRatio:  1,
			MinExpectedEdge: 0,
			MinOrderDelta:   0,
			MinLeverage:     1,
			MaxLeverage:     5,
		})
		pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: instrument})
		orders, err := pm.HandleSignal(Signal{
			Venue: "okx", Instrument: instrument, Side: SideBuy, Confidence: confidence,
			TakeProfit: takeProfit, StopLoss: 0, Score: score, Price: 100,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(orders) != 1 {
			t.Fatalf("expected one order for %s, got %+v", instrument, orders)
		}
		return orders[0].Leverage
	}

	low := leverageFor("LOW-USDT-SWAP", 0.2, 0, 0)
	scored := leverageFor("SCORE-USDT-SWAP", 0.2, 0, 1)
	high := leverageFor("HIGH-USDT-SWAP", 1, 0.02, 1)
	if low < 1 || high > 5 {
		t.Fatalf("expected leverage inside [1,5], low=%.8f high=%.8f", low, high)
	}
	if scored <= low {
		t.Fatalf("expected score to increase leverage, low=%.8f scored=%.8f", low, scored)
	}
	if math.Abs(high-5) > 1e-9 {
		t.Fatalf("expected max-confidence leverage to hit cap, got %.8f", high)
	}
}

func TestPositionManagerUpdateConfigKeepsStateAndChangesLeverage(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    1,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		RebalanceInterval: time.Hour,
		FlipFlopWindow:    0,
		MinLeverage:       5,
		MaxLeverage:       5,
	})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	opening, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Score: 1, Price: 100, Timestamp: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(opening) != 1 || math.Abs(opening[0].Leverage-5) > 1e-9 {
		t.Fatalf("expected initial 5x opening, got %+v", opening)
	}

	pm.UpdateConfig(PositionManagerConfig{
		MaxMarginRatio:    1,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		RebalanceInterval: time.Hour,
		FlipFlopWindow:    0,
		MinLeverage:       1,
		MaxLeverage:       1,
	})
	if positions := pm.Positions(); len(positions) != 1 {
		t.Fatalf("expected UpdateConfig to preserve position state, got %+v", positions)
	}

	closing, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideSell, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Score: -1, Price: 99, Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(closing) != 1 || !closing[0].ReduceOnly || math.Abs(closing[0].Leverage-1) > 1e-9 {
		t.Fatalf("expected hot config to produce 1x reduce-only close, got %+v", closing)
	}
}

func TestAssetAndInstrumentManagersProduceConcreteOrders(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: 1000, Available: 900, Used: 100, Equity: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 0.001, MinSize: 0.002, TickSize: 0.1, MaxLeverage: 2,
	})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.10,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		MinLeverage:       1,
		MaxLeverage:       5,
		AssetManager:      assets,
		InstrumentManager: instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100.07,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected one order, got %+v", orders)
	}
	order := orders[0]
	if order.SettlementCurrency != "USDT" {
		t.Fatalf("unexpected settlement currency: %+v", order)
	}
	if math.Abs(order.Price-100.1) > 1e-9 {
		t.Fatalf("expected tick-rounded price 100.1, got %.8f", order.Price)
	}
	if order.Leverage > 2 {
		t.Fatalf("expected instrument leverage cap to apply, got %.8f", order.Leverage)
	}
	if order.Quantity <= 0 || order.Notional <= 0 || order.EstimatedFeeValue <= 0 {
		t.Fatalf("expected concrete quantity/notional/fee value, got %+v", order)
	}
}

func TestPositionManagerRejectsOrdersBelowInstrumentMinSize(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Equity: 10})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 0.001, MinSize: 1, TickSize: 0.1,
	})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.01,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		AssetManager:      assets,
		InstrumentManager: instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 {
		t.Fatalf("expected below-min order to be rejected, got %+v", orders)
	}
}

func TestPositionManagerAllowsBelowMinimumClosingOrder(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Equity: 10})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 0.001, MinSize: 1, TickSize: 0.1,
	})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.01,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		AssetManager:      assets,
		InstrumentManager: instruments,
	})
	pm.AddPosition(Position{Venue: "okx", Instrument: "BTC-USDT-SWAP", Size: 0.01, EntryPrice: 100, LastPrice: 100})
	orders, err := pm.ClosePosition("okx", "BTC-USDT-SWAP")
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 || orders[0].Reason != "closing" || math.Abs(orders[0].TargetSize) > 1e-9 {
		t.Fatalf("expected below-min close to close exactly, got %+v", orders)
	}
	if math.Abs(orders[0].SizeDelta+0.01) > 1e-9 || math.Abs(orders[0].Quantity-0.01) > 1e-9 {
		t.Fatalf("closing order rounded residual position: %+v", orders[0])
	}
}

func TestPositionManagerUsesContractValueForLotSizing(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Equity: 100, Available: 100})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "TRUMP-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 1, MinSize: 1, TickSize: 0.001, ContractValue: 0.1, ContractMultiplier: 1,
	})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.05,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		MinLeverage:       5,
		MaxLeverage:       5,
		AssetManager:      assets,
		InstrumentManager: instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "TRUMP-USDT-SWAP", Side: SideSell, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected one contract-value-sized order, got %+v", orders)
	}
	if orders[0].Quantity != 124 || math.Abs(orders[0].SizeDelta+124) > 1e-12 || math.Abs(orders[0].Notional-24.8) > 1e-12 || math.Abs(orders[0].Margin-4.96) > 1e-12 {
		t.Fatalf("expected contract value to produce executable lots and margin, got %+v", orders[0])
	}
}

func TestPositionManagerPhasesReductionsBeforeOpenings(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: 1000, Available: 1000, Equity: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT"})
	instruments.UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "ETH-USDT-SWAP", SettlementCurrency: "USDT"})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.20,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		AssetManager:      assets,
		InstrumentManager: instruments,
	})
	pm.AddPosition(Position{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Size: 2, Confidence: 1,
		EntryPrice: 100, LastPrice: 100,
	})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	reductions, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "ETH-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reductions) != 1 || reductions[0].Instrument != "BTC-USDT-SWAP" || reductions[0].Side != SideSell {
		t.Fatalf("expected BTC reduction before ETH opening, got %+v", reductions)
	}
	if !reductions[0].ReduceOnly {
		t.Fatalf("expected reduction order to be reduce-only, got %+v", reductions[0])
	}
	expectedBTCTarget := (100.0 / (1 + reductions[0].Leverage*reductions[0].FeeRate)) / reductions[0].Price
	if math.Abs(reductions[0].TargetSize-expectedBTCTarget) > 1e-9 {
		t.Fatalf("expected BTC target to leave room for fees, got %+v", reductions[0])
	}
	openings, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "ETH-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(openings) != 1 || openings[0].Instrument != "ETH-USDT-SWAP" || openings[0].Side != SideBuy {
		t.Fatalf("expected ETH opening after reduction phase, got %+v", openings)
	}
	if openings[0].ReduceOnly {
		t.Fatalf("expected opening order not to be reduce-only, got %+v", openings[0])
	}
}

func TestPositionManagerCapsOpeningsToAvailableExposure(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: 1000, Available: 50, Equity: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT"})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:    0.20,
		MinExpectedEdge:   0,
		MinOrderDelta:     0,
		AssetManager:      assets,
		InstrumentManager: instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected capped opening, got %+v", orders)
	}
	if orderBudgetCost(orders[0])-50 > 1e-9 || orders[0].Margin >= 50 {
		t.Fatalf("expected opening plus fees capped to available exposure, got %+v", orders[0])
	}
}

func TestPositionManagerCapsOpeningsToPortfolioBudgetWithoutAssetSnapshot(t *testing.T) {
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT"})
	instruments.UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "ETH-USDT-SWAP", SettlementCurrency: "USDT"})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:         1,
		MinExpectedEdge:        0,
		MinOrderDelta:          0,
		RebalanceInterval:      6 * time.Hour,
		MinLeverage:            1,
		MaxLeverage:            1,
		ExecutableMarginBuffer: 0,
		InstrumentManager:      instruments,
	})
	now := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.51,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected first opening, got %+v", orders)
	}
	orders, err = pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "ETH-USDT-SWAP", Side: SideBuy, Confidence: 0.51,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, pos := range pm.Positions() {
		total += math.Abs(pos.Size)
	}
	if total > 0.01+1e-9 {
		t.Fatalf("expected total lots to stay within the 1 USDT portfolio budget, total=%.12f orders=%+v positions=%+v", total, orders, pm.Positions())
	}
}

func TestPositionManagerReservesAvailableMarginBuffer(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: 1000, Available: 50, Equity: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT"})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:        0.20,
		MinExpectedEdge:       0,
		MinOrderDelta:         0,
		AvailableMarginBuffer: 0.10,
		AssetManager:          assets,
		InstrumentManager:     instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected buffered capped opening, got %+v", orders)
	}
	if orderBudgetCost(orders[0])-45 > 1e-9 || orders[0].Margin >= 45 {
		t.Fatalf("expected opening plus fees capped to 90%% of available exposure, got %+v", orders[0])
	}
}

func TestPositionManagerSuppressesOpeningWhenBufferedBudgetCannotFundNextLot(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: 1000, Available: 50, Equity: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 0.5, MinSize: 0.5, TickSize: 0.1,
	})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:        0.20,
		MinExpectedEdge:       0,
		MinOrderDelta:         0,
		AvailableMarginBuffer: 0.10,
		AssetManager:          assets,
		InstrumentManager:     instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 {
		t.Fatalf("expected opening below the next executable lot to be suppressed, got %+v", orders)
	}
}

func TestPositionManagerAddsExecutableMarginBufferWithoutCrossingNextLot(t *testing.T) {
	assets := NewAssetManager()
	equity := 10.0
	price := 2.045824286
	leverage := 4.0
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: equity, Available: equity, Equity: equity})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "TRUMP-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 1, MinSize: 1, TickSize: 0,
	})
	oneLotMargin := price / leverage
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:         (oneLotMargin * 1.01) / equity,
		MinExpectedEdge:        0,
		MinOrderDelta:          0,
		MinLeverage:            leverage,
		MaxLeverage:            leverage,
		ExecutableMarginBuffer: 0.001,
		AssetManager:           assets,
		InstrumentManager:      instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "TRUMP-USDT-SWAP", Side: SideSell, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: price,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("expected buffered one-lot opening, got %+v", orders)
	}
	if orders[0].Quantity != 1 {
		t.Fatalf("expected exactly one executable lot, got %+v", orders[0])
	}
	if math.Abs(orders[0].SizeDelta+1) > 1e-9 {
		t.Fatalf("expected one-lot size delta, got %+v", orders[0])
	}
	if orders[0].Quantity >= 2 {
		t.Fatalf("buffer crossed into another lot: %+v", orders[0])
	}
}

func TestPositionManagerRejectsOpeningBelowMinimumPositionSizeRatio(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Equity: 1000, Available: 0.5})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "DUST-USDT-SWAP", SettlementCurrency: "USDT", LotSize: 0.1, MinSize: 0.1})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:       0.10,
		MinPositionSizeRatio: 0.01,
		MinExpectedEdge:      0,
		MinOrderDelta:        0,
		AssetManager:         assets,
		InstrumentManager:    instruments,
	})
	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "DUST-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 {
		t.Fatalf("expected target below 1%% portfolio minimum to be suppressed, got %+v", orders)
	}
}

func TestPositionManagerClosesPositionBelowMinimumPositionSizeRatio(t *testing.T) {
	lastSignalAt := time.Now().UTC().Add(-time.Minute)
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: 1000, Available: 0.5, Used: 999.5, Equity: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "DUST-USDT-SWAP", SettlementCurrency: "USDT"})
	pm := NewPositionManager(nil, PositionManagerConfig{
		MaxMarginRatio:       1,
		MinPositionSizeRatio: 0.01,
		MinExpectedEdge:      0,
		MinOrderDelta:        0,
		RebalanceInterval:    6 * time.Hour,
		AssetManager:         assets,
		InstrumentManager:    instruments,
	})
	pm.AddPosition(Position{
		Venue: "okx", Instrument: "DUST-USDT-SWAP",
		Size: 0.005, Confidence: 0.5, EntryPrice: 100, LastPrice: 100,
		LastSignalAt: lastSignalAt,
	})

	orders, err := pm.HandleSignal(Signal{
		Venue: "okx", Instrument: "DUST-USDT-SWAP", Side: SideBuy, Confidence: 1,
		TakeProfit: 0.02, StopLoss: 0.004, Price: 100, Timestamp: lastSignalAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 || orders[0].Side != SideSell || orders[0].Reason != "closing" || math.Abs(orders[0].TargetSize) > 1e-9 {
		t.Fatalf("expected below-minimum position to close, got %+v", orders)
	}
	if math.Abs(orders[0].SizeDelta+0.005) > 1e-9 || math.Abs(orders[0].Quantity-0.005) > 1e-9 {
		t.Fatalf("closing order rounded residual position: %+v", orders[0])
	}
}

func TestPositionManagerReplacePositionsDropsMissingVenuePositions(t *testing.T) {
	pm := NewPositionManager(nil, PositionManagerConfig{MinExpectedEdge: 0, MinOrderDelta: 0})
	pm.InstrumentManager().UpdateInstrument(InstrumentMetadata{Venue: "okx", Instrument: "BTC-USDT-SWAP"})
	pm.AddPosition(Position{Venue: "okx", Instrument: "BTC-USDT-SWAP", Size: 10, EntryPrice: 100, LastPrice: 100})
	pm.ReplacePositions(nil)
	if got := pm.Positions(); len(got) != 0 {
		t.Fatalf("expected missing exchange position to be dropped, got %+v", got)
	}
}

func TestPositionManagerStats(t *testing.T) {
	assets := NewAssetManager()
	assets.UpdateAsset(AssetSnapshot{Currency: "USDT", Cash: 1000, Available: 800, Used: 200, Equity: 1000})
	instruments := NewInstrumentManager()
	instruments.UpdateInstrument(InstrumentMetadata{
		Venue: "okx", Instrument: "ETH-USDT-SWAP", SettlementCurrency: "USDT",
		LotSize: 0.01, MinSize: 0.01, TickSize: 0.01,
	})
	pm := NewPositionManager(nil, PositionManagerConfig{AssetManager: assets, InstrumentManager: instruments})
	pm.AddPosition(Position{
		Venue: "okx", Instrument: "ETH-USDT-SWAP", Size: 10, EntryPrice: 100, LastPrice: 110,
		Leverage: 2, RealizedPnL: 0.01, Fees: 0.001,
	})
	stats := pm.Stats()
	if stats.Equity != 1000 || stats.Available != 800 || stats.Used != 200 {
		t.Fatalf("unexpected asset stats: %+v", stats)
	}
	instrument := stats.ByInstrument["okx:ETH-USDT-SWAP"]
	if instrument.SettlementCurrency != "USDT" || instrument.Quantity <= 0 || instrument.UnrealizedPnL <= 0 {
		t.Fatalf("unexpected instrument stats: %+v", instrument)
	}
	if stats.RealizedPnLPercent <= 0 || stats.TotalPnLPercent <= 0 {
		t.Fatalf("expected positive pnl percentages, got %+v", stats)
	}
}
