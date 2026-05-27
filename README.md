# Grexie Signals Go Client

Typed Go client for Grexie Signals websocket subscriptions and in-memory position management.

```sh
go get github.com/grexie/signals-client-go@v0.1.10
```

## Websocket Client

`SignalsClient` opens an authenticated websocket with a `SignalsWebSocketToken`, subscribes and unsubscribes from instruments, and emits typed events.

```go
package main

import (
	"context"
	"log"

	signalsclient "github.com/grexie/signals-client-go"
)

func main() {
	ctx := context.Background()
	client := signalsclient.NewSignalsClient(
		signalsclient.SignalsWebSocketToken("ws_your_token"),
		signalsclient.WithBaseURL("https://signals.grexie.com"),
	)
	if err := client.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Subscribe(ctx, "okx", "BTC-USDT-SWAP"); err != nil {
		log.Fatal(err)
	}

	for {
		event, err := client.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		}
		switch event := event.(type) {
		case signalsclient.SignalEvent:
			log.Printf("%s %s %.2f", event.Signal.Instrument, event.Signal.Side, event.Signal.Confidence)
		case signalsclient.InfoEvent:
			log.Printf("%s: %s", event.Stage, event.Message)
		}
	}
}
```

The server sends `ReadyEvent`, `SubscribedEvent`, `UnsubscribedEvent`, `InfoEvent`, `SignalEvent`, and `ErrorEvent`. Subscription replays are represented by `Replay` and `ReplayedAt` on `InfoEvent` and `SignalEvent`.

## Position Manager

`PositionManager` consumes signal events and maintains a full in-memory position list. It uses the same portfolio model as the Grexie Signals server:

- `MaxMarginRatio` is the fraction of `AssetManager` capital that may be allocated as margin across all open positions;
- `Position.Size`, `Order.SizeDelta`, and `Order.Quantity` are executable lots/contracts, not portfolio percentages or margin values;
- `Order.Margin` and `Order.Notional` carry the capital usage behind those lots;
- confidence is stored separately from size;
- positions are rebalanced by confidence weight;
- exposure reductions, closes, and first-phase flips are emitted before openings or increases;
- openings and increases are capped by live `AssetManager` available exposure when asset snapshots are attached;
- `MinOrderDelta` is scaled by the max portfolio margin budget, so a `0.20` threshold with a 100 USDT margin budget means a 20 USDT minimum order;
- `MinPositionSizeRatio` defaults to `0.01`, suppressing new positions whose margin would be less than 1% of total portfolio capital;
- same-side churn can be suppressed by `RebalanceInterval`, while opposite-side signals can still flip positions;
- fees are applied to order recommendations and realized PnL;
- `AvailableMarginBuffer` reserves part of available margin before sizing openings, and openings are capped or suppressed when the buffered budget cannot fund the next executable lot/min-size step including fees.

```go
manager := signalsclient.NewPositionManager(client, signalsclient.PositionManagerConfig{
	MaxMarginRatio:       0.10,
	MinExpectedEdge:      0.0045,
	MinOrderDelta:        0.20,
	MinPositionSizeRatio: 0.01,
	RebalanceInterval:    6 * time.Hour,
	MakerFeeRate:         0.0002,
	TakerFeeRate:         0.0005,
	MinLeverage:          1,
	MaxLeverage:          3,
	AvailableMarginBuffer: 0.10,
	Instruments: map[string]signalsclient.InstrumentConfig{
		"okx:BTC-USDT-SWAP": {TakerFeeRate: 0.00045, MaxLeverage: 5},
	},
})
manager.InstrumentManager().UpdateInstrument(signalsclient.InstrumentMetadata{
	Venue: "okx", Instrument: "BTC-USDT-SWAP", SettlementCurrency: "USDT",
})

orders, err := manager.HandleSignal(signalsclient.Signal{
	Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: signalsclient.SideBuy,
	Confidence: 0.82, TakeProfit: 0.012, StopLoss: 0.004, Price: 68000,
})
```

`PositionManager` ignores replay signal events and ignores live signals whose venue/instrument pair has not been configured in its `InstrumentManager`. `Run` uses an independent event subscription, so multiple position managers can share one `SignalsClient`.

The manager can also be run directly against a client:

```go
go func() {
	if err := manager.Run(context.Background()); err != nil {
		log.Println(err)
	}
}()

for order := range manager.Orders() {
	log.Printf("%s %s %.4f target %.4f lev %.2f", order.Instrument, order.Side, order.SizeDelta, order.TargetSize, order.Leverage)
}
```

Use `AddPosition`, `UpdatePosition`, and `ClosePosition` to hydrate or mutate the runtime from your exchange account. Use `UpdatePrice` with exchange mark prices to evaluate take-profit and stop-loss exits between websocket signals.

Use `ProductionPositionManagerConfig()` when you want to start from the same execution-policy defaults as the Grexie Signals server and then override individual fields.

## Assets, Instruments, And Stats

Attach `AssetManager` updates for account cash, available balance, used margin, and equity. Attach `InstrumentManager` updates for settlement currency, lot size, minimum size, tick size, contract value, and exchange max leverage. `PositionManager` uses these to emit concrete `Quantity`, `SizeDelta`, `Margin`, `Notional`, `SettlementCurrency`, and fee-value estimates.

Call `Stats()` for realized and unrealized PnL in account value and percent, grouped by instrument and settlement currency.

## Price Data

The current Signals websocket payload exposes strategy direction, confidence, risk levels, and component diagnostics. If your feed does not include signal prices, call `UpdatePosition` or `UpdatePrice` with exchange marks before relying on realized PnL. Sizing recommendations are emitted as executable lots plus the margin/notional required by those lots.

## Development

```sh
go test ./...
```
