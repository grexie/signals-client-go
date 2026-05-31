# Grexie Signals Go Client

Typed Go client for the Grexie Signals Bollinger router websocket protocol.

```sh
go get github.com/grexie/signals-client-go@v0.1.20
```

## SignalsManager

`SignalsManager` owns one router basket subscription. Several managers can share one `SignalsClient`; the reconnecting transport automatically reconnects and replays each active basket subscription.

```go
ctx := context.Background()
client := signalsclient.NewSignalsClient(
	signalsclient.SignalsWebSocketToken("ws_your_token"),
	signalsclient.WithBaseURL("https://signals.grexie.com"),
)
defer client.Close()

manager := signalsclient.NewSignalsManager(client, signalsclient.SignalsManagerState{
	Assets: []signalsclient.AssetSnapshot{{
		Venue: "okx", Currency: "USDT",
		Cash: 1000, Available: 1000, Equity: 1000,
		MaxUsage: 1,
	}},
}, signalsclient.SignalsManagerConfig{
	Venue:       "okx",
	Instruments: []string{"BTC-USDT-SWAP", "ETH-USDT-SWAP"},
	Risk: signalsclient.RiskConfig{
		MaxMarginRatio: 1, MaxConcurrentPositions: 1,
		MinLeverage: 1, MaxLeverage: 1,
	},
})

intents := manager.SubscribeIntents(ctx)
go manager.Run(ctx)

for intent := range intents {
	log.Printf("%s %s %.4f reduceOnly=%t", intent.Instrument, intent.Side, intent.ContractSize, intent.ReduceOnly)
	// Execute on your venue, then call manager.UpdatePosition(...)
}
```

Client-to-server updates:

- `UpdateAsset`: currency, cash, available, used, equity, maxUsage.
- `UpdatePosition`: instrument, side, status, size, entry/mark, leverage, TP/SL.
- `AddInstrument` / `RemoveInstrument`: edit a basket while it is running.
- `UpdateConfig`: runtime settings such as `profitWithdrawRatio`.
- `ScheduleWithdrawal`: ask the router to free profitable/breakeven equity.

Server-to-client intents:

- `create-market-order`: open, increase, reduce, close, or preempt with contract size, leverage, reduce-only, TP, and SL metadata.
- `update-tpsl`: update protection for an existing position.
- `withdraw`: withdraw an amount after the router has made room.

## Example

The `examples/signalsbot` command is a minimal router-intent listener. It does not place exchange orders by itself.

```sh
cd examples/signalsbot
cp .env.example .env
go run . 
```

Set `SIGNALS_WEBSOCKET_URL`, `SIGNALS_WEBSOCKET_TOKEN`, `SIGNALS_INSTRUMENTS`, `SIGNALS_INITIAL_EQUITY`, `SIGNALS_MAX_USAGE`, and optionally `SIGNALS_PROFIT_WITHDRAW_RATIO`.

## Development

```sh
go test ./...
```
