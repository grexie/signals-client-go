# Go Signalsbot Example

Minimal command line listener for the Bollinger router basket protocol.

It subscribes to `SIGNALS_INSTRUMENTS` through `SignalsManager` and logs:

- `create-market-order`
- `update-tpsl`
- `withdraw`
- lifecycle/info/error events

It deliberately does not place exchange orders. Use the emitted intents to wire your own paper trader or venue executor, then publish fills and positions back with `SignalsManager.UpdatePosition`.

## Run

```sh
cd examples/signalsbot
cp .env.example .env
$EDITOR .env
go run .
```

Set `SIGNALS_WEBSOCKET_URL` for local testing, for example `ws://localhost:8080/public/ws`.
