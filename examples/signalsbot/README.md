# Go Signalsbot Example

Paper-trading command line bot for Grexie Signals. It subscribes to `SIGNALS_INSTRUMENTS`, reads OKX `candle1m` prices, feeds the Go client `PositionManager`, and persists open positions, closed trades, orders, and PnL snapshots in a local bbolt database.

## Run

```sh
cd examples/signalsbot
cp .env.example .env
$EDITOR .env
go run . papertrader
```

The bot logs every position open, close, margin add, and margin removal with order sizing, fees, PnL, take-profit, stop-loss, and confidence details. Every five minutes it prints position-manager stats and current PnL.

Clean the local bbolt database with:

```sh
go run . clean
```

## Docker

```sh
cd examples/signalsbot
cp .env.example .env
docker compose up --build
```

The compose file stores the bbolt database in the `signalsbot-data` volume.
