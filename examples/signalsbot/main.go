package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	signalsclient "github.com/grexie/signals-client-go"
	bolt "go.etcd.io/bbolt"
)

const (
	defaultSignalsWSURL = "wss://signals.grexie.com/ws"
	defaultOKXBaseURL   = "https://www.okx.com"
	defaultOKXWSURL     = "wss://ws.okx.com:8443"
	defaultDBPath       = "signalsbot.db"
	defaultEquity       = 10000.0
	defaultBar          = "1m"
)

var (
	bucketState     = []byte("state")
	bucketOrders    = []byte("orders")
	bucketSnapshots = []byte("snapshots")
)

type config struct {
	token         string
	websocketURL  string
	instruments   []string
	dbPath        string
	initialEquity float64
	okxBaseURL    string
	okxWSURL      string
	candleBar     string
	statsInterval time.Duration
}

type bot struct {
	mu               sync.Mutex
	manager          *signalsclient.PositionManager
	store            *store
	initialEquity    float64
	closedRealized   float64
	lastClosedCount  int
	latestPriceByKey map[string]priceTick
}

type priceTick struct {
	instrument string
	price      float64
	timestamp  time.Time
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	loadDotEnv(".env")

	command := "papertrader"
	if len(os.Args) > 1 {
		command = strings.TrimSpace(os.Args[1])
	}
	switch command {
	case "papertrader":
		if err := runPaperTrader(); err != nil {
			log.Fatal(err)
		}
	case "clean":
		if err := cleanDB(); err != nil {
			log.Fatal(err)
		}
	default:
		fmt.Fprintf(os.Stderr, "usage: signalsbot [papertrader|clean]\n")
		os.Exit(2)
	}
}

func runPaperTrader() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := openStore(cfg.dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	initialState, err := db.LoadState()
	if err != nil {
		return err
	}

	managerConfig := signalsclient.ProductionPositionManagerConfig()
	managerConfig.InitialState = initialState
	managerConfig.Persist = func(state signalsclient.PositionManagerState) {
		if err := db.SaveState(state); err != nil {
			log.Printf("persist position manager state: %v", err)
		}
	}
	manager := signalsclient.NewPositionManager(nil, managerConfig)
	runner := &bot{
		manager:          manager,
		store:            db,
		initialEquity:    cfg.initialEquity,
		closedRealized:   stateClosedRealized(initialState),
		lastClosedCount:  len(initialState.ClosedTrades),
		latestPriceByKey: make(map[string]priceTick),
	}
	runner.syncAssetLocked()

	if err := runner.configureOKX(ctx, cfg); err != nil {
		return err
	}
	if len(initialState.Positions) > 0 || len(initialState.ClosedTrades) > 0 {
		log.Printf("Hydrated position manager state open_positions=%d closed_trades=%d", len(initialState.Positions), len(initialState.ClosedTrades))
	}

	ticks := make(chan priceTick, 512)
	go subscribeOKXCandles(ctx, cfg.okxWSURL, cfg.candleBar, cfg.instruments, ticks)
	go runner.consumePrices(ctx, ticks)
	go runner.reportEvery(ctx, cfg.statsInterval)

	client := signalsclient.NewSignalsClient(
		signalsclient.SignalsWebSocketToken(cfg.token),
		signalsclient.WithURL(cfg.websocketURL),
	)
	if err := client.Connect(ctx); err != nil {
		return err
	}
	defer client.Close()

	for _, instrument := range cfg.instruments {
		if err := client.Subscribe(ctx, "okx", instrument); err != nil {
			return fmt.Errorf("subscribe %s: %w", instrument, err)
		}
		log.Printf("Subscribed to Grexie Signals venue=okx instrument=%s", instrument)
	}

	log.Printf("signalsbot running instruments=%s db=%s ws=%s", strings.Join(cfg.instruments, ","), cfg.dbPath, cfg.websocketURL)
	for {
		event, err := client.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("signals websocket receive: %w", err)
		}
		runner.handleSignalEvent(event)
	}
}

func (b *bot) configureOKX(ctx context.Context, cfg config) error {
	for _, instrument := range cfg.instruments {
		metadata, err := fetchOKXInstrument(ctx, cfg.okxBaseURL, instrument)
		if err != nil {
			return err
		}
		b.manager.InstrumentManager().UpdateInstrument(metadata)
		if tick, ok := fetchLatestCandle(ctx, cfg.okxBaseURL, cfg.candleBar, instrument); ok {
			b.latestPriceByKey[positionKey("okx", instrument)] = tick
			orders, err := b.manager.UpdatePrice("okx", instrument, tick.price, tick.timestamp)
			if err != nil {
				return err
			}
			b.handleOrdersLocked(orders)
		}
		log.Printf("Loaded OKX instrument instrument=%s settlement=%s lot=%.8f min=%.8f tick=%.8f contract=%.8f",
			metadata.Instrument,
			metadata.SettlementCurrency,
			metadata.LotSize,
			metadata.MinSize,
			metadata.TickSize,
			metadata.ContractValue,
		)
	}
	return nil
}

func (b *bot) consumePrices(ctx context.Context, ticks <-chan priceTick) {
	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-ticks:
			if !ok {
				return
			}
			b.mu.Lock()
			b.latestPriceByKey[positionKey("okx", tick.instrument)] = tick
			orders, err := b.manager.UpdatePrice("okx", tick.instrument, tick.price, tick.timestamp)
			if err != nil {
				log.Printf("price update %s: %v", tick.instrument, err)
				b.mu.Unlock()
				continue
			}
			b.handleOrdersLocked(orders)
			b.mu.Unlock()
		}
	}
}

func (b *bot) handleSignalEvent(event signalsclient.Event) {
	switch ev := event.(type) {
	case signalsclient.ReadyEvent:
		log.Printf("Signals websocket ready message=%q", ev.Message)
	case signalsclient.InfoEvent:
		log.Printf("Instrument info instrument=%s stage=%s replay=%t message=%q", ev.Instrument, ev.Stage, ev.Replay, ev.Message)
	case signalsclient.ErrorEvent:
		log.Printf("Signals websocket error code=%s message=%q", ev.Code, ev.Message)
	case signalsclient.SubscribedEvent:
		log.Printf("Subscription confirmed subscription=%d instrument=%s", ev.SubscriptionID, ev.Instrument)
	case signalsclient.UnsubscribedEvent:
		log.Printf("Subscription removed subscription=%d instrument=%s code=%s message=%q", ev.SubscriptionID, ev.Instrument, ev.Code, ev.Message)
	case signalsclient.SignalEvent:
		b.mu.Lock()
		if ev.Signal.Price <= 0 {
			if tick, ok := b.latestPriceByKey[positionKey(firstNonEmpty(ev.Venue, "okx"), ev.Instrument)]; ok {
				ev.Signal.Price = tick.price
				if ev.Signal.Timestamp.IsZero() {
					ev.Signal.Timestamp = tick.timestamp
				}
			}
		}
		if ev.Signal.Price <= 0 {
			log.Printf("Signal skipped instrument=%s side=%s confidence=%.3f reason=no OKX candle price yet", ev.Instrument, ev.Signal.Side, ev.Signal.Confidence)
			b.mu.Unlock()
			return
		}
		orders, err := b.manager.HandleEvent(ev)
		if err != nil {
			log.Printf("signal handling instrument=%s: %v", ev.Instrument, err)
			b.mu.Unlock()
			return
		}
		log.Printf("Signal received instrument=%s side=%s confidence=%.3f price=%.8f replay=%t orders=%d",
			ev.Signal.Instrument,
			ev.Signal.Side,
			ev.Signal.Confidence,
			ev.Signal.Price,
			ev.Replay,
			len(orders),
		)
		b.handleOrdersLocked(orders)
		b.mu.Unlock()
	}
}

func (b *bot) handleOrdersLocked(orders []signalsclient.Order) {
	if len(orders) == 0 {
		return
	}
	for _, order := range orders {
		logOrder(order)
	}
	trades := b.manager.ClosedTrades()
	if b.lastClosedCount < len(trades) {
		newTrades := append([]signalsclient.ClosedTrade(nil), trades[b.lastClosedCount:]...)
		for _, trade := range newTrades {
			b.closedRealized += trade.RealizedPnL
			logClosedTrade(trade, b.initialEquity)
		}
		b.lastClosedCount = len(trades)
	}
	b.syncAssetLocked()
	if err := b.store.AppendOrders(orders); err != nil {
		log.Printf("persist orders: %v", err)
	}
	if err := b.store.SaveSnapshot(b.snapshotLocked()); err != nil {
		log.Printf("persist snapshot: %v", err)
	}
}

func (b *bot) reportEvery(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.mu.Lock()
			snapshot := b.snapshotLocked()
			positions := b.manager.Positions()
			b.mu.Unlock()
			log.Printf("Position manager stats equity=%s realized=%s unrealized=%s total=%s fees=%s open_positions=%d",
				money(snapshot.Equity),
				money(snapshot.RealizedPnL),
				money(snapshot.UnrealizedPnL),
				money(snapshot.TotalPnL),
				money(snapshot.Fees),
				len(positions),
			)
			for _, pos := range positions {
				log.Printf("Open position instrument=%s side=%s size=%.8f entry=%.8f last=%.8f unrealized=%s pnl=%s confidence=%.3f tp=%.4f sl=%.4f",
					pos.Instrument,
					pos.Side(),
					pos.Size,
					pos.EntryPrice,
					pos.LastPrice,
					money(pos.UnrealizedPnL()),
					percent(ratio(pos.UnrealizedPnL(), snapshot.Equity)),
					pos.Confidence,
					pos.TakeProfit,
					pos.StopLoss,
				)
			}
		}
	}
}

func (b *bot) syncAssetLocked() {
	openRealized := 0.0
	for _, pos := range b.manager.Positions() {
		openRealized += pos.RealizedPnL
	}
	equity := b.initialEquity + b.closedRealized + openRealized
	if equity <= 0 {
		equity = b.initialEquity
	}
	b.manager.AssetManager().UpdateAsset(signalsclient.AssetSnapshot{
		Currency:  "USDT",
		Cash:      equity,
		Available: equity,
		Equity:    equity,
		UpdatedAt: time.Now().UTC(),
	})
}

func (b *bot) snapshotLocked() pnlSnapshot {
	stats := b.manager.Stats()
	realized := b.closedRealized + stats.RealizedPnL
	unrealized := stats.UnrealizedPnL
	return pnlSnapshot{
		Timestamp:     time.Now().UTC(),
		Equity:        b.initialEquity + realized,
		RealizedPnL:   realized,
		UnrealizedPnL: unrealized,
		TotalPnL:      realized + unrealized,
		Fees:          stats.Fees,
		RealizedPct:   ratio(realized, b.initialEquity),
		UnrealizedPct: ratio(unrealized, b.initialEquity),
		TotalPct:      ratio(realized+unrealized, b.initialEquity),
	}
}

type pnlSnapshot struct {
	Timestamp     time.Time `json:"timestamp"`
	Equity        float64   `json:"equity"`
	RealizedPnL   float64   `json:"realizedPnl"`
	UnrealizedPnL float64   `json:"unrealizedPnl"`
	TotalPnL      float64   `json:"totalPnl"`
	Fees          float64   `json:"fees"`
	RealizedPct   float64   `json:"realizedPct"`
	UnrealizedPct float64   `json:"unrealizedPct"`
	TotalPct      float64   `json:"totalPct"`
}

type store struct {
	db *bolt.DB
}

func openStore(path string) (*store, error) {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	s := &store{db: db}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{bucketState, bucketOrders, bucketSnapshots} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *store) Close() error {
	return s.db.Close()
}

func (s *store) LoadState() (signalsclient.PositionManagerState, error) {
	var state signalsclient.PositionManagerState
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketState).Get([]byte("latest"))
		if len(data) == 0 {
			return nil
		}
		return json.Unmarshal(data, &state)
	})
	return state, err
}

func (s *store) SaveState(state signalsclient.PositionManagerState) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketState).Put([]byte("latest"), data)
	})
}

func (s *store) AppendOrders(orders []signalsclient.Order) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOrders)
		for _, order := range orders {
			id, err := b.NextSequence()
			if err != nil {
				return err
			}
			data, err := json.Marshal(order)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("%020d", id)), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *store) SaveSnapshot(snapshot pnlSnapshot) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketSnapshots).Put([]byte(snapshot.Timestamp.Format(time.RFC3339Nano)), data)
	})
}

func loadConfig() (config, error) {
	cfg := config{
		token:         strings.TrimSpace(os.Getenv("SIGNALS_WEBSOCKET_TOKEN")),
		websocketURL:  envString("SIGNALS_WEBSOCKET_URL", defaultSignalsWSURL),
		instruments:   splitCSV(envString("SIGNALS_INSTRUMENTS", "DOGE-USDT-SWAP")),
		dbPath:        envString("SIGNALS_DB_PATH", defaultDBPath),
		initialEquity: envFloat("SIGNALS_INITIAL_EQUITY", defaultEquity),
		okxBaseURL:    strings.TrimRight(envString("SIGNALS_OKX_BASE_URL", defaultOKXBaseURL), "/"),
		okxWSURL:      strings.TrimRight(envString("SIGNALS_OKX_WEBSOCKET_URL", defaultOKXWSURL), "/"),
		candleBar:     envString("SIGNALS_OKX_CANDLE_BAR", defaultBar),
		statsInterval: envDuration("SIGNALS_STATS_INTERVAL", 5*time.Minute),
	}
	if cfg.token == "" {
		return cfg, errors.New("SIGNALS_WEBSOCKET_TOKEN is required")
	}
	if len(cfg.instruments) == 0 {
		return cfg, errors.New("SIGNALS_INSTRUMENTS must contain at least one OKX instrument")
	}
	return cfg, nil
}

func cleanDB() error {
	path := envString("SIGNALS_DB_PATH", defaultDBPath)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	log.Printf("Cleaned signalsbot local database path=%s", path)
	return nil
}

func stateClosedRealized(state signalsclient.PositionManagerState) float64 {
	total := 0.0
	for _, trade := range state.ClosedTrades {
		total += trade.RealizedPnL
	}
	return total
}

func loadDotEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
}

func fetchOKXInstrument(ctx context.Context, baseURL string, instrument string) (signalsclient.InstrumentMetadata, error) {
	u, _ := url.Parse(baseURL + "/api/v5/public/instruments")
	q := u.Query()
	q.Set("instType", "SWAP")
	q.Set("instId", instrument)
	u.RawQuery = q.Encode()
	var response struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstID   string `json:"instId"`
			Settle   string `json:"settleCcy"`
			LotSize  string `json:"lotSz"`
			MinSize  string `json:"minSz"`
			TickSize string `json:"tickSz"`
			CtVal    string `json:"ctVal"`
			CtMult   string `json:"ctMult"`
		} `json:"data"`
	}
	if err := getJSON(ctx, u.String(), &response); err != nil {
		return signalsclient.InstrumentMetadata{}, err
	}
	if response.Code != "0" || len(response.Data) == 0 {
		return signalsclient.InstrumentMetadata{}, fmt.Errorf("okx instrument %s: %s %s", instrument, response.Code, response.Msg)
	}
	row := response.Data[0]
	return signalsclient.InstrumentMetadata{
		Venue:              "okx",
		Instrument:         row.InstID,
		SettlementCurrency: firstNonEmpty(row.Settle, "USDT"),
		LotSize:            parseFloat(row.LotSize),
		MinSize:            parseFloat(row.MinSize),
		TickSize:           parseFloat(row.TickSize),
		ContractValue:      parseFloat(row.CtVal),
		ContractMultiplier: positive(parseFloat(row.CtMult), 1),
		MaxLeverage:        1,
	}, nil
}

func fetchLatestCandle(ctx context.Context, baseURL string, bar string, instrument string) (priceTick, bool) {
	u, _ := url.Parse(baseURL + "/api/v5/market/candles")
	q := u.Query()
	q.Set("instId", instrument)
	q.Set("bar", bar)
	q.Set("limit", "1")
	u.RawQuery = q.Encode()
	var response struct {
		Code string     `json:"code"`
		Data [][]string `json:"data"`
	}
	if err := getJSON(ctx, u.String(), &response); err != nil || response.Code != "0" || len(response.Data) == 0 {
		if err != nil {
			log.Printf("fetch latest candle %s: %v", instrument, err)
		}
		return priceTick{}, false
	}
	tick, ok := tickFromOKXCandle(instrument, response.Data[0])
	return tick, ok
}

func subscribeOKXCandles(ctx context.Context, wsBaseURL string, bar string, instruments []string, out chan<- priceTick) {
	channel := "candle" + bar
	delay := time.Second
	for ctx.Err() == nil {
		if err := connectOKXCandles(ctx, wsBaseURL, channel, instruments, out); err != nil && ctx.Err() == nil {
			log.Printf("okx candle websocket: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < time.Minute {
			delay *= 2
		}
	}
}

func connectOKXCandles(ctx context.Context, wsBaseURL string, channel string, instruments []string, out chan<- priceTick) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsBaseURL+"/ws/v5/business", nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	args := make([]map[string]string, 0, len(instruments))
	for _, instrument := range instruments {
		args = append(args, map[string]string{"channel": channel, "instId": instrument})
	}
	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": args}); err != nil {
		return err
	}
	log.Printf("Connected OKX candle websocket channel=%s instruments=%s", channel, strings.Join(instruments, ","))
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		text := strings.TrimSpace(string(data))
		if text == "ping" {
			if err := conn.WriteMessage(websocket.TextMessage, []byte("pong")); err != nil {
				return err
			}
			continue
		}
		var msg struct {
			Event string `json:"event"`
			Code  string `json:"code"`
			Msg   string `json:"msg"`
			Arg   struct {
				Channel string `json:"channel"`
				InstID  string `json:"instId"`
			} `json:"arg"`
			Data [][]string `json:"data"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}
		if msg.Event == "error" || msg.Code != "" {
			return fmt.Errorf("subscription error %s: %s", msg.Code, msg.Msg)
		}
		for _, row := range msg.Data {
			tick, ok := tickFromOKXCandle(msg.Arg.InstID, row)
			if !ok {
				continue
			}
			select {
			case out <- tick:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func tickFromOKXCandle(instrument string, row []string) (priceTick, bool) {
	if len(row) < 5 {
		return priceTick{}, false
	}
	ms, err := strconv.ParseInt(row[0], 10, 64)
	if err != nil {
		return priceTick{}, false
	}
	price := parseFloat(row[4])
	if price <= 0 {
		return priceTick{}, false
	}
	return priceTick{instrument: instrument, price: price, timestamp: time.UnixMilli(ms).UTC()}, true
}

func getJSON(ctx context.Context, rawURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "grexie-signalsbot-example/0.1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, out)
}

func logOrder(order signalsclient.Order) {
	action := "Order"
	if math.Abs(order.PreviousSize) <= 1e-9 && math.Abs(order.TargetSize) > 1e-9 {
		action = "Position Opened"
	} else if sameSign(order.PreviousSize, order.TargetSize) && math.Abs(order.TargetSize) > math.Abs(order.PreviousSize) {
		action = "Added margin to position"
	} else if sameSign(order.PreviousSize, order.TargetSize) && math.Abs(order.TargetSize) < math.Abs(order.PreviousSize) {
		action = "Removed margin from position"
	} else if math.Abs(order.TargetSize) <= 1e-9 && math.Abs(order.PreviousSize) > 1e-9 {
		action = "Position close order"
	} else if !sameSign(order.PreviousSize, order.TargetSize) {
		action = "Position flip reduction"
	}
	log.Printf("%s instrument=%s side=%s reason=%s delta=%.8f previous=%.8f target=%.8f price=%.8f margin=%s notional=%s fee=%s leverage=%.2f confidence=%.3f expected_edge=%.4f tp=%.4f sl=%.4f reduce_only=%t",
		action,
		order.Instrument,
		order.Side,
		order.Reason,
		order.SizeDelta,
		order.PreviousSize,
		order.TargetSize,
		order.Price,
		money(order.Margin),
		money(order.Notional),
		money(order.EstimatedFeeValue),
		order.Leverage,
		order.Confidence,
		order.ExpectedEdge,
		order.TakeProfit,
		order.StopLoss,
		order.ReduceOnly,
	)
}

func logClosedTrade(trade signalsclient.ClosedTrade, initialEquity float64) {
	accountPct := ratio(trade.RealizedPnL, initialEquity)
	log.Printf("Position Closed instrument=%s side=%s reason=%s pnl=%s realized=%s gross=%s fees=%s entry=%.8f exit=%.8f size=%.8f move=%s mfe=%s mae=%s opened_at=%s closed_at=%s",
		trade.Instrument,
		trade.Side,
		trade.ExitReason,
		percent(accountPct),
		money(trade.RealizedPnL),
		money(trade.RealizedGross),
		money(trade.Fees),
		trade.EntryPrice,
		trade.ExitPrice,
		trade.Size,
		percent(trade.ExitMove),
		percent(trade.MFE),
		percent(trade.MAE),
		trade.OpenedAt.Format(time.RFC3339),
		trade.ClosedAt.Format(time.RFC3339),
	)
}

func envString(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ToUpper(strings.TrimSpace(part))
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func positionKey(venue string, instrument string) string {
	return strings.ToLower(strings.TrimSpace(venue)) + ":" + strings.ToUpper(strings.TrimSpace(instrument))
}

func parseFloat(value string) float64 {
	parsed, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return parsed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func positive(value float64, fallback float64) float64 {
	if value > 0 {
		return value
	}
	return fallback
}

func sameSign(a, b float64) bool {
	if math.Abs(a) <= 1e-9 || math.Abs(b) <= 1e-9 {
		return true
	}
	return (a < 0) == (b < 0)
}

func ratio(value float64, basis float64) float64 {
	if basis == 0 {
		return 0
	}
	return value / basis
}

func money(value float64) string {
	return fmt.Sprintf("%+.2f USDT", value)
}

func percent(value float64) string {
	return fmt.Sprintf("%+.2f%%", value*100)
}
