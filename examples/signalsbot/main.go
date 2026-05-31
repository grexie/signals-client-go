package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	signalsclient "github.com/grexie/signals-client-go"
)

const (
	defaultSignalsWSURL = "wss://signals.grexie.com/ws"
	defaultEquity       = 10000.0
)

type config struct {
	token               string
	websocketURL        string
	venue               string
	instruments         []string
	initialEquity       float64
	maxUsage            float64
	profitWithdrawRatio float64
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	loadDotEnv(".env")
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := signalsclient.NewSignalsClient(
		signalsclient.SignalsWebSocketToken(cfg.token),
		signalsclient.WithURL(cfg.websocketURL),
	)
	defer client.Close()

	manager := signalsclient.NewSignalsManager(client, signalsclient.SignalsManagerState{
		Assets: []signalsclient.AssetSnapshot{{
			Venue:     cfg.venue,
			Currency:  "USDT",
			Cash:      cfg.initialEquity,
			Available: cfg.initialEquity,
			Equity:    cfg.initialEquity,
			MaxUsage:  cfg.maxUsage,
			UpdatedAt: time.Now().UTC(),
		}},
	}, signalsclient.SignalsManagerConfig{
		Venue:               cfg.venue,
		Instruments:         cfg.instruments,
		ProfitWithdrawRatio: cfg.profitWithdrawRatio,
		Risk: signalsclient.RiskConfig{
			MaxMarginRatio:         cfg.maxUsage,
			MaxConcurrentPositions: 1,
			MinLeverage:            1,
			MaxLeverage:            1,
		},
	})

	intents := manager.SubscribeIntents(ctx)
	protections := manager.SubscribeProtectionUpdates(ctx)
	withdrawals := manager.SubscribeWithdrawals(ctx)
	events := manager.SubscribeManagerEvents(ctx)
	errs := manager.SubscribeErrors(ctx)

	go func() {
		if err := manager.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("signals manager stopped: %v", err)
		}
	}()

	log.Printf("signalsbot running venue=%s instruments=%s ws=%s", cfg.venue, strings.Join(cfg.instruments, ","), cfg.websocketURL)
	for {
		select {
		case <-ctx.Done():
			return nil
		case intent := <-intents:
			log.Printf("create-market-order intent=%s action=%s instrument=%s side=%s size=%.8f leverage=%.2f reduceOnly=%t tp=%.4f sl=%.4f",
				intent.IntentID, intent.Action, intent.Instrument, intent.Side, intent.ContractSize, intent.Leverage, intent.ReduceOnly, intent.TakeProfit, intent.StopLoss)
		case update := <-protections:
			log.Printf("update-tpsl intent=%s instrument=%s side=%s takeProfitPrice=%.8f stopLossPrice=%.8f", update.IntentID, update.Instrument, update.Side, update.TakeProfitPrice, update.StopLossPrice)
		case withdrawal := <-withdrawals:
			log.Printf("withdraw intent=%s currency=%s amount=%.8f", withdrawal.IntentID, withdrawal.Currency, withdrawal.Amount)
		case event := <-events:
			switch ev := event.(type) {
			case signalsclient.ReadyEvent:
				log.Printf("ready %q", ev.Message)
			case signalsclient.SubscribedEvent:
				log.Printf("subscribed subscription=%d venue=%s", ev.SubscriptionID, ev.Venue)
			case signalsclient.InfoEvent:
				log.Printf("info instrument=%s stage=%s message=%q", ev.Instrument, ev.Stage, ev.Message)
			case signalsclient.ErrorEvent:
				log.Printf("server error code=%s message=%q", ev.Code, ev.Message)
			}
		case err := <-errs:
			if err != nil {
				log.Printf("signals error: %v", err)
			}
		}
	}
}

func loadConfig() (config, error) {
	cfg := config{
		token:               strings.TrimSpace(os.Getenv("SIGNALS_WEBSOCKET_TOKEN")),
		websocketURL:        envString("SIGNALS_WEBSOCKET_URL", defaultSignalsWSURL),
		venue:               strings.ToLower(envString("SIGNALS_VENUE", "okx")),
		instruments:         splitCSV(envString("SIGNALS_INSTRUMENTS", "BTC-USDT-SWAP")),
		initialEquity:       envFloat("SIGNALS_INITIAL_EQUITY", defaultEquity),
		maxUsage:            envFloat("SIGNALS_MAX_USAGE", 1),
		profitWithdrawRatio: envFloat("SIGNALS_PROFIT_WITHDRAW_RATIO", 0),
	}
	if cfg.token == "" {
		return cfg, errors.New("SIGNALS_WEBSOCKET_TOKEN is required")
	}
	if len(cfg.instruments) == 0 {
		return cfg, errors.New("SIGNALS_INSTRUMENTS must contain at least one instrument")
	}
	return cfg, nil
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

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		part = strings.ToUpper(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func envString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		parsed, err := strconv.ParseFloat(value, 64)
		if err == nil {
			return parsed
		}
		fmt.Fprintf(os.Stderr, "Ignoring invalid %s=%q\n", key, value)
	}
	return fallback
}
