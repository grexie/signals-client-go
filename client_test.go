package signalsclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestParseSignalEvent(t *testing.T) {
	ts := time.Date(2026, 5, 26, 1, 2, 3, 0, time.UTC)
	raw := []byte(`{"type":"signal","subscriptionId":7,"venue":"okx","instrument":"BTC-USDT-SWAP","timestamp":"` + ts.Format(time.RFC3339) + `","replay":true,"signal":{"confidence":0.72,"side":"buy","takeProfit":0.01,"stopLoss":0.004,"score":0.32,"predictionMode":"dual-directional","modelVariant":"dual-directional","modelVersion":"dual-cfg-01","confidenceMapping":"evBased","upProbability":0.61,"downProbability":0.42,"directionalEdge":0.19,"normalizedEdge":0.18,"expectedValue":0.0021,"regime":"trend","atrPercent":0.006,"generatedAt":"` + ts.Format(time.RFC3339) + `","artifactID":"okx:btc-usdt-swap:15m:dual-directional","artifactVersion":"v1","managePositionsOnly":true}}`)
	ev, err := ParseEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	signal, ok := ev.(SignalEvent)
	if !ok {
		t.Fatalf("expected SignalEvent, got %T", ev)
	}
	if signal.SubscriptionID != 7 || signal.Signal.Venue != "okx" || signal.Signal.Instrument != "BTC-USDT-SWAP" {
		t.Fatalf("unexpected signal event: %+v", signal)
	}
	if !signal.Replay || !signal.Timestamp.Equal(ts) || signal.Signal.Side != SideBuy {
		t.Fatalf("unexpected replay/timestamp/side: %+v", signal)
	}
	if signal.Signal.PredictionMode != "dual-directional" || signal.Signal.ModelVersion != "dual-cfg-01" || signal.Signal.ConfidenceMapping != "evBased" {
		t.Fatalf("dual metadata not decoded: %+v", signal.Signal)
	}
	if signal.Signal.UpProbability != 0.61 || signal.Signal.DownProbability != 0.42 || signal.Signal.ExpectedValue != 0.0021 {
		t.Fatalf("dual probabilities not decoded: %+v", signal.Signal)
	}
	if !signal.Signal.ManagePositionsOnly {
		t.Fatalf("managePositionsOnly not decoded: %+v", signal.Signal)
	}
}

func TestParseInfoAndErrorEvents(t *testing.T) {
	infoRaw := []byte(`{"type":"info","subscriptionId":9,"venue":"okx","instrument":"ETH-USDT-SWAP","stage":"ready","message":"ready","timestamp":"2026-05-26T00:00:00Z","replay":true,"replayedAt":"2026-05-26T00:00:01Z"}`)
	infoEvent, err := ParseEvent(infoRaw)
	if err != nil {
		t.Fatal(err)
	}
	info, ok := infoEvent.(InfoEvent)
	if !ok {
		t.Fatalf("expected InfoEvent, got %T", infoEvent)
	}
	if info.Stage != "ready" || !info.Replay || info.ReplayedAt == nil {
		t.Fatalf("unexpected info event: %+v", info)
	}

	errorEvent, err := ParseEvent([]byte(`{"type":"error","code":"forbidden","message":"no access"}`))
	if err != nil {
		t.Fatal(err)
	}
	protocolError, ok := errorEvent.(ErrorEvent)
	if !ok {
		t.Fatalf("expected ErrorEvent, got %T", errorEvent)
	}
	if protocolError.Code != "forbidden" || protocolError.Message != "no access" {
		t.Fatalf("unexpected error event: %+v", protocolError)
	}
}

func TestParseOrderRouterEvents(t *testing.T) {
	orderEvent, err := ParseEvent([]byte(`{"type":"create-market-order","subscriptionId":12,"intentId":"intent_1","reason":"preempted_by_better_route","venue":"okx","instrument":"BTC-USDT-SWAP","side":"buy","orderType":"market","contractSize":3,"margin":125.5,"leverage":2,"confidence":0.73}`))
	if err != nil {
		t.Fatal(err)
	}
	order, ok := orderEvent.(CreateMarketOrderEvent)
	if !ok {
		t.Fatalf("expected CreateMarketOrderEvent, got %T", orderEvent)
	}
	if order.SubscriptionID != 12 || order.IntentID != "intent_1" || order.Reason != "preempted_by_better_route" || order.Side != SideBuy || order.ContractSize != 3 || order.Margin != 125.5 || order.Leverage != 2 || order.Confidence != 0.73 {
		t.Fatalf("unexpected order event: %+v", order)
	}

	tpslEvent, err := ParseEvent([]byte(`{"type":"update-tpsl","subscriptionId":12,"intentId":"intent_2","venue":"okx","instrument":"BTC-USDT-SWAP","side":"buy","takeProfitPrice":72100,"stopLossPrice":70050,"takeProfit":0.03,"stopLoss":0.0007}`))
	if err != nil {
		t.Fatal(err)
	}
	tpsl, ok := tpslEvent.(UpdateTPSLEvent)
	if !ok {
		t.Fatalf("expected UpdateTPSLEvent, got %T", tpslEvent)
	}
	if tpsl.SubscriptionID != 12 || tpsl.IntentID != "intent_2" || tpsl.TakeProfitPrice != 72100 || tpsl.StopLossPrice != 70050 || tpsl.TakeProfit != 0.03 || tpsl.StopLoss != 0.0007 {
		t.Fatalf("unexpected update-tpsl event: %+v", tpsl)
	}

	withdrawEvent, err := ParseEvent([]byte(`{"type":"withdraw","subscriptionId":12,"intentId":"withdraw_1","venue":"okx","currency":"USDT","amount":42}`))
	if err != nil {
		t.Fatal(err)
	}
	withdraw, ok := withdrawEvent.(WithdrawEvent)
	if !ok {
		t.Fatalf("expected WithdrawEvent, got %T", withdrawEvent)
	}
	if withdraw.SubscriptionID != 12 || withdraw.IntentID != "withdraw_1" || withdraw.Currency != "USDT" || withdraw.Amount != 42 {
		t.Fatalf("unexpected withdraw event: %+v", withdraw)
	}

	backtestEvent, err := ParseEvent([]byte(`{"type":"backtest","subscriptionId":12,"venue":"okx","instrument":"BASKET:default","backtest":{"id":"bt_1","kind":"scheduled-basket","venue":"okx","instrument":"BASKET:default","generatedAt":"2026-05-31T12:00:00Z","from":"2026-05-24T12:00:00Z","to":"2026-05-31T12:00:00Z","accepted":true,"candidate":{"trades":3,"totalWithUnrealized":0.02,"averageDailyRealized":0.003,"maxDrawdown":0.01,"positiveDays":4,"negativeDays":2,"breakevenDays":1,"instruments":[{"instrument":"BTC-USDT-SWAP","trades":2,"realized":0.01}]}}}`))
	if err != nil {
		t.Fatalf("parse backtest: %v", err)
	}
	backtest, ok := backtestEvent.(BacktestEvent)
	if !ok {
		t.Fatalf("expected BacktestEvent, got %T", backtestEvent)
	}
	if backtest.SubscriptionID != 12 || backtest.Backtest.Candidate.Trades != 3 || len(backtest.Backtest.Candidate.Instruments) != 1 {
		t.Fatalf("unexpected backtest event: %+v", backtest)
	}
}

func TestSignalsClientSubscribe(t *testing.T) {
	upgrader := websocket.Upgrader{}
	requests := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer ws_test" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"type": "ready", "message": "ok"})
		var msg map[string]any
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatal(err)
		}
		requests <- msg
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewWebSocketSignalsClient("ws_test", WithURL(wsURL))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if ev, err := client.Receive(ctx); err != nil || ev.EventType() != "ready" {
		t.Fatalf("expected ready event, got %#v err=%v", ev, err)
	}
	if _, err := client.Subscribe(ctx, SubscribeRequest{Venue: "okx", Instruments: []string{"BTC-USDT-SWAP"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-requests:
		encoded, _ := json.Marshal(msg)
		instruments, _ := msg["instruments"].([]any)
		if msg["type"] != "subscribe" || msg["venue"] != "okx" || len(instruments) != 1 || instruments[0] != "BTC-USDT-SWAP" {
			t.Fatalf("unexpected subscribe request: %s", encoded)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestSignalsClientFansOutEvents(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"type": "ready", "message": "ok"})
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewWebSocketSignalsClient("ws_test", WithURL(wsURL))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first, _ := client.SubscribeEvents(ctx)
	second, _ := client.SubscribeEvents(ctx)
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for name, events := range map[string]<-chan Event{"first": first, "second": second} {
		select {
		case ev := <-events:
			if ev.EventType() != "ready" {
				t.Fatalf("%s subscriber got unexpected event %#v", name, ev)
			}
		case <-ctx.Done():
			t.Fatalf("%s subscriber did not receive fan-out event: %v", name, ctx.Err())
		}
	}
}

func TestSignalsClientUnsubscribe(t *testing.T) {
	upgrader := websocket.Upgrader{}
	requests := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var msg map[string]any
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatal(err)
		}
		requests <- msg
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewWebSocketSignalsClient("ws_test", WithURL(wsURL))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Unsubscribe(ctx, 44); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-requests:
		if msg["type"] != "unsubscribe" || msg["subscriptionId"].(float64) != 44 {
			t.Fatalf("unexpected unsubscribe request: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
