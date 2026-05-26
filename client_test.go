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
	raw := []byte(`{"type":"signal","subscriptionId":7,"venue":"okx","instrument":"BTC-USDT-SWAP","timestamp":"` + ts.Format(time.RFC3339) + `","replay":true,"signal":{"confidence":0.72,"side":"buy","takeProfit":0.01,"stopLoss":0.004,"score":0.32,"predictionMode":"dual-directional","modelVariant":"dual-directional","modelVersion":"dual-cfg-01","confidenceMapping":"evBased","upProbability":0.61,"downProbability":0.42,"directionalEdge":0.19,"normalizedEdge":0.18,"expectedValue":0.0021,"regime":"trend","atrPercent":0.006,"generatedAt":"` + ts.Format(time.RFC3339) + `","artifactID":"okx:btc-usdt-swap:15m:dual-directional","artifactVersion":"v1"}}`)
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
	client := NewSignalsClient("ws_test", WithURL(wsURL))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if ev, err := client.Receive(ctx); err != nil || ev.EventType() != "ready" {
		t.Fatalf("expected ready event, got %#v err=%v", ev, err)
	}
	if err := client.Subscribe(ctx, "okx", "BTC-USDT-SWAP"); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-requests:
		encoded, _ := json.Marshal(msg)
		if msg["type"] != "subscribe" || msg["venue"] != "okx" || msg["instrument"] != "BTC-USDT-SWAP" {
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
	client := NewSignalsClient("ws_test", WithURL(wsURL))
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
	client := NewSignalsClient("ws_test", WithURL(wsURL))
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
