package signalsclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestReconnectingSignalsClientReplaysLatestManagerStateAfterDisconnect(t *testing.T) {
	upgrader := websocket.Upgrader{}
	connections := make(chan *websocket.Conn, 4)
	messages := make(chan map[string]any, 16)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		connections <- conn
		go func() {
			defer conn.Close()
			for {
				var msg map[string]any
				if err := conn.ReadJSON(&msg); err != nil {
					return
				}
				messages <- msg
			}
		}()
	}))
	defer server.Close()

	client := NewReconnectingSignalsClient("ws_test", WithURL("ws"+strings.TrimPrefix(server.URL, "http")), WithBufferSize(16))
	manager := NewSignalsManager(client, SignalsManagerState{}, SignalsManagerConfig{
		Venue:       "okx",
		Instruments: []string{"BTC-USDT-SWAP"},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := manager.SubscribeErrors(ctx)
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx)
	}()

	firstConn := waitReconnectConn(t, connections)
	firstSubscribe := waitReconnectMessageType(t, messages, "subscribe")
	if got := len(asSlice(firstSubscribe["assets"])); got != 0 {
		t.Fatalf("initial subscribe assets = %d, want 0", got)
	}
	if err := firstConn.WriteJSON(map[string]any{"type": "subscribed", "subscriptionId": 101, "venue": "okx", "instrument": "BTC-USDT-SWAP"}); err != nil {
		t.Fatalf("write subscribed: %v", err)
	}

	waitForSubscription(t, manager)
	manager.UpdateAsset(AssetSnapshot{Venue: "okx", Currency: "usdt", Available: 100, Equity: 100, MaxUsage: 0.75})
	manager.UpdatePosition(Position{Venue: "okx", Instrument: "BTC-USDT-SWAP", Size: 2, EntryPrice: 100, LastPrice: 101, Margin: 50, Leverage: 2})
	waitReconnectMessageType(t, messages, "update-asset")
	waitReconnectMessageType(t, messages, "update-position")

	_ = firstConn.Close()
	secondConn := waitReconnectConn(t, connections)
	defer secondConn.Close()
	secondSubscribe := waitReconnectMessageType(t, messages, "subscribe")
	assets := asSlice(secondSubscribe["assets"])
	if len(assets) != 1 || valueAt(assets[0], "currency") != "USDT" {
		t.Fatalf("replayed assets = %#v, want current USDT snapshot", secondSubscribe["assets"])
	}
	positions := asSlice(secondSubscribe["positions"])
	if len(positions) != 1 || firstNonEmpty(valueAt(positions[0], "instrument"), valueAt(positions[0], "Instrument")) != "BTC-USDT-SWAP" {
		t.Fatalf("replayed positions = %#v, want current BTC position", secondSubscribe["positions"])
	}
	select {
	case err := <-errs:
		t.Fatalf("transient reconnect error was surfaced to manager: %v", err)
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not stop")
	}
}

func waitReconnectConn(t *testing.T, ch <-chan *websocket.Conn) *websocket.Conn {
	t.Helper()
	select {
	case conn := <-ch:
		return conn
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for websocket connection")
		return nil
	}
}

func waitReconnectMessageType(t *testing.T, ch <-chan map[string]any, typ string) map[string]any {
	t.Helper()
	deadline := time.After(4 * time.Second)
	for {
		select {
		case msg := <-ch:
			if msg["type"] == typ {
				return msg
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s message", typ)
			return nil
		}
	}
}

func waitForSubscription(t *testing.T, manager *SignalsManager) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if manager.SubscriptionID() > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("manager did not subscribe")
}

func asSlice(value any) []any {
	if slice, ok := value.([]any); ok {
		return slice
	}
	return nil
}

func valueAt(value any, key string) string {
	if m, ok := value.(map[string]any); ok {
		if v, ok := m[key].(string); ok {
			return v
		}
	}
	return ""
}
