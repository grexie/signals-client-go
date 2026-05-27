package signalsclient

import (
	"context"
	"testing"
	"time"
)

func TestSharedClientDeduplicatesSubscriptionReferences(t *testing.T) {
	client := NewSharedClient(SharedClientConfig{Enabled: true, Token: "test", Now: func() time.Time { return time.Unix(0, 0).UTC() }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first, err := client.Subscribe(ctx, "OKX", "DOGEUSDT")
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Subscribe(ctx, "okx", "DOGE-USDT-SWAP")
	if err != nil {
		t.Fatal(err)
	}

	key := positionKey("okx", "DOGE-USDT-SWAP")
	client.mu.RLock()
	sub := client.subscriptions[key]
	client.mu.RUnlock()
	if sub == nil || sub.refCount != 2 {
		t.Fatalf("expected one shared subscription with two refs, got %#v", sub)
	}

	first.Close()
	client.mu.RLock()
	sub = client.subscriptions[key]
	client.mu.RUnlock()
	if sub == nil || sub.refCount != 1 {
		t.Fatalf("expected unsubscribe to wait for final ref, got %#v", sub)
	}

	second.Close()
	client.mu.RLock()
	_, ok := client.subscriptions[key]
	client.mu.RUnlock()
	if ok {
		t.Fatalf("expected final close to remove shared subscription")
	}
}

func TestSharedClientSignalStateAndEventFanout(t *testing.T) {
	now := time.Date(2026, 5, 27, 6, 0, 0, 0, time.UTC)
	client := NewSharedClient(SharedClientConfig{Enabled: true, Token: "test", Now: func() time.Time { return now }})
	client.connected = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, _ := client.SubscribeEvents(ctx)
	updates := client.SubscribeUpdates(ctx)

	ev := SignalEvent{
		Venue: "okx", Instrument: "BTC-USDT-SWAP", Timestamp: now,
		Signal: Signal{Venue: "okx", Instrument: "BTC-USDT-SWAP", Side: SideBuy, Confidence: 0.75, Score: 0.4, Price: 100, TakeProfit: 0.02, StopLoss: 0.004},
	}
	client.applyEvent(ev)

	select {
	case got := <-events:
		if got.EventType() != "signal" {
			t.Fatalf("expected signal fanout, got %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event fanout")
	}
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update pulse")
	}

	state, ok := client.SignalState("okx", "BTCUSDT")
	if !ok || state.Instrument != "BTC-USDT-SWAP" || state.Confidence != 0.75 || state.Stale {
		t.Fatalf("unexpected signal state: ok=%v state=%+v", ok, state)
	}
}
