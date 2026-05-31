package signalsclient

import (
	"context"
	"errors"
	"testing"
	"time"
)

type managerTestClient struct {
	events            chan Event
	errs              chan error
	subscribed        chan struct{}
	unsubscribeCtxErr chan error
}

func newManagerTestClient() *managerTestClient {
	return &managerTestClient{
		events:            make(chan Event, 8),
		errs:              make(chan error, 8),
		subscribed:        make(chan struct{}),
		unsubscribeCtxErr: make(chan error, 1),
	}
}

func (c *managerTestClient) Subscribe(context.Context, SubscribeRequest) (int64, error) {
	close(c.subscribed)
	return 44, nil
}

func (c *managerTestClient) UpdateAsset(context.Context, int64, AssetSnapshot) error { return nil }
func (c *managerTestClient) UpdatePosition(context.Context, int64, Position) error   { return nil }
func (c *managerTestClient) AddInstrument(context.Context, int64, string) error      { return nil }
func (c *managerTestClient) RemoveInstrument(context.Context, int64, string) error   { return nil }
func (c *managerTestClient) UpdateConfig(context.Context, int64, RuntimeConfig) error {
	return nil
}
func (c *managerTestClient) ScheduleWithdrawal(context.Context, int64, WithdrawalRequest) error {
	return nil
}
func (c *managerTestClient) Unsubscribe(ctx context.Context, _ int64) error {
	c.unsubscribeCtxErr <- ctx.Err()
	return ctx.Err()
}
func (c *managerTestClient) SubscribeEvents(context.Context) (<-chan Event, <-chan error) {
	return c.events, c.errs
}

func TestSignalsManagerFanoutSubscriptionsCloseWithContext(t *testing.T) {
	manager := NewSignalsManager(nil, SignalsManagerState{}, SignalsManagerConfig{Venue: "okx", Instruments: []string{"BTC-USDT-SWAP"}, BufferSize: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := manager.SubscribeIntents(ctx)
	second := manager.SubscribeIntents(ctx)
	events := manager.SubscribeManagerEvents(ctx)
	withdrawals := manager.SubscribeWithdrawals(ctx)
	backtests := manager.SubscribeBacktests(ctx)
	messages := manager.SubscribeMessages(ctx)

	manager.handleEvent(context.Background(), CreateMarketOrderEvent{
		SubscriptionID: 5,
		IntentID:       "intent_1",
		Venue:          "okx",
		Instrument:     "BTC-USDT-SWAP",
		Side:           SideBuy,
		ContractSize:   2,
	})
	manager.handleEvent(context.Background(), WithdrawEvent{
		SubscriptionID: 5,
		IntentID:       "withdraw_1",
		Venue:          "okx",
		Currency:       "USDT",
		Amount:         42,
	})
	manager.handleEvent(context.Background(), BacktestEvent{
		SubscriptionID: 5,
		Venue:          "okx",
		Backtest:       BacktestReport{ID: "bt_1", Venue: "okx", Candidate: BacktestStats{Trades: 3}},
	})
	manager.handleEvent(context.Background(), InfoEvent{
		SubscriptionID: 5,
		Venue:          "okx",
		Instrument:     "BTC-USDT-SWAP",
		Stage:          "backtest",
		Message:        "scheduled backtest queued",
	})

	for name, ch := range map[string]<-chan Intent{"first": first, "second": second} {
		select {
		case intent := <-ch:
			if intent.IntentID != "intent_1" || intent.ContractSize != 2 {
				t.Fatalf("%s subscriber got unexpected intent: %+v", name, intent)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s subscriber did not receive intent fan-out", name)
		}
	}
	select {
	case event := <-events:
		if event.EventType() != "create-market-order" {
			t.Fatalf("unexpected manager event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("manager event subscriber did not receive fan-out")
	}
	select {
	case withdrawal := <-withdrawals:
		if withdrawal.IntentID != "withdraw_1" || withdrawal.Amount != 42 {
			t.Fatalf("unexpected withdrawal fan-out: %+v", withdrawal)
		}
	case <-time.After(time.Second):
		t.Fatal("withdrawal subscriber did not receive fan-out")
	}
	select {
	case backtest := <-backtests:
		if backtest.Backtest.ID != "bt_1" || backtest.Backtest.Candidate.Trades != 3 {
			t.Fatalf("unexpected backtest fan-out: %+v", backtest)
		}
	case <-time.After(time.Second):
		t.Fatal("backtest subscriber did not receive fan-out")
	}
	select {
	case message := <-messages:
		if message.Stage != "backtest" || message.Message == "" {
			t.Fatalf("unexpected message fan-out: %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("message subscriber did not receive fan-out")
	}

	cancel()
	select {
	case _, ok := <-first:
		if ok {
			t.Fatal("intent subscriber stayed open after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("intent subscriber did not close after context cancellation")
	}
}

func TestSignalsManagerRunStopsWhenContextDone(t *testing.T) {
	client := newManagerTestClient()
	manager := NewSignalsManager(client, SignalsManagerState{}, SignalsManagerConfig{Venue: "okx", Instruments: []string{"BTC-USDT-SWAP"}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx)
	}()

	select {
	case <-client.subscribed:
	case <-time.After(time.Second):
		t.Fatal("manager did not subscribe")
	}
	client.events <- SubscribedEvent{SubscriptionID: 44, Venue: "okx"}
	deadline := time.Now().Add(time.Second)
	for manager.SubscriptionID() != 44 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if manager.SubscriptionID() != 44 {
		t.Fatal("manager did not process subscribed event")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
	select {
	case err := <-client.unsubscribeCtxErr:
		if err != nil {
			t.Fatalf("unsubscribe used canceled context: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not attempt context-bound unsubscribe")
	}
}

func TestSignalsManagerFiltersSharedClientEventsBySubscription(t *testing.T) {
	client := newManagerTestClient()
	manager := NewSignalsManager(client, SignalsManagerState{}, SignalsManagerConfig{Venue: "okx", Instruments: []string{"BTC-USDT-SWAP"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx)
	}()
	select {
	case <-client.subscribed:
	case <-time.After(time.Second):
		t.Fatal("manager did not subscribe")
	}
	intents := manager.SubscribeIntents(ctx)
	client.events <- SubscribedEvent{SubscriptionID: 44, Venue: "okx"}
	client.events <- CreateMarketOrderEvent{
		SubscriptionID: 45,
		IntentID:       "wrong_basket",
		Venue:          "okx",
		Instrument:     "ETH-USDT-SWAP",
		Side:           SideBuy,
		ContractSize:   1,
	}
	select {
	case intent := <-intents:
		t.Fatalf("manager accepted another basket event: %+v", intent)
	case <-time.After(50 * time.Millisecond):
	}
	client.events <- CreateMarketOrderEvent{
		SubscriptionID: 44,
		IntentID:       "right_basket",
		Venue:          "okx",
		Instrument:     "BTC-USDT-SWAP",
		Side:           SideBuy,
		ContractSize:   2,
	}
	select {
	case intent := <-intents:
		if intent.IntentID != "right_basket" || intent.ContractSize != 2 {
			t.Fatalf("unexpected intent: %+v", intent)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not emit own basket intent")
	}
	cancel()
	<-done
}
