package signalsclient

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// SignalsWebSocketToken authenticates a websocket connection to Grexie Signals.
type SignalsWebSocketToken string

// Side is the direction produced by a signal or held by a position.
type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

// SignalComponent describes one timeframe contribution to an aggregate signal.
type SignalComponent struct {
	Timeframe   string    `json:"timeframe"`
	Side        Side      `json:"side"`
	Confidence  float64   `json:"confidence"`
	Weight      float64   `json:"weight"`
	SignedScore float64   `json:"signedScore"`
	TakeProfit  float64   `json:"takeProfit"`
	StopLoss    float64   `json:"stopLoss"`
	Probability []float64 `json:"probability,omitempty"`
}

// Signal is the public signal payload sent by the Grexie Signals websocket.
//
// Price and Timestamp are optional forward-compatible fields.
type Signal struct {
	Venue                  string            `json:"venue"`
	Instrument             string            `json:"instrument"`
	Timeframe              string            `json:"timeframe,omitempty"`
	Confidence             float64           `json:"confidence"`
	Side                   Side              `json:"side"`
	TakeProfit             float64           `json:"takeProfit"`
	StopLoss               float64           `json:"stopLoss"`
	TrailingStopActivation float64           `json:"trailingStopActivation,omitempty"`
	TrailingStopDistance   float64           `json:"trailingStopDistance,omitempty"`
	TrailingStopMinProfit  float64           `json:"trailingStopMinProfit,omitempty"`
	Score                  float64           `json:"score,omitempty"`
	Components             []SignalComponent `json:"components,omitempty"`
	ModelVariant           string            `json:"modelVariant,omitempty"`
	ModelVersion           string            `json:"modelVersion,omitempty"`
	PredictionMode         string            `json:"predictionMode,omitempty"`
	ConfidenceMapping      string            `json:"confidenceMapping,omitempty"`
	UpProbability          float64           `json:"upProbability,omitempty"`
	DownProbability        float64           `json:"downProbability,omitempty"`
	DirectionalEdge        float64           `json:"directionalEdge,omitempty"`
	NormalizedEdge         float64           `json:"normalizedEdge,omitempty"`
	ExpectedValue          float64           `json:"expectedValue,omitempty"`
	Regime                 string            `json:"regime,omitempty"`
	RegimeConfidence       float64           `json:"regimeConfidence,omitempty"`
	VolatilityState        string            `json:"volatilityState,omitempty"`
	SqueezeState           string            `json:"squeezeState,omitempty"`
	TrendState             string            `json:"trendState,omitempty"`
	ATRPercent             float64           `json:"atrPercent,omitempty"`
	SignalTTL              time.Duration     `json:"signalTTL,omitempty"`
	GeneratedAt            time.Time         `json:"generatedAt,omitempty"`
	ArtifactID             string            `json:"artifactID,omitempty"`
	ArtifactVersion        string            `json:"artifactVersion,omitempty"`
	RejectedReason         string            `json:"rejectedReason,omitempty"`
	ManagePositionsOnly    bool              `json:"managePositionsOnly,omitempty"`
	Timestamp              time.Time         `json:"timestamp,omitempty"`
	Price                  float64           `json:"price,omitempty"`
}

// AssetSnapshot is the current account state for a settlement currency.
type AssetSnapshot struct {
	Venue     string    `json:"venue,omitempty"`
	Currency  string    `json:"currency"`
	Cash      float64   `json:"cash"`
	Available float64   `json:"available"`
	Used      float64   `json:"used"`
	Equity    float64   `json:"equity"`
	MaxUsage  float64   `json:"maxUsage,omitempty"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
}

// Position is the current venue position for one instrument.
type Position struct {
	Venue                  string
	Instrument             string
	Status                 string
	Size                   float64
	Confidence             float64
	EntryPrice             float64
	LastPrice              float64
	TakeProfit             float64
	StopLoss               float64
	TakeProfitPrice        float64
	StopLossPrice          float64
	TrailingStopActivation float64
	TrailingStopDistance   float64
	TrailingStopMinProfit  float64
	Leverage               float64
	MFE                    float64
	MAE                    float64
	RealizedGross          float64
	Fees                   float64
	RealizedPnL            float64
	OpenedAt               time.Time
	LastSignalAt           time.Time
}

// Side returns the current position direction.
func (p Position) Side() Side {
	if p.Size < 0 {
		return SideSell
	}
	if p.Size > 0 {
		return SideBuy
	}
	return ""
}

// UnrealizedPnL returns approximate settlement-currency PnL for linear
// instruments that have a contract value of 1.
func (p Position) UnrealizedPnL() float64 {
	return p.move() * math.Abs(p.Size) * positiveOr(p.EntryPrice, 1)
}

func (p Position) move() float64 {
	if p.EntryPrice == 0 || p.LastPrice == 0 {
		return 0
	}
	if p.Size < 0 {
		return (p.EntryPrice - p.LastPrice) / p.EntryPrice
	}
	return (p.LastPrice - p.EntryPrice) / p.EntryPrice
}

// Event is implemented by every websocket event emitted by SignalsClient.
type Event interface {
	EventType() string
}

// ReadyEvent is sent when the server is ready to receive subscribe messages.
type ReadyEvent struct {
	Message string `json:"message"`
}

func (ReadyEvent) EventType() string { return "ready" }

// SubscribedEvent confirms a subscription and carries the server subscription id.
type SubscribedEvent struct {
	SubscriptionID int64  `json:"subscriptionId"`
	Venue          string `json:"venue"`
	Instrument     string `json:"instrument"`
}

func (SubscribedEvent) EventType() string { return "subscribed" }

// UnsubscribedEvent confirms that a subscription has been removed.
type UnsubscribedEvent struct {
	SubscriptionID int64  `json:"subscriptionId"`
	Venue          string `json:"venue"`
	Instrument     string `json:"instrument"`
	Code           string `json:"code,omitempty"`
	Message        string `json:"message,omitempty"`
}

func (UnsubscribedEvent) EventType() string { return "unsubscribed" }

// CreateMarketOrderEvent asks the client-side venue executor to submit a
// market order. Execution still lives with the client/user, not the Signals
// server.
type CreateMarketOrderEvent struct {
	SubscriptionID  int64     `json:"subscriptionId"`
	IntentID        string    `json:"intentId,omitempty"`
	Action          string    `json:"action,omitempty"`
	Reason          string    `json:"reason,omitempty"`
	Venue           string    `json:"venue,omitempty"`
	Instrument      string    `json:"instrument"`
	Side            Side      `json:"side"`
	OrderType       string    `json:"orderType,omitempty"`
	ContractSize    float64   `json:"contractSize,omitempty"`
	Leverage        float64   `json:"leverage,omitempty"`
	ReduceOnly      bool      `json:"reduceOnly,omitempty"`
	TakeProfitPrice float64   `json:"takeProfitPrice,omitempty"`
	StopLossPrice   float64   `json:"stopLossPrice,omitempty"`
	TakeProfit      float64   `json:"takeProfit,omitempty"`
	StopLoss        float64   `json:"stopLoss,omitempty"`
	Timestamp       time.Time `json:"timestamp,omitempty"`
}

func (CreateMarketOrderEvent) EventType() string { return "create-market-order" }

// UpdateTPSLEvent asks the client-side venue executor to update take-profit
// and/or stop-loss for an existing venue/instrument/side position.
type UpdateTPSLEvent struct {
	SubscriptionID  int64     `json:"subscriptionId"`
	IntentID        string    `json:"intentId,omitempty"`
	Venue           string    `json:"venue,omitempty"`
	Instrument      string    `json:"instrument"`
	Side            Side      `json:"side"`
	TakeProfitPrice float64   `json:"takeProfitPrice,omitempty"`
	StopLossPrice   float64   `json:"stopLossPrice,omitempty"`
	TakeProfit      float64   `json:"takeProfit,omitempty"`
	StopLoss        float64   `json:"stopLoss,omitempty"`
	Timestamp       time.Time `json:"timestamp,omitempty"`
}

func (UpdateTPSLEvent) EventType() string { return "update-tpsl" }

// WithdrawEvent asks the client to withdraw a currency amount after the router
// has made room for scheduled or profit-sharing withdrawals.
type WithdrawEvent struct {
	SubscriptionID int64     `json:"subscriptionId"`
	IntentID       string    `json:"intentId,omitempty"`
	Venue          string    `json:"venue,omitempty"`
	Currency       string    `json:"currency"`
	Amount         float64   `json:"amount"`
	Timestamp      time.Time `json:"timestamp,omitempty"`
}

func (WithdrawEvent) EventType() string { return "withdraw" }

// InfoEvent is a user-friendly lifecycle update for an instrument.
type InfoEvent struct {
	SubscriptionID int64      `json:"subscriptionId"`
	Venue          string     `json:"venue"`
	Instrument     string     `json:"instrument"`
	Stage          string     `json:"stage"`
	Message        string     `json:"message"`
	Timestamp      time.Time  `json:"timestamp"`
	Replay         bool       `json:"replay,omitempty"`
	ReplayedAt     *time.Time `json:"replayedAt,omitempty"`
}

func (InfoEvent) EventType() string { return "info" }

// BacktestInstrumentResult is an instrument-level result from a scheduled
// basket/router backtest.
type BacktestInstrumentResult struct {
	Instrument                       string        `json:"instrument"`
	Signals                          int           `json:"signals"`
	BuySignals                       int           `json:"buySignals"`
	SellSignals                      int           `json:"sellSignals"`
	Trades                           int           `json:"trades"`
	Orders                           int           `json:"orders"`
	Wins                             int           `json:"wins"`
	Losses                           int           `json:"losses"`
	Realized                         float64       `json:"realized"`
	Unrealized                       float64       `json:"unrealized"`
	Fees                             float64       `json:"fees"`
	TotalWithUnrealized              float64       `json:"totalWithUnrealized"`
	MaxDrawdown                      float64       `json:"maxDrawdown"`
	AverageDailyRealized             float64       `json:"averageDailyRealized"`
	GeometricAverageDaily            float64       `json:"geometricAverageDaily"`
	Days                             int           `json:"days"`
	PositiveDays                     int           `json:"positiveDays"`
	NegativeDays                     int           `json:"negativeDays"`
	BreakevenDays                    int           `json:"breakevenDays"`
	ProfitableDayRate                float64       `json:"profitableDayRate"`
	TradingDays                      int           `json:"tradingDays"`
	TradingDayRate                   float64       `json:"tradingDayRate"`
	AverageTradingDayRealized        float64       `json:"averageTradingDayRealized"`
	AverageWinningTradingDayRealized float64       `json:"averageWinningTradingDayRealized"`
	AverageLosingTradingDayRealized  float64       `json:"averageLosingTradingDayRealized"`
	ProfitFactor                     float64       `json:"profitFactor"`
	AverageHold                      time.Duration `json:"averageHold"`
	AverageTimeBetweenPositions      time.Duration `json:"averageTimeBetweenPositions"`
	TotalTimeInPosition              time.Duration `json:"totalTimeInPosition"`
	AverageDailyTimeInPosition       time.Duration `json:"averageDailyTimeInPosition"`
	TimeInPositionRate               float64       `json:"timeInPositionRate"`
}

// BacktestStats summarizes one scheduled basket/router backtest.
type BacktestStats struct {
	Signals                     int                        `json:"signals"`
	BuySignals                  int                        `json:"buySignals"`
	SellSignals                 int                        `json:"sellSignals"`
	Orders                      int                        `json:"orders"`
	Trades                      int                        `json:"trades"`
	Wins                        int                        `json:"wins"`
	Losses                      int                        `json:"losses"`
	WinRate                     float64                    `json:"winRate"`
	Realized                    float64                    `json:"realized"`
	Unrealized                  float64                    `json:"unrealized"`
	Fees                        float64                    `json:"fees"`
	TotalWithUnrealized         float64                    `json:"totalWithUnrealized"`
	MaxDrawdown                 float64                    `json:"maxDrawdown"`
	ProfitFactor                float64                    `json:"profitFactor"`
	AverageDailyRealized        float64                    `json:"averageDailyRealized"`
	GeometricAverageDaily       float64                    `json:"geometricAverageDaily"`
	AverageTimeBetweenPositions time.Duration              `json:"averageTimeBetweenPositions"`
	TotalTimeInPosition         time.Duration              `json:"totalTimeInPosition"`
	AverageDailyTimeInPosition  time.Duration              `json:"averageDailyTimeInPosition"`
	TimeInPositionRate          float64                    `json:"timeInPositionRate"`
	Days                        int                        `json:"days"`
	PositiveDays                int                        `json:"positiveDays"`
	NegativeDays                int                        `json:"negativeDays"`
	BreakevenDays               int                        `json:"breakevenDays"`
	ProfitableDayRate           float64                    `json:"profitableDayRate"`
	GoalDays                    int                        `json:"goalDays"`
	GoalDayRate                 float64                    `json:"goalDayRate"`
	Instruments                 []BacktestInstrumentResult `json:"instruments"`
}

// BacktestReport is a compact scheduled backtest payload emitted for a
// SignalsManager basket subscription.
type BacktestReport struct {
	ID                        string        `json:"id"`
	Kind                      string        `json:"kind,omitempty"`
	Venue                     string        `json:"venue"`
	Instrument                string        `json:"instrument"`
	BasketID                  string        `json:"basketId,omitempty"`
	Instruments               []string      `json:"instruments,omitempty"`
	ModelVariant              string        `json:"modelVariant,omitempty"`
	ModelConfig               string        `json:"modelConfig,omitempty"`
	Timeframes                []string      `json:"timeframes,omitempty"`
	GeneratedAt               time.Time     `json:"generatedAt"`
	From                      time.Time     `json:"from"`
	To                        time.Time     `json:"to"`
	Accepted                  bool          `json:"accepted"`
	Reason                    string        `json:"reason,omitempty"`
	Candidate                 BacktestStats `json:"candidate"`
	DeltaRealized             float64       `json:"deltaRealized"`
	DeltaTotalWithUnrealized  float64       `json:"deltaTotalWithUnrealized"`
	DeltaAverageDailyRealized float64       `json:"deltaAverageDailyRealized"`
}

// BacktestEvent carries an hourly scheduled basket/router backtest for one
// SignalsManager subscription.
type BacktestEvent struct {
	SubscriptionID int64          `json:"subscriptionId"`
	Venue          string         `json:"venue,omitempty"`
	Instrument     string         `json:"instrument,omitempty"`
	Backtest       BacktestReport `json:"backtest"`
	Timestamp      time.Time      `json:"timestamp,omitempty"`
}

func (BacktestEvent) EventType() string { return "backtest" }

// SignalEvent carries a trading signal for a subscribed venue/instrument pair.
type SignalEvent struct {
	SubscriptionID int64      `json:"subscriptionId"`
	Venue          string     `json:"venue"`
	Instrument     string     `json:"instrument"`
	Signal         Signal     `json:"signal"`
	Timestamp      time.Time  `json:"timestamp"`
	Replay         bool       `json:"replay,omitempty"`
	ReplayedAt     *time.Time `json:"replayedAt,omitempty"`
}

func (SignalEvent) EventType() string { return "signal" }

// ErrorEvent is emitted for protocol, authorization, and server errors.
type ErrorEvent struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (ErrorEvent) EventType() string { return "error" }

type serverMessage struct {
	Type            string          `json:"type"`
	SubscriptionID  int64           `json:"subscriptionId,omitempty"`
	Venue           string          `json:"venue,omitempty"`
	Instrument      string          `json:"instrument,omitempty"`
	Code            string          `json:"code,omitempty"`
	Message         string          `json:"message,omitempty"`
	Stage           string          `json:"stage,omitempty"`
	Timestamp       *time.Time      `json:"timestamp,omitempty"`
	Replay          bool            `json:"replay,omitempty"`
	ReplayedAt      *time.Time      `json:"replayedAt,omitempty"`
	Signal          json.RawMessage `json:"signal,omitempty"`
	Backtest        json.RawMessage `json:"backtest,omitempty"`
	IntentID        string          `json:"intentId,omitempty"`
	Action          string          `json:"action,omitempty"`
	Reason          string          `json:"reason,omitempty"`
	Side            Side            `json:"side,omitempty"`
	OrderType       string          `json:"orderType,omitempty"`
	ContractSize    float64         `json:"contractSize,omitempty"`
	Leverage        float64         `json:"leverage,omitempty"`
	ReduceOnly      bool            `json:"reduceOnly,omitempty"`
	TakeProfitPrice float64         `json:"takeProfitPrice,omitempty"`
	StopLossPrice   float64         `json:"stopLossPrice,omitempty"`
	TakeProfit      float64         `json:"takeProfit,omitempty"`
	StopLoss        float64         `json:"stopLoss,omitempty"`
	Currency        string          `json:"currency,omitempty"`
	Amount          float64         `json:"amount,omitempty"`
}

// ParseEvent decodes one raw websocket JSON message into the corresponding
// typed event.
func ParseEvent(data []byte) (Event, error) {
	var msg serverMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	switch msg.Type {
	case "ready":
		return ReadyEvent{Message: msg.Message}, nil
	case "subscribed":
		return SubscribedEvent{SubscriptionID: msg.SubscriptionID, Venue: msg.Venue, Instrument: msg.Instrument}, nil
	case "unsubscribed":
		return UnsubscribedEvent{SubscriptionID: msg.SubscriptionID, Venue: msg.Venue, Instrument: msg.Instrument, Code: msg.Code, Message: msg.Message}, nil
	case "create-market-order":
		ts := time.Time{}
		if msg.Timestamp != nil {
			ts = *msg.Timestamp
		}
		return CreateMarketOrderEvent{
			SubscriptionID:  msg.SubscriptionID,
			IntentID:        msg.IntentID,
			Action:          msg.Action,
			Reason:          msg.Reason,
			Venue:           msg.Venue,
			Instrument:      msg.Instrument,
			Side:            msg.Side,
			OrderType:       msg.OrderType,
			ContractSize:    msg.ContractSize,
			Leverage:        msg.Leverage,
			ReduceOnly:      msg.ReduceOnly,
			TakeProfitPrice: msg.TakeProfitPrice,
			StopLossPrice:   msg.StopLossPrice,
			TakeProfit:      msg.TakeProfit,
			StopLoss:        msg.StopLoss,
			Timestamp:       ts,
		}, nil
	case "update-tpsl":
		ts := time.Time{}
		if msg.Timestamp != nil {
			ts = *msg.Timestamp
		}
		return UpdateTPSLEvent{
			SubscriptionID:  msg.SubscriptionID,
			IntentID:        msg.IntentID,
			Venue:           msg.Venue,
			Instrument:      msg.Instrument,
			Side:            msg.Side,
			TakeProfitPrice: msg.TakeProfitPrice,
			StopLossPrice:   msg.StopLossPrice,
			TakeProfit:      msg.TakeProfit,
			StopLoss:        msg.StopLoss,
			Timestamp:       ts,
		}, nil
	case "withdraw":
		ts := time.Time{}
		if msg.Timestamp != nil {
			ts = *msg.Timestamp
		}
		return WithdrawEvent{
			SubscriptionID: msg.SubscriptionID,
			IntentID:       msg.IntentID,
			Venue:          msg.Venue,
			Currency:       msg.Currency,
			Amount:         msg.Amount,
			Timestamp:      ts,
		}, nil
	case "info":
		ts := time.Time{}
		if msg.Timestamp != nil {
			ts = *msg.Timestamp
		}
		return InfoEvent{
			SubscriptionID: msg.SubscriptionID,
			Venue:          msg.Venue,
			Instrument:     msg.Instrument,
			Stage:          msg.Stage,
			Message:        msg.Message,
			Timestamp:      ts,
			Replay:         msg.Replay,
			ReplayedAt:     msg.ReplayedAt,
		}, nil
	case "backtest":
		ts := time.Time{}
		if msg.Timestamp != nil {
			ts = *msg.Timestamp
		}
		var report BacktestReport
		if len(msg.Backtest) > 0 {
			if err := json.Unmarshal(msg.Backtest, &report); err != nil {
				return nil, err
			}
		}
		if report.Venue == "" {
			report.Venue = msg.Venue
		}
		if report.Instrument == "" {
			report.Instrument = msg.Instrument
		}
		if ts.IsZero() {
			ts = report.GeneratedAt
		}
		return BacktestEvent{
			SubscriptionID: msg.SubscriptionID,
			Venue:          msg.Venue,
			Instrument:     msg.Instrument,
			Backtest:       report,
			Timestamp:      ts,
		}, nil
	case "signal":
		var signal Signal
		if len(msg.Signal) > 0 {
			if err := json.Unmarshal(msg.Signal, &signal); err != nil {
				return nil, err
			}
		}
		if signal.Venue == "" {
			signal.Venue = msg.Venue
		}
		if signal.Instrument == "" {
			signal.Instrument = msg.Instrument
		}
		if msg.Timestamp != nil && signal.Timestamp.IsZero() {
			signal.Timestamp = *msg.Timestamp
		}
		ts := signal.Timestamp
		if ts.IsZero() && msg.Timestamp != nil {
			ts = *msg.Timestamp
		}
		return SignalEvent{
			SubscriptionID: msg.SubscriptionID,
			Venue:          msg.Venue,
			Instrument:     msg.Instrument,
			Signal:         signal,
			Timestamp:      ts,
			Replay:         msg.Replay,
			ReplayedAt:     msg.ReplayedAt,
		}, nil
	case "error":
		return ErrorEvent{Code: msg.Code, Message: msg.Message}, nil
	default:
		return nil, fmt.Errorf("signalsclient: unsupported websocket event type %q", msg.Type)
	}
}
