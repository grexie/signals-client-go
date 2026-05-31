package signalsclient

import (
	"math"
	"sort"
	"sync"
	"time"
)

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

// AssetManager tracks cash, available balance, used margin, and equity as an
// exchange account evolves.
type AssetManager struct {
	mu     sync.RWMutex
	assets map[string]AssetSnapshot
}

// NewAssetManager creates an empty asset manager.
func NewAssetManager() *AssetManager {
	return &AssetManager{assets: make(map[string]AssetSnapshot)}
}

// UpdateAsset upserts a currency snapshot.
func (m *AssetManager) UpdateAsset(snapshot AssetSnapshot) {
	if m == nil || snapshot.Currency == "" {
		return
	}
	if snapshot.UpdatedAt.IsZero() {
		snapshot.UpdatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	m.assets[snapshot.Currency] = snapshot
	m.mu.Unlock()
}

// Asset returns one currency snapshot.
func (m *AssetManager) Asset(currency string) (AssetSnapshot, bool) {
	if m == nil {
		return AssetSnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot, ok := m.assets[currency]
	return snapshot, ok
}

// Assets returns all snapshots in stable currency order.
func (m *AssetManager) Assets() []AssetSnapshot {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	assets := make([]AssetSnapshot, 0, len(m.assets))
	for _, asset := range m.assets {
		assets = append(assets, asset)
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].Currency < assets[j].Currency })
	return assets
}

// InstrumentMetadata contains exchange constraints used to turn capital
// allocations into executable lot quantities.
type InstrumentMetadata struct {
	Venue              string
	Instrument         string
	SettlementCurrency string
	LotSize            float64
	MinSize            float64
	TickSize           float64
	ContractValue      float64
	ContractMultiplier float64
	MaxLeverage        float64
}

// InstrumentManager tracks per-instrument lot size, minimum size, tick size,
// settlement currency, and exchange leverage caps.
type InstrumentManager struct {
	mu          sync.RWMutex
	instruments map[string]InstrumentMetadata
}

// NewInstrumentManager creates an empty instrument manager.
func NewInstrumentManager() *InstrumentManager {
	return &InstrumentManager{instruments: make(map[string]InstrumentMetadata)}
}

// UpdateInstrument upserts one instrument metadata snapshot.
func (m *InstrumentManager) UpdateInstrument(metadata InstrumentMetadata) {
	if m == nil || metadata.Venue == "" || metadata.Instrument == "" {
		return
	}
	m.mu.Lock()
	m.instruments[positionKey(metadata.Venue, metadata.Instrument)] = metadata
	m.mu.Unlock()
}

// RemoveInstrument removes one instrument metadata snapshot. PositionManager
// ignores later signals for the venue/instrument once the metadata is removed.
func (m *InstrumentManager) RemoveInstrument(venue, instrument string) {
	if m == nil || venue == "" || instrument == "" {
		return
	}
	m.mu.Lock()
	delete(m.instruments, positionKey(venue, instrument))
	m.mu.Unlock()
}

// Instrument returns metadata for one venue/instrument pair.
func (m *InstrumentManager) Instrument(venue, instrument string) (InstrumentMetadata, bool) {
	if m == nil {
		return InstrumentMetadata{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	metadata, ok := m.instruments[positionKey(venue, instrument)]
	return metadata, ok
}

// Instruments returns all metadata in stable venue/instrument order.
func (m *InstrumentManager) Instruments() []InstrumentMetadata {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	instruments := make([]InstrumentMetadata, 0, len(m.instruments))
	for _, instrument := range m.instruments {
		instruments = append(instruments, instrument)
	}
	sort.Slice(instruments, func(i, j int) bool {
		if instruments[i].Venue == instruments[j].Venue {
			return instruments[i].Instrument < instruments[j].Instrument
		}
		return instruments[i].Venue < instruments[j].Venue
	})
	return instruments
}

func roundDownToStep(value, step float64) float64 {
	if value <= 0 || step <= 0 {
		return value
	}
	return math.Floor(value/step) * step
}

func roundToTick(value, tick float64) float64 {
	if value <= 0 || tick <= 0 {
		return value
	}
	return math.Round(value/tick) * tick
}
