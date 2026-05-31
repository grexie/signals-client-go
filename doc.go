// Package signalsclient provides a typed Go client for Grexie Signals.
//
// New integrations should use SignalsManager with the Bollinger router basket
// websocket protocol. A SignalsClient transport can be shared across multiple
// managers; each manager owns one basket subscription, republishes account
// assets and positions after reconnects, and emits create-market-order,
// update-tpsl, and withdraw intents for client-side venue execution.
//
// The websocket token passed to NewSignalsClient is the SignalsWebSocketToken
// created in the Grexie Signals account UI or API.
package signalsclient
