// Package signalsclient provides a typed Go client for Grexie Signals.
//
// The package has two layers:
//   - SignalsClient manages authenticated websocket subscriptions and emits
//     typed lifecycle, subscription, info, signal, and error events.
//   - PositionManager consumes signal events and maintains an in-memory
//     portfolio using the same confidence-weighted sizing model used by the
//     Grexie Signals server, with configurable fees and leverage limits.
//
// The websocket token passed to NewSignalsClient is the
// SignalsWebSocketToken created in the Grexie Signals account UI or API.
package signalsclient
