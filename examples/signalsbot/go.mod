module github.com/grexie/signals-client-go/examples/signalsbot

go 1.22

require (
	github.com/gorilla/websocket v1.5.3
	github.com/grexie/signals-client-go v0.0.0
	go.etcd.io/bbolt v1.3.11
)

require golang.org/x/sys v0.4.0 // indirect

replace github.com/grexie/signals-client-go => ../..
