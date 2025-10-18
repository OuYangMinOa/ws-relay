package main

import (
	"log"
	"net/http"
	"os"

	"webscoket_relay_server/ws-relay"
)

func main() {
	manager := ws_relay.NewRelayManager()

	http.HandleFunc("/relay", ws_relay.HandleRelay(manager))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // 預設值
	}

	log.Printf("Starting WebSocket relay server on :%s", port)
	log.Printf("Connect clients to: ws://localhost:%s/relay?source=UPSTREAM_WS_URL", port)

	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
