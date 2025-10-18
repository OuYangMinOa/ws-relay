// relay.go
package ws_relay

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// --- 常數設定 ---

const (
	// Client 送 PING 的間隔
	pingPeriod = (pongWait * 9) / 10
	// 等待 Client PONG 的最長時間
	pongWait = 10 * time.Second
	// Client 寫入訊息的等待時間
	writeWait = 10 * time.Second
	// Hub 廣播 channel 的緩衝區大小
	broadcastBufferSize = 256
	// 上游重連間隔
	reconnectWait = 5 * time.Second
	// Hub 關閉緩衝期 (在最後一個 client 離開後)
	shutdownGracePeriod = 10 * time.Second
	// 強制中斷 ReadMessage 的截止時間
	interruptReadDeadline = 100 * time.Millisecond
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// 允許所有來源 (CORS)，在生產環境中您可能需要修改
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func HandleRelay(m *RelayManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sourceURL := r.URL.Query().Get("source")
		if sourceURL == "" {
			http.Error(w, "Query parameter 'source' is required", http.StatusBadRequest)
			return
		}

		hub, err := m.GetOrCreateHub(sourceURL)
		if err != nil {
			log.Printf("Failed to get or create hub for %s: %v", sourceURL, err)
			http.Error(w, fmt.Sprintf("Could not connect to upstream source: %v", err), http.StatusServiceUnavailable)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Failed to upgrade connection: %v", err)
			return
		}

		client := &Client{
			hub:  hub,
			conn: conn,
			send: make(chan []byte, broadcastBufferSize),
		}
		client.hub.register <- client

		go client.writePump()
		go client.readPump()
	}
}
