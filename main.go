// main.go
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
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

// --- Client (下游用戶端) ---

// Client 代表一個連接到我們 Proxy 的下游用戶端
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte // 緩衝的 channel，用於發送訊息
}

// readPump 從 Client 讀取訊息。(用於偵測 Client 斷線)
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Client read error: %v", err)
			}
			break
		}
	}
}

// writePump 將 Hub 來的訊息寫入 Client 的 WebSocket
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// --- Hub (廣播中心) ---

// Hub 管理一個上游連線和多個下游 Client
type Hub struct {
	sourceURL  string
	manager    *RelayManager
	upstream   *websocket.Conn
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	stop       chan struct{} // 用於通知 upstream listener 停止

	// 【修改 1】新增緩衝期計時器
	shutdownTimer *time.Timer
	mu            sync.RWMutex
}

func NewHub(manager *RelayManager, sourceURL string) *Hub {
	return &Hub{
		sourceURL:  sourceURL,
		manager:    manager,
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, broadcastBufferSize),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		stop:       make(chan struct{}),
		// shutdownTimer 預設為 nil
	}
}

// connectUpstream 嘗試連接到上游伺服器
func (h *Hub) connectUpstream() error {
	log.Printf("Hub: Connecting to upstream %s", h.sourceURL)
	c, _, err := websocket.DefaultDialer.Dial(h.sourceURL, nil)
	if err != nil {
		return fmt.Errorf("failed to dial upstream %s: %w", h.sourceURL, err)
	}
	h.upstream = c
	log.Printf("Hub: Connected to upstream %s", h.sourceURL)
	return nil
}

// listenToUpstream 負責從上游讀取並處理重連
func (h *Hub) listenToUpstream() {
	defer func() {
		if h.upstream != nil {
			h.upstream.Close()
		}
		log.Printf("Hub: Upstream listener for %s stopped.", h.sourceURL)
	}()

	for {
		// 檢查是否收到停止訊號
		select {
		case <-h.stop:
			return
		default:
		}

		// 如果連線不存在，嘗試連線
		if h.upstream == nil {
			if err := h.connectUpstream(); err != nil {
				log.Printf("Hub: Failed to connect to %s: %v. Retrying in %v", h.sourceURL, err, reconnectWait)
				// 等待時也檢查停止訊號
				select {
				case <-h.stop:
					return
				case <-time.After(reconnectWait):
					continue // 嘗試重連
				}
			}
			// 連線成功後，重設 ReadDeadline
			h.upstream.SetReadDeadline(time.Time{}) // 清除 ReadDeadline
		}

		// 讀取上游訊息 (阻塞點)
		mt, message, err := h.upstream.ReadMessage()
		if err != nil {
			// 檢查是否是我們主動觸發的錯誤
			if _, ok := err.(*websocket.CloseError); ok || time.Now().After(time.Now().Add(-2*interruptReadDeadline)) {
				// 如果是 CloseError 或是 SetReadDeadline 造成的 I/O timeout
				// 我們檢查 h.stop channel
				select {
				case <-h.stop:
					log.Printf("Hub: Upstream read interrupted by shutdown signal for %s", h.sourceURL)
					return // 正常關閉
				default:
					// 繼續
				}
			}

			log.Printf("Hub: Upstream read error from %s: %v. Reconnecting...", h.sourceURL, err)
			h.upstream.Close()
			h.upstream = nil // 標記為 nil 以觸發重連
			continue         // 返回迴圈頂部
		}

		// 只轉發 Text 和 Binary 訊息
		if mt == websocket.TextMessage || mt == websocket.BinaryMessage {
			select {
			case h.broadcast <- message:
			default:
				log.Printf("Hub: Broadcast channel full for %s. Message dropped.", h.sourceURL)
			}
		}
	}
}

// 【修改 2】重寫 run 方法以包含緩衝期和主動中斷邏輯
func (h *Hub) run() {
	// 啟動上游監聽器
	go h.listenToUpstream()

	var shutdownC <-chan time.Time

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("Client connected. Total clients for %s: %d", h.sourceURL, len(h.clients))

			// (關鍵) 如果有 Client 註冊，取消任何待處理的關閉
			if h.shutdownTimer != nil {
				log.Printf("Hub: Client registered, cancelling shutdown timer for %s", h.sourceURL)
				if !h.shutdownTimer.Stop() {
					// 如果 Stop() 回傳 false，表示計時器已經觸發
					// 為了安全起見，清空 channel
					<-h.shutdownTimer.C
				}
				h.shutdownTimer = nil
				shutdownC = nil // 將 channel 設回 nil
			}

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("Client disconnected. Remaining clients for %s: %d", h.sourceURL, len(h.clients))
			}

			// (關鍵) 檢查是否是最後一個 client
			if len(h.clients) == 0 {
				log.Printf("Hub: Last client disconnected from %s. Starting %v shutdown timer...", h.sourceURL, shutdownGracePeriod)
				// 啟動關閉計時器
				h.shutdownTimer = time.NewTimer(shutdownGracePeriod)
				shutdownC = h.shutdownTimer.C
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					log.Printf("Client send buffer full. Closing connection for client.")
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()

		// (關鍵) 新增 case 來處理計時器觸發
		case <-shutdownC:
			log.Printf("Hub: Shutdown timer expired for %s. Shutting down hub.", h.sourceURL)
			h.mu.Lock()
			// 再次檢查，以防在計時器觸發和此 case 執行之間有新 client 加入
			if len(h.clients) == 0 {

				// 【修改 3】主動中斷 listenToUpstream 中的 ReadMessage()
				if h.upstream != nil {
					log.Printf("Hub: Setting read deadline to interrupt upstream listener for %s", h.sourceURL)
					// 設定一個「立刻」到期的 ReadDeadline，這會使 ReadMessage() 立即返回一個 I/O timeout 錯誤
					h.upstream.SetReadDeadline(time.Now().Add(interruptReadDeadline))
				}

				close(h.stop)                    // 通知 listenToUpstream 停止
				h.manager.RemoveHub(h.sourceURL) // 從管理器中移除自己
				h.mu.Unlock()
				return // 結束 run 迴圈，此 Hub 被銷毀
			}
			log.Printf("Hub: Shutdown timer expired but new client joined. Aborting shutdown.")
			h.mu.Unlock()
			// 如果有新 client，我們就重置計時器 channel
			shutdownC = nil
			h.shutdownTimer = nil
		}
	}
}

// --- RelayManager (Hub 管理器) ---

// RelayManager 管理所有活躍的 Hub
type RelayManager struct {
	hubs map[string]*Hub
	mu   sync.RWMutex
}

func NewRelayManager() *RelayManager {
	return &RelayManager{
		hubs: make(map[string]*Hub),
	}
}

// GetOrCreateHub 獲取或創建一個 Hub
func (m *RelayManager) GetOrCreateHub(sourceURL string) (*Hub, error) {
	m.mu.RLock()
	hub, ok := m.hubs[sourceURL]
	m.mu.RUnlock()
	if ok {
		log.Printf("Manager: Reusing existing hub for %s", sourceURL)
		return hub, nil // Hub 已存在
	}

	// Hub 不存在，需要寫入鎖定
	m.mu.Lock()
	defer m.mu.Unlock()

	// 再次檢查 (Double-check locking)
	hub, ok = m.hubs[sourceURL]
	if ok {
		log.Printf("Manager: Reusing existing hub for %s (double-check)", sourceURL)
		return hub, nil
	}

	// 創建新的 Hub
	log.Printf("Manager: Creating new hub for %s", sourceURL)
	hub = NewHub(m, sourceURL)

	// 在啟動 goroutine 之前先嘗試連接一次
	if err := hub.connectUpstream(); err != nil {
		log.Printf("Manager: Failed initial connect for %s: %v", sourceURL, err)
		return nil, err
	}

	// 連接成功，註冊 Hub 並啟動它
	m.hubs[sourceURL] = hub
	go hub.run() // 啟動 Client 管理和廣播

	return hub, nil
}

// RemoveHub 從管理器中移除 Hub (由 Hub 自己呼叫)
func (m *RelayManager) RemoveHub(sourceURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.hubs, sourceURL)
	log.Printf("Manager: Removed hub for %s", sourceURL)
}

// --- HTTP 伺服器 ---

func handleRelay(m *RelayManager) http.HandlerFunc {
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

func main() {
	manager := NewRelayManager()

	http.HandleFunc("/relay", handleRelay(manager))

	// 【修改 4】從環境變數 "PORT" 獲取埠號
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
