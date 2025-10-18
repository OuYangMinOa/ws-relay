// hub.go
package ws_relay

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

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

func newHub(manager *RelayManager, sourceURL string) *Hub {
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
