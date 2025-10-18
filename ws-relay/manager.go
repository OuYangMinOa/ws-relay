// manager.go
package ws_relay

import (
	"log"
	"sync"
)

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
	hub = newHub(m, sourceURL)

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
