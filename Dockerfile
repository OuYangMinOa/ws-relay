FROM golang:1.21-alpine AS builder

# 設定工作目錄
WORKDIR /app

# 複製 go module 檔案並下載依賴
COPY go.mod go.sum ./
RUN go mod download

# 複製所有原始碼
COPY . .

# 編譯應用程式，-ldflags "-w -s" 可以縮小檔案大小
# CGO_ENABLED=0 確保靜態連結
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /ws-relay main.go


# --- 階段 2: 執行 (Final Stage) ---
# 使用一個極小的基礎映像 (alpine)
FROM alpine:latest

# (可選) 安裝 CA 憑證，如果您的 WebSocket 需要連接 wss://
RUN apk --no-cache add ca-certificates

# 從 'builder' 階段複製編譯好的二進位檔案
COPY --from=builder /ws-relay /usr/local/bin/ws-relay

# 暴露我們在程式中設定的預設埠 8080
EXPOSE 8080

# 設定預設執行的命令
CMD ["/usr/local/bin/ws-relay"]