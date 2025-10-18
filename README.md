# Websocket Relay Server

A high-performance WebSocket relay server written in Go. This server acts as a bridge, connecting to an upstream WebSocket feed and broadcasting the messages to multiple downstream clients. This is particularly useful when you want to distribute a single data stream to many users without each client needing to connect to the source directly, thus reducing the load on the upstream server.

## Features

*   **Dynamic Hub Creation:** A new relay "hub" is created on-demand for each unique upstream `source` URL.
*   **Connection Pooling:** All clients requesting the same `source` URL will share a single connection to the upstream server.
*   **Automatic Reconnection:** If the connection to the upstream server is lost, the hub will automatically try to reconnect.
*   **Graceful Shutdown:** Hubs are automatically shut down and cleaned up after the last client disconnects, saving resources.
*   **Scalable:** The use of Goroutines and channels allows for handling a large number of concurrent clients efficiently.

## How It Works

The relay server has three main components:

1.  **RelayManager:** A global manager that keeps track of all active relay hubs.
2.  **Hub:** A hub is responsible for a single upstream connection. It reads messages from the upstream and broadcasts them to all connected clients.
3.  **Client:** A client represents a single downstream WebSocket connection from a user.

```
+-----------------+      +-----------------+      +-------------------+
| Upstream Server | <--> |      Hub        | <--> |      Client 1     |
+-----------------+      | (Manages Single |      +-------------------+
                         |  Upstream Conn) |      +-------------------+
                         +-----------------+ <--> |      Client 2     |
                                 |                +-------------------+
                                 |                +-------------------+
                                 | <------------> |      Client N     |
                                 |                +-------------------+
                                 |
                        +-------------------+
                        |  RelayManager     |
                        | (Manages all Hubs)|
                        +-------------------+
```

## Getting Started

### Prerequisites

*   [Go](https://golang.org/) (version 1.18 or later)

### Installation & Running

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/webscoket_relay_server.git
    cd webscoket_relay_server
    ```

2.  **Build the application:**
    ```bash
    go build
    ```

3.  **Run the server:**
    ```bash
    ./webscoket_relay_server
    ```

By default, the server will start on port `8080`.

## Usage

To connect to the relay server, you need to use a WebSocket client and provide the `source` URL as a query parameter. The `source` URL is the address of the upstream WebSocket server you want to relay messages from.

**Example:**

If the relay server is running on `localhost:8080` and you want to connect to an upstream server at `wss://stream.example.com/ws`, your connection URL would be:

`ws://localhost:8080/relay?source=wss://stream.example.com/ws`

You can use any WebSocket client, such as the one built into your browser's developer tools or a command-line tool like `websocat`.

## Configuration

The server can be configured using the following environment variable:

*   `PORT`: The port on which the server will run. Defaults to `8080`.

**Example of running on a different port:**

```bash
PORT=9000 ./webscoket_relay_server
```
