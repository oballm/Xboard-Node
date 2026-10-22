package panel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// A peer that authenticates and then goes silent looks exactly like a
// half-open connection from the client's side.
func TestWSClient_ReadTimeoutDropsSilentConnection(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var conns atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conns.Add(1)
		_ = conn.WriteJSON(wsMessage{Event: "auth.success"})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	var disconnects atomic.Int32
	ws := NewWSClient("ws://"+strings.TrimPrefix(server.URL, "http://"), "tok", 1,
		WSClientConfig{ReadTimeout: 200 * time.Millisecond, BackoffInitial: 50 * time.Millisecond, BackoffMax: 50 * time.Millisecond},
		nil,
		func(s WSStatusChange) {
			if !s.Connected {
				disconnects.Add(1)
			}
		},
		nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go ws.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if disconnects.Load() >= 1 && conns.Load() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("silent connection not dropped and redialed: disconnects=%d conns=%d", disconnects.Load(), conns.Load())
}
