package panel

import (
	"encoding/json"
	"testing"

	"github.com/cedar2025/xboard-node/internal/lifecycle"
)

// A control.restart addressed to one node (kernel-only restart) is not
// supported: it must neither reach the service layer nor restart the process.
func TestWSClient_ControlRestartForSingleNodeIgnored(t *testing.T) {
	for _, tc := range []struct {
		name      string
		nodeID    int
		machineID int
	}{
		{"per-node client", 7, 0},
		{"machine client", 0, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []WSEvent
			w := NewWSClient("ws://unused", "tok", tc.nodeID, WSClientConfig{MachineID: tc.machineID}, func(e WSEvent) { got = append(got, e) }, nil, nil)
			for _, data := range []string{`{"node_id":7}`, `{"node_id":"7"}`, `{"node_id":42}`} {
				w.handleMessage(wsMessage{Event: WSEventControlRestart, Data: json.RawMessage(data)})
			}
			if len(got) != 0 {
				t.Fatalf("forwarded %d events, want 0: %#v", len(got), got)
			}
			if lifecycle.RestartRequested() {
				t.Fatal("single-node control.restart must not restart the process")
			}
		})
	}
}

// Malformed payloads are dropped, not treated as "no node_id" (= restart).
func TestWSClient_ControlRestartBadPayloadIgnored(t *testing.T) {
	w := NewWSClient("ws://unused", "tok", 7, WSClientConfig{}, func(WSEvent) {}, nil, nil)
	w.handleMessage(wsMessage{Event: WSEventControlRestart, Data: json.RawMessage(`[1,2`)})
	if lifecycle.RestartRequested() {
		t.Fatal("malformed payload must not restart the process")
	}
}
