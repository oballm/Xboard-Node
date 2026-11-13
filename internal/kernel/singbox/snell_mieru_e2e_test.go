package singbox

// End-to-end for mieru and snell user updates: a real server kernel and a
// real sing-box client per user. Adding, removing or rotating a user must
// not restart the kernel (other users keep their open streams), a removed
// user's open streams end and new ones fail, and traffic is attributed to
// the right user.

import (
	"fmt"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
)

const e2eSnellPSK = "e2e-snell-psk-0123456789"

var e2eSnellMieruProtos = []e2eProto{
	{
		name: "mieru-tcp",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "mieru", ListenIP: "127.0.0.1", ServerPort: p, Transport: "TCP", CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"mieru","tag":"out","server":"127.0.0.1","server_port":%d,"transport":"TCP","username":%q,"password":%q}`, p, u, u)
		},
	},
	{
		name: "mieru-udp",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "mieru", ListenIP: "127.0.0.1", ServerPort: p, Transport: "UDP", CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"mieru","tag":"out","server":"127.0.0.1","server_port":%d,"transport":"UDP","username":%q,"password":%q}`, p, u, u)
		},
		udp: true,
	},
	{
		// Snell clients speak version 4 to a v5 server.
		name: "snell-v5",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "snell", ListenIP: "127.0.0.1", ServerPort: p, ServerKey: e2eSnellPSK, CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"snell","tag":"out","version":4,"server":"127.0.0.1","server_port":%d,"psk":%q,"userkey":%q}`, p, e2eSnellPSK, u)
		},
	},
	{
		name: "snell-v5-tls-obfs",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "snell", Version: 5, Obfs: "tls", ListenIP: "127.0.0.1", ServerPort: p, ServerKey: e2eSnellPSK, CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"snell","tag":"out","version":4,"obfs_mode":"tls","obfs_host":"test.local","server":"127.0.0.1","server_port":%d,"psk":%q,"userkey":%q}`, p, e2eSnellPSK, u)
		},
	},
	{
		// A reused connection carries several requests; each must be checked
		// against the current users.
		name: "snell-v5-reuse",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "snell", ListenIP: "127.0.0.1", ServerPort: p, ServerKey: e2eSnellPSK, CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"snell","tag":"out","version":4,"reuse":true,"server":"127.0.0.1","server_port":%d,"psk":%q,"userkey":%q}`, p, e2eSnellPSK, u)
		},
	},
	{
		name: "snell-v6",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "snell", Version: 6, ListenIP: "127.0.0.1", ServerPort: p, ServerKey: e2eSnellPSK, CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"snell","tag":"out","version":6,"server":"127.0.0.1","server_port":%d,"psk":%q,"userkey":%q}`, p, e2eSnellPSK, u)
		},
	},
}

// A mieru server cuts a user off by closing the server side, and the mieru
// client does not always notice: the local stream stays open but carries
// nothing. A rejected mieru client hangs the same way (the server never
// answers or closes an unauthenticated connection). So judge by data, not by
// whether the stream ends.

// stopsWithin reports whether the stream stops carrying data within d.
func (s *stream) stopsWithin(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		before := s.bytes.Load()
		select {
		case <-s.done:
			return true
		case <-time.After(500 * time.Millisecond):
		}
		if s.bytes.Load() == before {
			return true
		}
	}
	return false
}

// newStreamCarriesNothing opens a stream and checks that no data arrives.
func newStreamCarriesNothing(t *testing.T, clientPort, srcPort int) bool {
	s, err := openStream(t, clientPort, srcPort)
	if err != nil {
		return true
	}
	s.endsWithin(3 * time.Second)
	return s.bytes.Load() == 0
}

func TestE2ESnellMieruUserUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	for _, p := range e2eSnellMieruProtos {
		p := p
		t.Run(p.name, func(t *testing.T) {
			src := startSource(t)
			userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
			userB := model.UserSpec{ID: 2, UUID: e2eUUIDB}
			s, port := startServer(t, p, []model.UserSpec{userA})
			boxBefore := s.box
			clientA := startClient(t, p.outbound(port, e2eUUIDA))
			clientB := startClient(t, p.outbound(port, e2eUUIDB))

			a := mustFlow(t, clientA, src, "A before updates")
			if !newStreamCarriesNothing(t, clientB, src) {
				t.Fatal("B works before being added")
			}

			// Add B: no restart, A keeps flowing, B works.
			if _, _, err := s.UpdateUsers([]model.UserSpec{userA, userB}); err != nil {
				t.Fatalf("UpdateUsers add: %v", err)
			}
			if s.box != boxBefore {
				t.Fatal("adding a user restarted the kernel")
			}
			if !a.flowing(time.Second) {
				t.Fatalf("A's stream broke when B was added (err=%v)", a.err.Load())
			}
			// A client that was turned away may hold a dead connection (mieru
			// leaves a rejected TCP connection open), so its next try can
			// fail; use a fresh client for B.
			clientB = startClient(t, p.outbound(port, e2eUUIDB))
			b := mustFlow(t, clientB, src, "B after added")

			// Remove A: no restart, A's stream ends, A can't open new
			// streams, B unaffected.
			if _, removed, err := s.UpdateUsers([]model.UserSpec{userB}); err != nil || removed != 1 {
				t.Fatalf("UpdateUsers remove: removed=%d err=%v", removed, err)
			}
			if s.box != boxBefore {
				t.Fatal("removing a user restarted the kernel")
			}
			if !a.stopsWithin(3 * time.Second) {
				t.Fatalf("removed user's open stream still carries data (bytes=%d)", a.bytes.Load())
			}
			if !b.flowing(2 * time.Second) {
				t.Fatalf("B's stream broke when A was removed (err=%v)", b.err.Load())
			}
			if !newStreamCarriesNothing(t, clientA, src) {
				t.Fatal("removed user could open a new working stream")
			}

			// Rotate B's UUID: B's old stream ends, the new UUID works.
			rotated := model.UserSpec{ID: 2, UUID: e2eUUIDC}
			if _, _, err := s.UpdateUsers([]model.UserSpec{rotated}); err != nil {
				t.Fatalf("UpdateUsers rotate: %v", err)
			}
			if s.box != boxBefore {
				t.Fatal("rotating a UUID restarted the kernel")
			}
			if !b.stopsWithin(3 * time.Second) {
				t.Fatal("stream on the old UUID still carries data after rotation")
			}
			mustFlow(t, startClient(t, p.outbound(port, e2eUUIDC)), src, "rotated UUID")

			// Traffic went to the right users.
			traffic, _, _ := s.connTracker.GetUserTraffic()
			if traffic[1][1] <= 0 || traffic[2][1] <= 0 {
				t.Fatalf("traffic not attributed to both users: %v", traffic)
			}
		})
	}
}

// A snell node without server_key has no PSK: the kernel must refuse to
// start rather than run an inbound nobody can use.
func TestSnellRequiresPSK(t *testing.T) {
	s := New(testKernelConfig(t))
	node := &model.NodeSpec{Protocol: "snell", ListenIP: "127.0.0.1", ServerPort: freePort(t, "tcp")}
	if err := s.Start(node, []model.UserSpec{{ID: 1, UUID: e2eUUIDA}}, selfSignedCert(t)); err == nil {
		s.Stop()
		t.Fatal("snell node without PSK started")
	}
}
