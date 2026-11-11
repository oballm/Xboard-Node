package singbox

// End-to-end coverage of SingBox.Reload and Stop against the real kernel. These
// paths rely on runtime inbound/outbound replace and remove, which sing-box
// 1.14 dropped upstream and the oballm fork restores; a kernel without them
// fails these tests (or no longer compiles).

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
)

// countingForwarder forwards TCP to target and counts accepted connections,
// so a test can tell which upstream a node's outbound used.
func countingForwarder(t *testing.T, target int) (int, *atomic.Int64) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	var n atomic.Int64
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			go func() {
				defer c.Close()
				u, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", target))
				if err != nil {
					return
				}
				defer u.Close()
				go io.Copy(u, c)
				io.Copy(c, u)
			}()
		}
	}()
	return l.Addr().(*net.TCPAddr).Port, &n
}

// upstreamSocks is a socks/http proxy (a plain sing-box mixed inbound) behind
// a counting forwarder; the returned port is what a node outbound dials.
func upstreamSocks(t *testing.T) (int, *atomic.Int64) {
	t.Helper()
	mixed := startClient(t, `{"type":"direct","tag":"out"}`)
	return countingForwarder(t, mixed)
}

func socksOutbound(tag string, port int) model.OutboundConfig {
	return model.OutboundConfig{Tag: tag, Protocol: "socks", Settings: map[string]any{"server": "127.0.0.1", "server_port": port}}
}

func routeLoopbackVia(tag string) []map[string]any {
	return []map[string]any{{"ip_cidr": []string{"127.0.0.1/32"}, "outbound": tag}}
}

func mustFlow(t *testing.T, clientPort, src int, what string) *stream {
	t.Helper()
	s, err := openStream(t, clientPort, src)
	if err != nil {
		t.Fatalf("%s: open stream: %v", what, err)
	}
	if !s.flowing(time.Second) {
		t.Fatalf("%s: stream not flowing (bytes=%d err=%v)", what, s.bytes.Load(), s.err.Load())
	}
	return s
}

// A TLS certificate change recreates the inbound on the same port (Remove then
// Create); new connections work and are still counted for the user.
func TestE2EReloadRecreatesInboundOnCertChange(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[0] // anytls (TLS)
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	s, port := startServer(t, p, []model.UserSpec{userA})
	client := startClient(t, p.outbound(port, e2eUUIDA))
	old := mustFlow(t, client, src, "before reload")

	if err := s.Reload(p.node(port), []model.UserSpec{userA}, selfSignedCert(t)); err != nil {
		t.Fatalf("Reload with new cert: %v", err)
	}
	t.Logf("open stream across inbound recreate: ended=%v", old.endsWithin(500*time.Millisecond))

	before, _, _ := s.connTracker.GetUserTraffic()
	mustFlow(t, client, src, "after recreate")
	after, _, _ := s.connTracker.GetUserTraffic()
	if after[1][1] <= before[1][1] {
		t.Fatalf("traffic after recreate not counted: before=%v after=%v", before[1], after[1])
	}

	// Repeat: each recreate must free the port for the next one.
	for i := 0; i < 5; i++ {
		if err := s.Reload(p.node(port), []model.UserSpec{userA}, selfSignedCert(t)); err != nil {
			t.Fatalf("Reload %d: %v", i, err)
		}
	}
	mustFlow(t, client, src, "after 5 more recreates")
}

// Outbounds are reconciled in place: added, replaced (settings change) and
// removed without restarting the kernel, and traffic follows each change.
func TestE2EReloadReconcilesOutbounds(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[1] // vless (no TLS)
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	users := []model.UserSpec{userA}
	s, port := startServer(t, p, users)
	client := startClient(t, p.outbound(port, e2eUUIDA))
	mustFlow(t, client, src, "direct")
	boxBefore := s.box

	up1, hits1 := upstreamSocks(t)
	up2, hits2 := upstreamSocks(t)

	// Add an outbound and route the source through it.
	node := p.node(port)
	node.CustomOutbounds = []model.OutboundConfig{socksOutbound("via", up1)}
	node.CustomRoutes = routeLoopbackVia("via")
	if err := s.Reload(node, users, selfSignedCert(t)); err != nil {
		t.Fatalf("Reload add outbound: %v", err)
	}
	mustFlow(t, client, src, "via upstream 1")
	if hits1.Load() == 0 {
		t.Fatal("traffic did not use the added outbound")
	}

	// Change the outbound's settings: same tag, replaced in place.
	node2 := p.node(port)
	node2.CustomOutbounds = []model.OutboundConfig{socksOutbound("via", up2)}
	node2.CustomRoutes = routeLoopbackVia("via")
	h1 := hits1.Load()
	if err := s.Reload(node2, users, selfSignedCert(t)); err != nil {
		t.Fatalf("Reload replace outbound: %v", err)
	}
	mustFlow(t, client, src, "via upstream 2")
	if hits2.Load() == 0 || hits1.Load() != h1 {
		t.Fatalf("replaced outbound not used: up1 %d→%d, up2 %d", h1, hits1.Load(), hits2.Load())
	}

	// Remove it and route back to direct.
	h2 := hits2.Load()
	if err := s.Reload(p.node(port), users, selfSignedCert(t)); err != nil {
		t.Fatalf("Reload remove outbound: %v", err)
	}
	mustFlow(t, client, src, "direct again")
	if hits2.Load() != h2 {
		t.Fatal("traffic still went to the removed outbound")
	}
	if _, ok := s.box.Outbound().Outbound("via"); ok {
		t.Fatal("removed outbound still registered")
	}
	if s.box != boxBefore {
		t.Fatal("outbound changes restarted the kernel instead of hot-reloading")
	}
}

// Stop closes the listener first, then the box; the port is free afterwards
// and a new kernel can start on it straight away.
func TestE2EStopFreesPortForRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[1]
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	s, port := startServer(t, p, []model.UserSpec{userA})
	client := startClient(t, p.outbound(port, e2eUUIDA))
	mustFlow(t, client, src, "before stop")

	done := make(chan struct{})
	go func() { s.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Stop hung")
	}
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second); err == nil {
		c.Close()
		t.Fatal("port still listening after Stop")
	}
	if err := s.Start(p.node(port), []model.UserSpec{userA}, selfSignedCert(t)); err != nil {
		t.Fatalf("restart on the same port: %v", err)
	}
	mustFlow(t, client, src, "after restart")
}
