package singbox

// End-to-end coverage of SingBox.Reload, Start and Stop against the real
// kernel. Users are hot-swapped; every other change restarts the box.

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/model"
)

func testKernelConfig(t *testing.T) config.KernelConfig {
	return config.KernelConfig{Type: "singbox", LogLevel: "error", ConfigDir: t.TempDir()}
}

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

func selectorOutbound(tag string, members ...string) model.OutboundConfig {
	return model.OutboundConfig{Tag: tag, Protocol: "selector", Settings: map[string]any{"outbounds": members}}
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

// mustFlowAfterRestart opens a stream after the server restarted. A client
// may first try a pooled session the old server closed (anytls reuses idle
// sessions), so a failed first attempt is retried, as real clients do.
func mustFlowAfterRestart(t *testing.T, clientPort, src int, what string) *stream {
	t.Helper()
	var last error
	for i := 0; i < 3; i++ {
		s, err := openStream(t, clientPort, src)
		if err == nil && s.flowing(time.Second) {
			return s
		}
		if err == nil {
			err = fmt.Errorf("not flowing (bytes=%d err=%v)", s.bytes.Load(), s.err.Load())
		}
		last = err
	}
	t.Fatalf("%s: %v", what, last)
	return nil
}

// A config push that changes nothing but users is applied in place: the box
// is kept, open streams keep flowing and the added user can connect.
func TestE2EReloadUsersOnlyKeepsKernel(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[0] // anytls (TLS)
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	userB := model.UserSpec{ID: 2, UUID: e2eUUIDB}
	cert := selfSignedCert(t)
	s := New(testKernelConfig(t))
	port := freePort(t, "tcp")
	if err := s.Start(p.node(port), []model.UserSpec{userA}, cert); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	clientA := startClient(t, p.outbound(port, e2eUUIDA))
	old := mustFlow(t, clientA, src, "before reload")
	boxBefore := s.box

	// Same node config (a fresh but equal object) and same certificate.
	if err := s.Reload(p.node(port), []model.UserSpec{userA, userB}, cert); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if s.box != boxBefore {
		t.Fatal("users-only reload restarted the kernel")
	}
	if !old.flowing(time.Second) {
		t.Fatalf("open stream broke on users-only reload (err=%v)", old.err.Load())
	}
	clientB := startClient(t, p.outbound(port, e2eUUIDB))
	mustFlow(t, clientB, src, "added user")
}

// A new certificate restarts the box on the same port; new connections work
// and are counted.
func TestE2EReloadCertChangeRestartsKernel(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[0] // anytls (TLS)
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	s, port := startServer(t, p, []model.UserSpec{userA})
	client := startClient(t, p.outbound(port, e2eUUIDA))
	mustFlow(t, client, src, "before reload")
	boxBefore := s.box

	for i := 0; i < 3; i++ {
		if err := s.Reload(p.node(port), []model.UserSpec{userA}, selfSignedCert(t)); err != nil {
			t.Fatalf("Reload %d with new cert: %v", i, err)
		}
	}
	if s.box == boxBefore {
		t.Fatal("certificate change did not restart the kernel")
	}
	mustFlowAfterRestart(t, client, src, "after restart")
	traffic, _, _ := s.connTracker.GetUserTraffic()
	if traffic[1][1] == 0 {
		t.Fatalf("traffic after restart not counted: %v", traffic[1])
	}
}

// Outbound changes restart the box, and traffic follows each change. The
// replace step changes only a selector's member: the case where a hot
// replace would leave the selector holding the closed old member.
func TestE2EReloadOutboundChangeRestartsKernel(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[1] // vless (no TLS)
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	users := []model.UserSpec{userA}
	cert := selfSignedCert(t)
	s := New(testKernelConfig(t))
	port := freePort(t, "tcp")
	if err := s.Start(p.node(port), users, cert); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	client := startClient(t, p.outbound(port, e2eUUIDA))
	mustFlow(t, client, src, "direct")

	up1, hits1 := upstreamSocks(t)
	up2, hits2 := upstreamSocks(t)

	withOutbound := func(upPort int) *model.NodeSpec {
		n := p.node(port)
		n.CustomOutbounds = []model.OutboundConfig{socksOutbound("up", upPort), selectorOutbound("via", "up")}
		n.CustomRoutes = routeLoopbackVia("via")
		return n
	}

	box0 := s.box
	if err := s.Reload(withOutbound(up1), users, cert); err != nil {
		t.Fatalf("Reload add outbound: %v", err)
	}
	if s.box == box0 {
		t.Fatal("adding an outbound did not restart the kernel")
	}
	mustFlowAfterRestart(t, client, src, "via upstream 1")
	if hits1.Load() == 0 {
		t.Fatal("traffic did not use the added outbound")
	}

	// Pushing the same outbounds again is not a change.
	box1 := s.box
	if err := s.Reload(withOutbound(up1), users, cert); err != nil {
		t.Fatalf("Reload unchanged: %v", err)
	}
	if s.box != box1 {
		t.Fatal("unchanged config restarted the kernel")
	}

	h1 := hits1.Load()
	if err := s.Reload(withOutbound(up2), users, cert); err != nil {
		t.Fatalf("Reload replace selector member: %v", err)
	}
	mustFlowAfterRestart(t, client, src, "via upstream 2")
	if hits2.Load() == 0 || hits1.Load() != h1 {
		t.Fatalf("selector did not follow the replaced member: up1 %d→%d, up2 %d", h1, hits1.Load(), hits2.Load())
	}

	h2 := hits2.Load()
	if err := s.Reload(p.node(port), users, cert); err != nil {
		t.Fatalf("Reload remove outbound: %v", err)
	}
	mustFlowAfterRestart(t, client, src, "direct again")
	if hits2.Load() != h2 {
		t.Fatal("traffic still went to the removed outbound")
	}
	if _, ok := s.box.Outbound().Outbound("via"); ok {
		t.Fatal("removed outbound still registered")
	}
}

// A config that only fails at start (a selector member that does not exist)
// is reported, and the previous config is brought back up.
func TestE2EReloadFailingConfigRollsBack(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[1]
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	cert := selfSignedCert(t)
	s := New(testKernelConfig(t))
	port := freePort(t, "tcp")
	good := p.node(port)
	if err := s.Start(good, []model.UserSpec{userA}, cert); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	client := startClient(t, p.outbound(port, e2eUUIDA))
	mustFlow(t, client, src, "before")

	bad := p.node(port)
	bad.CustomOutbounds = []model.OutboundConfig{selectorOutbound("via", "no-such-outbound")}
	if err := s.Reload(bad, []model.UserSpec{userA}, cert); err == nil {
		t.Fatal("Reload accepted a selector with a missing member")
	}
	if s.box == nil {
		t.Fatal("kernel left down after a failing config")
	}
	if s.nodeConfig != good {
		t.Fatal("kernel state does not point at the restored config")
	}
	mustFlowAfterRestart(t, client, src, "after rollback")

	// The service falls back to Start with the same bad config; that must
	// roll back too.
	if err := s.Start(bad, []model.UserSpec{userA}, cert); err == nil {
		t.Fatal("Start accepted the failing config")
	}
	if s.box == nil {
		t.Fatal("kernel left down after Start with a failing config")
	}
	mustFlowAfterRestart(t, client, src, "after second rollback")
}

// A restart closes connections a client keeps across requests (an anytls
// session, the HTTP/2 connection under gRPC or the h2 transport; naive's
// HTTP/2 CONNECT is the same case, checked by hand). Left open, they stay attached
// to the stopped box: every new request on them fails at once and they never
// idle out, so the client is stuck until it restarts.
func TestE2ERestartClosesIdleSessions(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	grpc := e2eProto{
		name: "vless-grpc",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "vless", ListenIP: "127.0.0.1", ServerPort: p, Network: "grpc",
				NetworkSettings: map[string]any{"serviceName": "g"}, TLS: 1, ServerName: "test.local", CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"vless","tag":"out","server":"127.0.0.1","server_port":%d,"uuid":%q,"tls":{"enabled":true,"server_name":"test.local","insecure":true},"transport":{"type":"grpc","service_name":"g"}}`, p, u)
		},
	}
	h2 := e2eProto{
		name: "vless-h2",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "vless", ListenIP: "127.0.0.1", ServerPort: p, Network: "h2",
				NetworkSettings: map[string]any{"path": "/h"}, TLS: 1, ServerName: "test.local", CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"vless","tag":"out","server":"127.0.0.1","server_port":%d,"uuid":%q,"tls":{"enabled":true,"server_name":"test.local","insecure":true},"transport":{"type":"http","path":"/h"}}`, p, u)
		},
	}
	for _, p := range []e2eProto{e2eProtos[0], grpc, h2} {
		t.Run(p.name, func(t *testing.T) {
			src := startSource(t)
			userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
			s, port := startServer(t, p, []model.UserSpec{userA})
			client := startClient(t, p.outbound(port, e2eUUIDA))
			st := mustFlow(t, client, src, "before")
			st.conn.Close() // the client keeps the connection for the next request
			time.Sleep(300 * time.Millisecond)

			if err := s.Start(p.node(port), []model.UserSpec{userA}, selfSignedCert(t)); err != nil {
				t.Fatal(err)
			}
			mustFlowAfterRestart(t, client, src, "after restart")
		})
	}
}

// Start on a running kernel restarts it on the same port.
func TestE2EStartWhileRunningRestartsOnSamePort(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[1]
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	s, port := startServer(t, p, []model.UserSpec{userA})
	client := startClient(t, p.outbound(port, e2eUUIDA))
	mustFlow(t, client, src, "before")
	boxBefore := s.box
	if err := s.Start(p.node(port), []model.UserSpec{userA}, selfSignedCert(t)); err != nil {
		t.Fatalf("Start while running: %v", err)
	}
	if s.box == boxBefore {
		t.Fatal("kernel not replaced")
	}
	mustFlowAfterRestart(t, client, src, "after")
}

// Stop closes the box; the port is free afterwards and a new kernel can
// start on it straight away.
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
	mustFlowAfterRestart(t, client, src, "after restart")
}
