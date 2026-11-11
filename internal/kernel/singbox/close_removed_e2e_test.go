package singbox

// End-to-end: a real server kernel (SingBox.Start, as the node runs it) and a
// real sing-box client per user, streaming data through the proxy. Verifies
// that removing a user from the node closes that user's open connections and
// blocks new streams on the existing session, while other users keep flowing.
//
// Regression for production 2026-10-28: an over-quota anytls user, already
// removed from the user list, kept downloading on an open session for 34
// minutes (~92 GB).

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	singJSON "github.com/sagernet/sing/common/json"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

const (
	e2eUUIDA = "11111111-1111-4111-8111-111111111111"
	e2eUUIDB = "22222222-2222-4222-8222-222222222222"
	e2eUUIDC = "33333333-3333-4333-8333-333333333333" // A's rotated UUID
)

func freePort(t *testing.T, network string) int {
	t.Helper()
	if network == "udp" {
		c, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		return c.LocalAddr().(*net.UDPAddr).Port
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func selfSignedCert(t *testing.T) kernel.TLSCert {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test.local"},
		DNSNames:     []string{"test.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return kernel.TLSCert{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}),
	}
}

// startSource serves an endless byte stream to every TCP client.
func startSource(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	chunk := make([]byte, 32*1024)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					if _, err := c.Write(chunk); err != nil {
						return
					}
				}
			}()
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}

// startClient runs a sing-box client: mixed inbound on localhost → protocol outbound.
func startClient(t *testing.T, outbound string) int {
	t.Helper()
	port := freePort(t, "tcp")
	cfg := fmt.Sprintf(`{"log":{"level":"error"},
		"inbounds":[{"type":"mixed","tag":"in","listen":"127.0.0.1","listen_port":%d}],
		"outbounds":[%s],
		"route":{"final":"out"}}`, port, outbound)
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	opts, err := singJSON.UnmarshalExtendedContext[option.Options](ctx, []byte(cfg))
	if err != nil {
		cancel()
		t.Fatalf("client options: %v", err)
	}
	b, err := box.New(box.Options{Context: ctx, Options: opts})
	if err != nil {
		cancel()
		t.Fatalf("client box: %v", err)
	}
	if err := b.Start(); err != nil {
		b.Close()
		cancel()
		t.Fatalf("client start: %v", err)
	}
	t.Cleanup(func() { b.Close(); cancel() })
	return port
}

// stream is one download through the proxy, counting received bytes.
type stream struct {
	bytes atomic.Int64
	done  chan error // receives the read error when the stream ends
	conn  net.Conn
	err   atomic.Value
}

func openStream(t *testing.T, clientPort, srcPort int) (*stream, error) {
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", clientPort), 3*time.Second)
	if err != nil {
		return nil, err
	}
	target := fmt.Sprintf("127.0.0.1:%d", srcPort)
	fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(c)
	status, err := br.ReadString('\n')
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("CONNECT response: %w", err)
	}
	if !strings.Contains(status, " 200") {
		c.Close()
		return nil, fmt.Errorf("CONNECT status %q", strings.TrimSpace(status))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			c.Close()
			return nil, err
		}
		if line == "\r\n" {
			break
		}
	}
	c.SetReadDeadline(time.Time{})
	s := &stream{done: make(chan error, 1), conn: c}
	t.Cleanup(func() { c.Close() })
	go func() {
		buf := make([]byte, 64*1024)
		if n := br.Buffered(); n > 0 {
			k, _ := br.Read(buf[:n])
			s.bytes.Add(int64(k))
		}
		for {
			n, err := c.Read(buf)
			s.bytes.Add(int64(n))
			if err != nil {
				s.err.Store(fmt.Sprint(err))
				s.done <- err
				return
			}
		}
	}()
	return s, nil
}

// endsWithin reports whether the stream ends (read error) within d.
func (s *stream) endsWithin(d time.Duration) bool {
	select {
	case <-s.done:
		return true
	case <-time.After(d):
		return false
	}
}

// flowing reports whether the stream keeps receiving data for d.
func (s *stream) flowing(d time.Duration) bool {
	before := s.bytes.Load()
	select {
	case <-s.done:
		return false
	case <-time.After(d):
	}
	return s.bytes.Load()-before > 1<<20
}

// newStreamIsDead: a fresh stream for a removed user must not carry data.
func newStreamIsDead(t *testing.T, clientPort, srcPort int) bool {
	s, err := openStream(t, clientPort, srcPort)
	if err != nil {
		return true
	}
	if !s.endsWithin(3 * time.Second) {
		return false
	}
	return s.bytes.Load() < 1<<20
}

type e2eProto struct {
	name     string
	node     func(port int) *model.NodeSpec
	outbound func(port int, uuid string) string
	udp      bool
}

// The generated config blocks private/loopback destinations (SSRF guard);
// custom routes are evaluated before it, so let the test's 127.0.0.1 data
// source through.
var e2eAllowLoopback = []map[string]any{{"ip_cidr": []string{"127.0.0.1/32"}, "outbound": "direct"}}

var e2eProtos = []e2eProto{
	{
		name: "anytls",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "anytls", ListenIP: "127.0.0.1", ServerPort: p, ServerName: "test.local", CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"anytls","tag":"out","server":"127.0.0.1","server_port":%d,"password":%q,"tls":{"enabled":true,"server_name":"test.local","insecure":true}}`, p, u)
		},
	},
	{
		name: "vless",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "vless", ListenIP: "127.0.0.1", ServerPort: p, Network: "tcp", CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"vless","tag":"out","server":"127.0.0.1","server_port":%d,"uuid":%q}`, p, u)
		},
	},
	{
		name: "hysteria2",
		node: func(p int) *model.NodeSpec {
			return &model.NodeSpec{Protocol: "hysteria", Version: 2, ListenIP: "127.0.0.1", ServerPort: p, ServerName: "test.local", CustomRoutes: e2eAllowLoopback}
		},
		outbound: func(p int, u string) string {
			return fmt.Sprintf(`{"type":"hysteria2","tag":"out","server":"127.0.0.1","server_port":%d,"password":%q,"tls":{"enabled":true,"server_name":"test.local","insecure":true,"alpn":["h3"]}}`, p, u)
		},
		udp: true,
	},
}

func startServer(t *testing.T, p e2eProto, users []model.UserSpec) (*SingBox, int) {
	t.Helper()
	network := "tcp"
	if p.udp {
		network = "udp"
	}
	port := freePort(t, network)
	s := New(config.KernelConfig{Type: "singbox", LogLevel: "error", ConfigDir: t.TempDir()})
	if err := s.Start(p.node(port), users, selfSignedCert(t)); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() { s.Stop() })
	return s, port
}

func TestE2ERemovedUserConnectionsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	for _, p := range e2eProtos {
		p := p
		t.Run(p.name, func(t *testing.T) {
			src := startSource(t)
			userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
			userB := model.UserSpec{ID: 2, UUID: e2eUUIDB}
			s, port := startServer(t, p, []model.UserSpec{userA, userB})
			clientA := startClient(t, p.outbound(port, e2eUUIDA))
			clientB := startClient(t, p.outbound(port, e2eUUIDB))

			a, err := openStream(t, clientA, src)
			if err != nil {
				t.Fatalf("A stream: %v", err)
			}
			b, err := openStream(t, clientB, src)
			if err != nil {
				t.Fatalf("B stream: %v", err)
			}
			if !a.flowing(time.Second) || !b.flowing(time.Second) {
				t.Fatalf("streams not flowing before removal: A bytes=%d err=%v, B bytes=%d err=%v", a.bytes.Load(), a.err.Load(), b.bytes.Load(), b.err.Load())
			}

			// Remove A, as the service does when the panel drops an over-quota user.
			if _, removed, err := s.UpdateUsers([]model.UserSpec{userB}); err != nil || removed != 1 {
				t.Fatalf("UpdateUsers: removed=%d err=%v", removed, err)
			}

			if !a.endsWithin(3 * time.Second) {
				t.Fatalf("removed user's open stream still alive (bytes=%d)", a.bytes.Load())
			}
			if !b.flowing(2 * time.Second) {
				t.Fatal("remaining user's stream was affected")
			}
			if !newStreamIsDead(t, clientA, src) {
				t.Fatal("removed user could open a new working stream")
			}

			traffic, _, _ := s.connTracker.GetUserTraffic()
			if traffic[1][1] <= 0 {
				t.Fatalf("bytes before removal not counted for A: %v", traffic[1])
			}
		})
	}
}

func TestE2ERotatedUUIDClosesOldConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[0] // anytls: the protocol from the production incident
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	s, port := startServer(t, p, []model.UserSpec{userA})
	clientOld := startClient(t, p.outbound(port, e2eUUIDA))

	a, err := openStream(t, clientOld, src)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if !a.flowing(time.Second) {
		t.Fatal("not flowing")
	}
	rotated := model.UserSpec{ID: 1, UUID: e2eUUIDC}
	if _, _, err := s.UpdateUsers([]model.UserSpec{rotated}); err != nil {
		t.Fatalf("UpdateUsers: %v", err)
	}
	if !a.endsWithin(3 * time.Second) {
		t.Fatal("connection on the old UUID still alive after rotation")
	}
	clientNew := startClient(t, p.outbound(port, e2eUUIDC))
	n, err := openStream(t, clientNew, src)
	if err != nil {
		t.Fatalf("new UUID stream: %v", err)
	}
	if !n.flowing(time.Second) {
		t.Fatal("new UUID cannot use the node")
	}
}

func TestE2ERemoveUsersDeltaClosesConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[0]
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	userB := model.UserSpec{ID: 2, UUID: e2eUUIDB}
	s, port := startServer(t, p, []model.UserSpec{userA, userB})
	clientA := startClient(t, p.outbound(port, e2eUUIDA))
	a, err := openStream(t, clientA, src)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if !a.flowing(time.Second) {
		t.Fatal("not flowing")
	}
	if n, err := s.RemoveUsers([]model.UserSpec{userA}); err != nil || n != 1 {
		t.Fatalf("RemoveUsers: n=%d err=%v", n, err)
	}
	if !a.endsWithin(3 * time.Second) {
		t.Fatal("delta removal left the stream alive")
	}
}

// Guard against draining legitimate users: adding a user must not disturb an
// existing user's open stream (the tracker map is swapped during the update).
func TestE2EAddingUserKeepsExistingStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	p := e2eProtos[0]
	src := startSource(t)
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	userB := model.UserSpec{ID: 2, UUID: e2eUUIDB}
	s, port := startServer(t, p, []model.UserSpec{userA})
	clientA := startClient(t, p.outbound(port, e2eUUIDA))
	a, err := openStream(t, clientA, src)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if _, _, err := s.UpdateUsers([]model.UserSpec{userA, userB}); err != nil {
		t.Fatalf("UpdateUsers: %v", err)
	}
	if !a.flowing(2 * time.Second) {
		t.Fatal("existing user's stream disturbed by adding another user")
	}
	clientB := startClient(t, p.outbound(port, e2eUUIDB))
	b, err := openStream(t, clientB, src)
	if err != nil {
		t.Fatalf("added user stream: %v", err)
	}
	if !b.flowing(time.Second) {
		t.Fatal("added user cannot use the node")
	}
}

var _ io.Closer = (*trackedConn)(nil)
