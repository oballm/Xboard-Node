package xray

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	xrayCore "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
	xproxy "golang.org/x/net/proxy"
)

// A CDN keeps a pool of connections to the origin and sends every user's
// requests over it. After a full restart those pooled connections must not stay
// with the old instance: new sessions there would see the old routes and users,
// and their traffic would never be counted.
func TestRestartBehindKeepAlivePoolUsesNewInstance(t *testing.T) {
	const uuid = "aaaaaaaa-1111-2222-3333-444444444444"
	const size = 256 << 10

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" { // a download still running during the restarts
			for i := 0; i < 600 && r.Context().Err() == nil; i++ {
				w.Write(make([]byte, 1024))
				w.(http.Flusher).Flush()
				time.Sleep(100 * time.Millisecond)
			}
			return
		}
		w.Write(make([]byte, size))
	}))
	defer backend.Close()
	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port

	serverPort := freePort(t)
	// xray refuses private targets from a vless inbound by default; the test
	// backend is on loopback, so the panel's custom_config routes it through a
	// freedom outbound that allows it. outboundTag selects what happens to it.
	nc := func(outboundTag, extra string) *model.NodeSpec {
		rules := []any{map[string]any{"type": "field", "port": fmt.Sprint(backendPort), "outboundTag": outboundTag}}
		if extra != "" {
			rules = append(rules, map[string]any{"type": "field", "domain": []any{extra}, "outboundTag": "block"})
		}
		return &model.NodeSpec{
			Protocol: "vless", Network: "xhttp", ListenIP: "127.0.0.1", ServerPort: serverPort, KernelType: "xray",
			NetworkSettings: map[string]any{"path": "/x"},
			CustomConfig: map[string]any{
				"outbounds": []any{map[string]any{"tag": "lo", "protocol": "freedom",
					"settings": map[string]any{"finalRules": []any{map[string]any{"action": "allow", "ip": []any{"127.0.0.0/8", "::1/128"}}}}}},
				"routing": map[string]any{"rules": rules},
			},
		}
	}
	users := []model.UserSpec{{ID: 1, UUID: uuid}}

	x := New(config.KernelConfig{LogLevel: "none"})
	if err := x.Start(nc("lo", ""), users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	defer x.Stop()

	// The "CDN": one reverse proxy with a keep-alive pool to the origin.
	target, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", serverPort))
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.FlushInterval = -1
	rp.Transport = &http.Transport{MaxIdleConnsPerHost: 16, IdleConnTimeout: time.Minute}
	cdn := httptest.NewServer(rp)
	defer cdn.Close()
	cdnPort := cdn.Listener.Addr().(*net.TCPAddr).Port

	socksPort := freePort(t)
	clientJSON := fmt.Sprintf(`{
	  "log": {"loglevel": "none"},
	  "inbounds": [{"listen": "127.0.0.1", "port": %d, "protocol": "socks", "settings": {"udp": false}}],
	  "outbounds": [{"protocol": "vless",
	    "settings": {"vnext": [{"address": "127.0.0.1", "port": %d, "users": [{"id": %q, "encryption": "none"}]}]},
	    "streamSettings": {"network": "xhttp", "xhttpSettings": {"path": "/x", "mode": "packet-up"}}}]
	}`, socksPort, cdnPort, uuid)
	pb, err := serial.LoadJSONConfig(strings.NewReader(clientJSON))
	if err != nil {
		t.Fatal(err)
	}
	client, err := xrayCore.New(pb)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	dialer, err := xproxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort), nil, xproxy.Direct)
	if err != nil {
		t.Fatal(err)
	}
	// fetch opens a new proxied session every time; returns the bytes received.
	fetch := func() int {
		hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Dial: dialer.Dial, DisableKeepAlives: true}}
		resp, err := hc.Get(fmt.Sprintf("http://localhost:%d/", backendPort))
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		n, _ := io.Copy(io.Discard, resp.Body)
		return int(n)
	}
	downlink := func() int64 {
		traffic, _, _, err := x.GetUserTraffic(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return traffic[1][1]
	}

	for i := 0; i < 4; i++ {
		if n := fetch(); n != size {
			t.Fatalf("before restart: fetch %d got %d bytes", i, n)
		}
	}

	slowStarted := make(chan struct{})
	slowDone := make(chan struct{})
	go func() {
		hc := &http.Client{Transport: &http.Transport{Dial: dialer.Dial, DisableKeepAlives: true}}
		resp, err := hc.Get(fmt.Sprintf("http://localhost:%d/slow", backendPort))
		if err != nil {
			close(slowStarted)
			return
		}
		defer resp.Body.Close()
		buf := make([]byte, 1024)
		resp.Body.Read(buf)
		close(slowStarted)
		io.Copy(io.Discard, resp.Body)
		close(slowDone)
	}()
	<-slowStarted

	// Restart 1: a change that does not affect this traffic. Every byte from
	// now on must be counted by the new instance.
	if err := x.Reload(nc("lo", "unrelated.example"), users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	// The download was on the replaced instance, where nothing counts it any
	// more. It must be cut, so the client reconnects to the new instance.
	select {
	case <-slowDone:
	case <-time.After(5 * time.Second):
		t.Error("a download on the replaced instance was still running 5s after the restart")
	}
	base := downlink()
	const rounds = 8
	for i := 0; i < rounds; i++ {
		if n := fetch(); n != size {
			t.Fatalf("after restart 1: fetch %d got %d bytes", i, n)
		}
	}
	if got := downlink() - base; got < rounds*size {
		t.Errorf("after restart 1: counted %d bytes downlink, want at least %d", got, rounds*size)
	}

	// Restart 2: block the target. No new session may get through.
	if err := x.Reload(nc("block", ""), users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	passed := 0
	for i := 0; i < rounds; i++ {
		if fetch() > 0 {
			passed++
		}
	}
	if passed > 0 {
		t.Errorf("after restart 2: %d of %d sessions reached the blocked target", passed, rounds)
	}
}
