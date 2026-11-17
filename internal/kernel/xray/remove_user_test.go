package xray

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	xrayCore "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
	xproxy "golang.org/x/net/proxy"
)

// An expired or over-quota user is removed from the user list. Their running
// download must be cut at that moment (xray keeps it going otherwise, and its
// traffic is no longer counted); another user's download must not be.
func TestRemovedUserConnectionsAreClosed(t *testing.T) {
	const (
		uuidA = "aaaaaaaa-1111-2222-3333-444444444444"
		uuidB = "bbbbbbbb-1111-2222-3333-444444444444"
	)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 600 && r.Context().Err() == nil; i++ { // throttled at the source
			w.Write(make([]byte, 1024))
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
	}))
	defer backend.Close()
	backendPort := backend.Listener.Addr().(*net.TCPAddr).Port

	serverPort := freePort(t)
	nc := &model.NodeSpec{
		Protocol: "vless", Network: "xhttp", ListenIP: "127.0.0.1", ServerPort: serverPort, KernelType: "xray",
		NetworkSettings: map[string]any{"path": "/x"},
		// xray refuses private targets from a vless inbound by default.
		CustomConfig: map[string]any{
			"outbounds": []any{map[string]any{"tag": "lo", "protocol": "freedom",
				"settings": map[string]any{"finalRules": []any{map[string]any{"action": "allow", "ip": []any{"127.0.0.0/8", "::1/128"}}}}}},
			"routing": map[string]any{"rules": []any{map[string]any{"type": "field", "port": fmt.Sprint(backendPort), "outboundTag": "lo"}}},
		},
	}
	userA := model.UserSpec{ID: 1, UUID: uuidA}
	userB := model.UserSpec{ID: 2, UUID: uuidB}

	x := New(config.KernelConfig{LogLevel: "none"})
	if err := x.Start(nc, []model.UserSpec{userA, userB}, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	defer x.Stop()

	// One client, one SOCKS port per user.
	portA, portB := freePort(t), freePort(t)
	outbound := func(tag, uuid string) string {
		return fmt.Sprintf(`{"tag": %q, "protocol": "vless",
		  "settings": {"vnext": [{"address": "127.0.0.1", "port": %d, "users": [{"id": %q, "encryption": "none"}]}]},
		  "streamSettings": {"network": "xhttp", "xhttpSettings": {"path": "/x"}}}`, tag, serverPort, uuid)
	}
	clientJSON := fmt.Sprintf(`{
	  "log": {"loglevel": "none"},
	  "inbounds": [
	    {"tag": "in-a", "listen": "127.0.0.1", "port": %d, "protocol": "socks", "settings": {"udp": false}},
	    {"tag": "in-b", "listen": "127.0.0.1", "port": %d, "protocol": "socks", "settings": {"udp": false}}],
	  "outbounds": [%s, %s],
	  "routing": {"rules": [
	    {"type": "field", "inboundTag": ["in-a"], "outboundTag": "out-a"},
	    {"type": "field", "inboundTag": ["in-b"], "outboundTag": "out-b"}]}
	}`, portA, portB, outbound("out-a", uuidA), outbound("out-b", uuidB))
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

	// download starts a running download through the user's port and returns
	// a byte counter and a channel closed when the download ends.
	download := func(port int) (*atomic.Int64, chan struct{}) {
		dialer, err := xproxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", port), nil, xproxy.Direct)
		if err != nil {
			t.Fatal(err)
		}
		var n atomic.Int64
		done := make(chan struct{})
		started := make(chan struct{})
		go func() {
			defer close(done)
			hc := &http.Client{Transport: &http.Transport{Dial: dialer.Dial, DisableKeepAlives: true}}
			resp, err := hc.Get(fmt.Sprintf("http://127.0.0.1:%d/", backendPort))
			if err != nil {
				close(started)
				return
			}
			defer resp.Body.Close()
			buf := make([]byte, 4096)
			first := true
			for {
				k, err := resp.Body.Read(buf)
				n.Add(int64(k))
				if first && k > 0 {
					close(started)
					first = false
				}
				if err != nil {
					if first {
						close(started)
					}
					return
				}
			}
		}()
		<-started
		return &n, done
	}

	gotA, doneA := download(portA)
	gotB, doneB := download(portB)
	if gotA.Load() == 0 || gotB.Load() == 0 {
		t.Fatal("downloads did not start")
	}

	// User A expires: the panel's list no longer has them.
	if _, _, err := x.UpdateUsers([]model.UserSpec{userB}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-doneA:
	case <-time.After(3 * time.Second):
		t.Fatalf("removed user's download still running 3s after removal (%d bytes)", gotA.Load())
	}

	before := gotB.Load()
	time.Sleep(time.Second)
	select {
	case <-doneB:
		t.Fatal("the remaining user's download was cut")
	default:
	}
	if gotB.Load() <= before {
		t.Fatal("the remaining user's download stopped moving")
	}

	// And the removed user cannot start a new one.
	dialer, _ := xproxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", portA), nil, xproxy.Direct)
	hc := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Dial: dialer.Dial, DisableKeepAlives: true}}
	if resp, err := hc.Get(fmt.Sprintf("http://127.0.0.1:%d/", backendPort)); err == nil {
		k, _ := io.CopyN(io.Discard, resp.Body, 1)
		resp.Body.Close()
		if k > 0 {
			t.Fatal("removed user started a new download")
		}
	}
}
