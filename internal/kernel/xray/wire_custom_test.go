package xray

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	xrayCore "github.com/xtls/xray-core/core"
	xdns "github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/infra/conf/serial"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

var xrayDNS = map[string]any{"servers": []any{"https://1.1.1.1/dns-query"}}

// The reserved path works: wire custom_config wins over the local file.
func TestMergeCustomXrayWirePrecedence(t *testing.T) {
	local := filepath.Join(t.TempDir(), "custom.json")
	if err := os.WriteFile(local, []byte(`{"dns":{"servers":["8.8.8.8"]}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	kcfg := config.KernelConfig{CustomConfig: local}

	cfg := M{"outbounds": []M{}}
	mergeCustomXray(cfg, kcfg, map[string]any{"dns": xrayDNS})
	if dns, _ := cfg["dns"].(map[string]any); dns == nil || dns["servers"].([]any)[0] != "https://1.1.1.1/dns-query" {
		t.Fatalf("wire dns not merged: %#v", cfg["dns"])
	}

	cfg = M{"outbounds": []M{}}
	mergeCustomXray(cfg, kcfg, nil)
	if dns, _ := cfg["dns"].(map[string]any); dns == nil || dns["servers"].([]any)[0] != "8.8.8.8" {
		t.Fatalf("local file not used without wire config: %#v", cfg["dns"])
	}
}

// buildConfig merges the panel's custom_config into an xray node when it is
// xray dialect, and skips a sing-box one (panel-proxy: empty kernel_type).
func TestXrayBuildConfigUsesWireCustomConfig(t *testing.T) {
	build := func(panelKernel string) M {
		nc := &model.NodeSpec{Protocol: "vless", Network: "tcp", ServerPort: 443, KernelType: panelKernel,
			CustomConfig: map[string]any{"dns": xrayDNS}}
		return buildConfig(config.KernelConfig{}, nc, nil, kernel.TLSCert{})
	}
	if _, ok := build("xray")["dns"]; !ok {
		t.Fatal("xray-dialect panel custom_config not merged")
	}
	if dns, ok := build("")["dns"]; ok {
		t.Fatalf("sing-box-dialect panel custom_config merged into xray: %#v", dns)
	}
}

// End to end: the panel's DNS block reaches a running xray instance. A hosts
// entry from the panel's custom_config resolves through xray's own DNS.
func TestXrayPanelDNSAppliedToRunningKernel(t *testing.T) {
	nc := &model.NodeSpec{Protocol: "vless", Network: "tcp", ListenIP: "127.0.0.1", ServerPort: freePort(t), KernelType: "xray",
		CustomConfig: map[string]any{"dns": map[string]any{
			"hosts":   map[string]any{"xbn-panel-dns.test": "10.9.8.7"},
			"servers": []any{"localhost"},
		}}}
	cfg := buildConfig(config.KernelConfig{LogLevel: "warn"}, nc, []model.UserSpec{{ID: 1, UUID: "aaaaaaaa-1111-2222-3333-444444444444"}}, kernel.TLSCert{})
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := serial.LoadJSONConfig(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("xray rejected the config: %v", err)
	}
	inst, err := xrayCore.New(pb)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	defer inst.Close()

	client, ok := inst.GetFeature(xdns.ClientType()).(xdns.Client)
	if !ok {
		t.Fatal("no dns client")
	}
	ips, _, err := client.LookupIP("xbn-panel-dns.test", xdns.IPOption{IPv4Enable: true})
	if err != nil {
		t.Fatalf("lookup through xray dns: %v", err)
	}
	if len(ips) == 0 || ips[0].String() != "10.9.8.7" {
		t.Fatalf("got %v, want the panel hosts entry 10.9.8.7", ips)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
