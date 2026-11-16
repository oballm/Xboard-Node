package singbox

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

// sing-box still merges the panel's custom_config (panel-proxy: empty
// kernel_type = sing-box), but not one written for xray: a node whose local
// config pins sing-box while the panel says xray would otherwise get xray's
// dialect merged in and fail to start.
func TestSingboxWireCustomConfigDialectGuard(t *testing.T) {
	singboxDNS := map[string]any{"servers": []any{map[string]any{"type": "local", "tag": "local"}}}
	build := func(panelKernel string) M {
		nc := &model.NodeSpec{Protocol: "vless", Network: "tcp", ServerPort: 443, KernelType: panelKernel,
			CustomConfig: map[string]any{"dns": singboxDNS}}
		return buildConfig(config.KernelConfig{LogLevel: "warn"}, nc, testUsers, kernel.TLSCert{})
	}
	for _, k := range []string{"", "singbox"} {
		if _, ok := build(k)["dns"]; !ok {
			t.Errorf("panel kernel_type %q: custom_config not merged", k)
		}
	}
	if dns, ok := build("xray")["dns"]; ok {
		t.Errorf("xray-dialect custom_config merged into sing-box: %#v", dns)
	}
}
