package kernel

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

func TestWireCustomConfigDialectGuard(t *testing.T) {
	cc := map[string]any{"dns": map[string]any{"servers": []any{}}}
	cases := []struct {
		panelKernel, running string
		want                 bool
	}{
		{"", model.KernelSingBox, true}, // panel-proxy: empty kernel_type = sing-box
		{"singbox", model.KernelSingBox, true},
		{"sing-box", model.KernelSingBox, true},
		{"xray", model.KernelXray, true},
		{"XRAY", model.KernelXray, true},
		{"", model.KernelXray, false},        // sing-box dialect on xray
		{"xray", model.KernelSingBox, false}, // xray dialect on sing-box
		{"singbox", model.KernelXray, false},
	}
	for _, c := range cases {
		got := WireCustomConfig(&model.NodeSpec{KernelType: c.panelKernel, CustomConfig: cc}, c.running)
		if (got != nil) != c.want {
			t.Errorf("panel %q running %q: used=%v, want %v", c.panelKernel, c.running, got != nil, c.want)
		}
	}
	if WireCustomConfig(&model.NodeSpec{KernelType: "xray"}, model.KernelXray) != nil || WireCustomConfig(nil, model.KernelXray) != nil {
		t.Error("empty custom_config should give nil")
	}
}
