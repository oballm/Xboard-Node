package service

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/buildinfo"
	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestAddVersionMetrics(t *testing.T) {
	old := buildinfo.Version
	buildinfo.Version = "v9.9.9-test"
	defer func() { buildinfo.Version = old }()

	m := map[string]interface{}{}
	(&Service{}).addVersionMetrics(m)
	if m["node_version"] != "v9.9.9-test" {
		t.Fatalf("node_version = %v", m["node_version"])
	}
}

func TestKernelTypeMetricFollowsSwitch(t *testing.T) {
	m := map[string]interface{}{}
	(&Service{}).addVersionMetrics(m)
	if _, ok := m["kernel_type"]; ok {
		t.Fatalf("kernel_type reported before any kernel is built: %v", m["kernel_type"])
	}

	s := newService(&config.Config{Kernel: config.KernelConfig{Type: model.KernelAuto}}, nil)
	m = map[string]interface{}{}
	s.addVersionMetrics(m)
	if m["kernel_type"] != model.KernelSingBox {
		t.Fatalf("auto placeholder kernel_type = %v, want %s", m["kernel_type"], model.KernelSingBox)
	}

	s.ensureKernelType(model.KernelXray)
	m = map[string]interface{}{}
	s.addVersionMetrics(m)
	if m["kernel_type"] != model.KernelXray {
		t.Fatalf("after switch kernel_type = %v, want %s", m["kernel_type"], model.KernelXray)
	}
}
