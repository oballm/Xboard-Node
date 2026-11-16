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

	if _, ok := m["kernel_source"]; ok {
		t.Fatalf("kernel_source reported before the first config: %v", m["kernel_source"])
	}

	// Panel says xray on a TCP node: auto must follow it, not the transport.
	want, err := s.resolveKernelType(&model.NodeSpec{Protocol: "vless", Network: "tcp", KernelType: "xray"})
	if err != nil {
		t.Fatal(err)
	}
	s.ensureKernelType(want)
	m = map[string]interface{}{}
	s.addVersionMetrics(m)
	if m["kernel_type"] != model.KernelXray || m["kernel_source"] != model.KernelSourcePanel {
		t.Fatalf("after switch kernel_type/source = %v/%v, want xray/panel", m["kernel_type"], m["kernel_source"])
	}

	// Panel singbox on xhttp: rejected, the running kernel is left alone.
	if _, err := s.resolveKernelType(&model.NodeSpec{Protocol: "vless", Network: "xhttp", KernelType: "singbox"}); err == nil {
		t.Fatal("panel singbox on xhttp: want error")
	}
	if s.kernelType != model.KernelXray {
		t.Fatalf("kernel changed after a rejected config: %s", s.kernelType)
	}
}
