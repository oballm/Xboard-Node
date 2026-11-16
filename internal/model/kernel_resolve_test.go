package model

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
)

func TestResolveEffectiveKernel(t *testing.T) {
	cases := []struct {
		mode    string
		network string
		want    string
	}{
		// auto: transport decides
		{"auto", "tcp", KernelSingBox},
		{"auto", "ws", KernelSingBox},
		{"auto", "grpc", KernelSingBox},
		{"auto", "xhttp", KernelXray},
		{"auto", "splithttp", KernelXray},
		{"auto", "XHTTP", KernelXray}, // case-insensitive
		// explicit values pass through unchanged (no transport override)
		{"singbox", "tcp", KernelSingBox},
		{"singbox", "xhttp", KernelSingBox}, // incompatible, but respected here; rejected by ValidateTransportKernel
		{"xray", "tcp", KernelXray},
		{"xray", "xhttp", KernelXray},
	}
	for _, c := range cases {
		if got := ResolveEffectiveKernel(c.network, c.mode); got != c.want {
			t.Errorf("ResolveEffectiveKernel(%q, %q) = %q, want %q", c.network, c.mode, got, c.want)
		}
	}
}

func TestNormalizeKernelType(t *testing.T) {
	ok := map[string]string{
		"auto":     KernelAuto,
		"singbox":  KernelSingBox,
		"sing-box": KernelSingBox,
		"xray":     KernelXray,
		"XRAY":     KernelXray,
	}
	for in, want := range ok {
		got, err := normalizeKernelType(in)
		if err != nil || got != want {
			t.Errorf("normalizeKernelType(%q) = (%q, %v), want (%q, nil)", in, got, err, want)
		}
	}
	if _, err := normalizeKernelType("bogus"); err == nil {
		t.Errorf("normalizeKernelType(\"bogus\") expected error, got nil")
	}
}

func TestValidateTransportKernel(t *testing.T) {
	// sing-box cannot serve xhttp/splithttp
	for _, net := range []string{"xhttp", "splithttp"} {
		if err := ValidateTransportKernel(net, KernelSingBox); err == nil {
			t.Errorf("ValidateTransportKernel(%q, singbox) expected error, got nil", net)
		}
		if err := ValidateTransportKernel(net, KernelXray); err != nil {
			t.Errorf("ValidateTransportKernel(%q, xray) unexpected error: %v", net, err)
		}
	}
	// sing-box serves everything else
	if err := ValidateTransportKernel("tcp", KernelSingBox); err != nil {
		t.Errorf("ValidateTransportKernel(tcp, singbox) unexpected error: %v", err)
	}
}

func TestSelectKernel(t *testing.T) {
	cases := []struct {
		name, configured, panel, network string
		want, source                     string
		wantErr                          bool
	}{
		// Local singbox/xray always wins, whatever the panel says.
		{"local singbox ignores panel", "singbox", "xray", "tcp", KernelSingBox, KernelSourceLocal, false},
		{"local xray ignores panel", "xray", "singbox", "ws", KernelXray, KernelSourceLocal, false},
		{"local sing-box spelling", "sing-box", "", "tcp", KernelSingBox, KernelSourceLocal, false},
		// auto follows the panel — including the node 111 case (REALITY/TCP, panel xray).
		{"auto follows panel xray on tcp", "auto", "xray", "tcp", KernelXray, KernelSourcePanel, false},
		{"auto follows panel singbox on ws", "auto", "singbox", "ws", KernelSingBox, KernelSourcePanel, false},
		{"auto follows panel xray on xhttp", "auto", "xray", "xhttp", KernelXray, KernelSourcePanel, false},
		// No fallback: the panel's choice is returned even if it can't serve the
		// transport; ValidateTransportKernel rejects it afterwards.
		{"auto panel singbox on xhttp not overridden", "auto", "singbox", "xhttp", KernelSingBox, KernelSourcePanel, false},
		// No kernel_type from the panel (Xboard / old panel) → transport rule.
		{"auto no panel tcp", "auto", "", "tcp", KernelSingBox, KernelSourceTransport, false},
		{"auto no panel xhttp", "auto", "", "xhttp", KernelXray, KernelSourceTransport, false},
		{"auto panel says auto", "auto", "auto", "splithttp", KernelXray, KernelSourceTransport, false},
		{"empty configured is auto", "", "xray", "tcp", KernelXray, KernelSourcePanel, false},
		// Bad values are errors, not silently ignored.
		{"bad panel value", "auto", "v2ray", "tcp", "", "", true},
		{"bad local value", "clash", "", "tcp", "", "", true},
	}
	for _, c := range cases {
		got, source, err := SelectKernel(c.configured, c.panel, c.network)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: want error, got %q/%q", c.name, got, source)
			}
			continue
		}
		if err != nil || got != c.want || source != c.source {
			t.Errorf("%s: SelectKernel(%q,%q,%q) = %q,%q,%v; want %q,%q", c.name,
				c.configured, c.panel, c.network, got, source, err, c.want, c.source)
		}
	}
}

func TestValidateNodeSpecUsesPanelKernel(t *testing.T) {
	// auto + panel singbox on xhttp must be rejected, not silently moved to xray.
	spec := &NodeSpec{Protocol: "vless", Network: "xhttp", KernelType: "singbox"}
	if err := ValidateNodeSpec(spec, config.KernelConfig{Type: KernelAuto}); err == nil {
		t.Fatal("auto + panel singbox + xhttp: want error")
	}
	// auto + panel xray on tcp is valid (xray serves tcp).
	spec = &NodeSpec{Protocol: "vless", Network: "tcp", KernelType: "xray"}
	if err := ValidateNodeSpec(spec, config.KernelConfig{Type: KernelAuto}); err != nil {
		t.Fatalf("auto + panel xray + tcp: %v", err)
	}
}
