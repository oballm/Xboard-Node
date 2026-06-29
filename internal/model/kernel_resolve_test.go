package model

import "testing"

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
