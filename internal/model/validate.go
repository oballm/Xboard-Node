package model

import (
	"fmt"
	"strings"

	"github.com/cedar2025/xboard-node/internal/config"
)

const (
	// KernelAuto lets the node pick its kernel from the transport: xray for
	// transports sing-box cannot handle (xhttp/splithttp), sing-box otherwise.
	KernelAuto = "auto"
	// KernelSingBox / KernelXray are the two concrete kernels.
	KernelSingBox = "singbox"
	KernelXray    = "xray"
)

func ValidateNodeSpec(n *NodeSpec, kcfg config.KernelConfig) error {
	if n == nil {
		return nil
	}

	effectiveKernelType := strings.TrimSpace(kcfg.Type)
	if effectiveKernelType == "" {
		effectiveKernelType = strings.TrimSpace(n.KernelType)
	}
	kernelType, err := normalizeKernelType(effectiveKernelType)
	if err != nil {
		return fmt.Errorf("normalize kernel type: %w", err)
	}
	// Resolve "auto" into a concrete kernel based on the transport. Explicit
	// "singbox"/"xray" pass through unchanged — an explicit sing-box paired with
	// an unsupported transport is then rejected by validateTransportKernel below.
	kernelType = ResolveEffectiveKernel(n.Network, kernelType)

	additionalOutboundSources, err := collectAdditionalOutboundTagSources(kcfg.CustomConfig, kcfg.CustomOutbound)
	if err != nil {
		return fmt.Errorf("collect additional outbound tags: %w", err)
	}
	if err := validateOutboundTagCollisions(n.CustomOutbounds, additionalOutboundSources); err != nil {
		return fmt.Errorf("validate outbound tags: %w", err)
	}
	additionalTags := additionalTagNames(additionalOutboundSources)
	availableTags := buildAvailableOutboundTags(n.CustomOutbounds, additionalTags)
	if err := ValidateCustomOutboundsForKernel(n.CustomOutbounds, kernelType, additionalTags); err != nil {
		return fmt.Errorf("validate custom outbounds: %w", err)
	}
	if err := ValidateCustomRouteRules(n.CustomRouteRules, kernelType, availableTags); err != nil {
		return fmt.Errorf("validate custom route rules: %w", err)
	}
	if err := ValidateTransportKernel(n.Network, kernelType); err != nil {
		return err
	}
	return nil
}

// singboxUnsupportedTransports lists transport types that sing-box does not support.
var singboxUnsupportedTransports = map[string]bool{
	"xhttp":     true,
	"splithttp": true,
}

// ValidateTransportKernel reports an error when the concrete kernel cannot
// serve the given transport (currently: sing-box cannot do xhttp/splithttp).
func ValidateTransportKernel(network, kernelType string) error {
	net := strings.ToLower(strings.TrimSpace(network))
	if kernelType == KernelSingBox && singboxUnsupportedTransports[net] {
		return fmt.Errorf("transport %q is not supported by sing-box kernel; use xray kernel instead", net)
	}
	return nil
}

// ResolveEffectiveKernel turns a configured kernel strategy into the concrete
// kernel for a transport. Only "auto" is resolved: it picks xray for transports
// sing-box cannot handle (xhttp/splithttp) and sing-box otherwise. Explicit
// "singbox"/"xray" are returned unchanged (no transport-based override) — an
// explicit, incompatible choice is surfaced as an error elsewhere, not silently
// corrected. Input should be a normalized kernel value.
func ResolveEffectiveKernel(network, configuredKernel string) string {
	if strings.ToLower(strings.TrimSpace(configuredKernel)) != KernelAuto {
		return configuredKernel
	}
	if singboxUnsupportedTransports[strings.ToLower(strings.TrimSpace(network))] {
		return KernelXray
	}
	return KernelSingBox
}

func normalizeKernelType(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case KernelAuto:
		return KernelAuto, nil
	case "singbox", "sing-box":
		return KernelSingBox, nil
	case "xray":
		return KernelXray, nil
	default:
		return "", fmt.Errorf("unsupported kernel type %q", value)
	}
}

func buildAvailableOutboundTags(structured []OutboundConfig, rawTags []string) map[string]struct{} {
	available := map[string]struct{}{
		"direct": {},
		"block":  {},
	}
	for _, outbound := range structured {
		tag := strings.ToLower(strings.TrimSpace(outbound.Tag))
		if tag != "" {
			available[tag] = struct{}{}
		}
	}
	for _, tag := range rawTags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag != "" {
			available[tag] = struct{}{}
		}
	}
	return available
}
