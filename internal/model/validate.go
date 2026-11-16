package model

import (
	"fmt"
	"strings"

	"github.com/cedar2025/xboard-node/internal/config"
)

const (
	// KernelAuto lets the node pick its kernel: the panel's kernel_type when the
	// panel sends one, otherwise the transport (xray for transports sing-box
	// cannot handle, sing-box otherwise). See SelectKernel.
	KernelAuto = "auto"
	// KernelSingBox / KernelXray are the two concrete kernels.
	KernelSingBox = "singbox"
	KernelXray    = "xray"

	// Who chose the concrete kernel (see SelectKernel).
	KernelSourceLocal     = "local"     // kernel.type is singbox/xray in the node config
	KernelSourcePanel     = "panel"     // kernel.type=auto, the panel sent kernel_type
	KernelSourceTransport = "transport" // kernel.type=auto, no kernel_type from the panel
)

// SelectKernel picks the concrete kernel for a node.
//
//   - kernel.type singbox/xray in the local config always wins.
//   - kernel.type auto follows the panel's kernel_type when the panel sends one.
//     The panel generates kernel-specific config (routes, outbounds, DNS) for
//     the kernel it names, so the node must run that kernel; there is no
//     fallback — a panel kernel that cannot serve the transport is an error.
//   - kernel.type auto with no kernel_type (upstream Xboard, older panels, or
//     "auto") falls back to the transport rule, ResolveEffectiveKernel.
//
// The returned kernel is concrete; callers still run ValidateTransportKernel.
func SelectKernel(configured, panelKernel, network string) (kernel, source string, err error) {
	if strings.TrimSpace(configured) == "" {
		configured = KernelAuto
	}
	local, err := normalizeKernelType(configured)
	if err != nil {
		return "", "", err
	}
	if local != KernelAuto {
		return local, KernelSourceLocal, nil
	}
	if strings.TrimSpace(panelKernel) != "" {
		pk, err := normalizeKernelType(panelKernel)
		if err != nil {
			return "", "", fmt.Errorf("panel kernel_type: %w", err)
		}
		if pk != KernelAuto {
			return pk, KernelSourcePanel, nil
		}
	}
	return ResolveEffectiveKernel(network, KernelAuto), KernelSourceTransport, nil
}

func ValidateNodeSpec(n *NodeSpec, kcfg config.KernelConfig) error {
	if n == nil {
		return nil
	}

	// Validate against the kernel the node will actually run. An explicit or
	// panel-chosen sing-box paired with an unsupported transport is rejected by
	// ValidateTransportKernel below.
	kernelType, _, err := SelectKernel(kcfg.Type, n.KernelType, n.Network)
	if err != nil {
		return fmt.Errorf("select kernel: %w", err)
	}

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
