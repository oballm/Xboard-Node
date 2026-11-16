package kernel

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"gopkg.in/yaml.v3"
)

// LoadCustomConfig reads a custom config file (JSON or YAML) and returns it
// as a generic map. Returns nil if path is empty or file does not exist.
func LoadCustomConfig(path string) (map[string]any, error) {
	if path == "" {
		return nil, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			nlog.Core().Warn("custom config file not found, skipping", "path", path)
			return nil, nil
		}
		return nil, fmt.Errorf("read custom config %s: %w", path, err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	result := make(map[string]any)

	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "{") {
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("parse custom config as JSON: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("parse custom config as YAML: %w", err)
		}
	}

	nlog.Core().Info("loaded custom config", "path", path, "keys", mapKeys(result))
	return result, nil
}

// MergeAppendList appends items from src list into dst, returning the merged list.
// Each item is converted to map[string]any for JSON-marshal compatibility.
func MergeAppendList(dst []map[string]any, srcRaw any) []map[string]any {
	srcList, ok := toSliceOfMaps(srcRaw)
	if !ok {
		return dst
	}
	return append(dst, srcList...)
}

// MergePrependList prepends items from src list before dst items.
func MergePrependList(dst []map[string]any, srcRaw any) []map[string]any {
	srcList, ok := toSliceOfMaps(srcRaw)
	if !ok {
		return dst
	}
	return append(srcList, dst...)
}

func toSliceOfMaps(v any) ([]map[string]any, bool) {
	if v == nil {
		return nil, false
	}
	switch items := v.(type) {
	case []any:
		result := make([]map[string]any, 0, len(items))
		for _, item := range items {
			if m, ok := toMap(item); ok {
				result = append(result, m)
			}
		}
		return result, len(result) > 0
	case []map[string]any:
		return items, len(items) > 0
	default:
		return nil, false
	}
}

func toMap(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case map[any]any:
		result := make(map[string]any, len(m))
		for k, val := range m {
			result[fmt.Sprint(k)] = val
		}
		return result, true
	default:
		return nil, false
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// WireCustomConfig returns the panel-delivered custom_config to merge into a
// kernel's generated config, or nil when it must not be used.
//
// custom_config is raw kernel-native config (DNS, rule-sets, experimental,
// ...) that the panel writes for the kernel named by the node's kernel_type —
// panel-proxy treats an empty kernel_type as sing-box. Merging one kernel's
// dialect into the other breaks the kernel (sing-box rejects xray's "dns"
// shape and vice versa), so the wire config is used only when its dialect
// matches the kernel actually running. With kernel.type=auto the node follows
// the panel's kernel_type and they always match; a mismatch means the local
// config pins a different kernel than the panel, and the wire config is
// skipped with a warning (the local custom_config file still applies).
func WireCustomConfig(nc *model.NodeSpec, running string) map[string]any {
	if nc == nil || len(nc.CustomConfig) == 0 {
		return nil
	}
	dialect := strings.ToLower(strings.TrimSpace(nc.KernelType))
	switch dialect {
	case "", "sing-box":
		dialect = model.KernelSingBox
	}
	if dialect != running {
		nlog.Core().Warn("ignoring the panel's custom_config: it is written for another kernel",
			"panel_kernel_type", dialect, "running_kernel", running)
		return nil
	}
	return nc.CustomConfig
}
