package service

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/buildinfo"
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
