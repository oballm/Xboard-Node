package main

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
)

func TestFreeHealthPort(t *testing.T) {
	inst := func(p int) config.Config { return config.Config{HealthPort: p} }
	cases := []struct {
		name     string
		existing []config.Config
		port     int
		want     int
	}{
		{"no instances", nil, 65530, 65530},
		{"port free", []config.Config{inst(65531)}, 65530, 65530},
		{"taken: next one", []config.Config{inst(65530)}, 65530, 65531},
		{"skips every taken port", []config.Config{inst(65530), inst(65531), inst(0)}, 65530, 65532},
		{"disabled stays disabled", []config.Config{inst(65530)}, 0, 0},
		{"runs out: disabled", []config.Config{inst(65535)}, 65535, 0},
	}
	for _, c := range cases {
		if got := freeHealthPort(c.existing, c.port); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

// A re-installed binding keeps a free port: re-running the installer for the
// binding that was moved to 65531 must not put it back on 65530.
func TestMergeInstanceHealthPort(t *testing.T) {
	inst := func(id string, p int) config.Config { return config.Config{InstanceID: id, HealthPort: p} }

	list, got := mergeInstance(nil, inst("a", 65530))
	if got.HealthPort != 65530 || len(list) != 1 {
		t.Fatalf("first: port %d, %d instances", got.HealthPort, len(list))
	}
	list, got = mergeInstance(list, inst("b", 65530))
	if got.HealthPort != 65531 || len(list) != 2 {
		t.Fatalf("append: port %d, %d instances", got.HealthPort, len(list))
	}
	list, got = mergeInstance(list, inst("b", 65530))
	if got.HealthPort != 65531 || len(list) != 2 || list[1].HealthPort != 65531 {
		t.Fatalf("re-install b: port %d, stored %d, %d instances", got.HealthPort, list[1].HealthPort, len(list))
	}
	list, got = mergeInstance(list, inst("a", 65530))
	if got.HealthPort != 65530 || list[0].HealthPort != 65530 {
		t.Fatalf("re-install a: port %d, stored %d", got.HealthPort, list[0].HealthPort)
	}
}
