package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func expandAll(t *testing.T, path string) []*Config {
	t.Helper()
	rc, err := LoadRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	insts, err := rc.NormalizeInstances()
	if err != nil {
		t.Fatal(err)
	}
	var out []*Config
	for _, inst := range insts {
		out = append(out, inst.ExpandNodes()...)
	}
	return out
}

// xbctl (the one-line installer) writes one instance per node, each with its
// own config_dir. Their geo dir used to follow config_dir, so a second node
// downloaded the geo files again and the two xray nodes overwrote each other's
// process-wide XRAY_LOCATION_ASSET. Now all share the config file's directory,
// with the old per-instance dir as a fallback to copy from.
func TestGeoDirSharedAcrossXbctlInstances(t *testing.T) {
	path := writeTemp(t, "")
	base := filepath.Dir(path)
	inst5 := filepath.Join(base, "instances", "p-node-5")
	inst6 := filepath.Join(base, "instances", "p-node-6")
	path = writeTempAt(t, path, `
instances:
  - panel: {url: "https://p.example", token: "t5", node_id: 5}
    kernel: {type: auto, config_dir: "`+inst5+`"}
  - panel: {url: "https://p.example", token: "t6", node_id: 6}
    kernel: {type: auto, config_dir: "`+inst6+`"}
`)
	nodes := expandAll(t, path)
	if len(nodes) != 2 {
		t.Fatalf("nodes = %d", len(nodes))
	}
	for i, want := range []string{inst5, inst6} {
		n := nodes[i]
		if n.Kernel.GeoDataDir != base {
			t.Errorf("node %d geo_data_dir = %q, want shared %q", n.Panel.NodeID, n.Kernel.GeoDataDir, base)
		}
		if !reflect.DeepEqual(n.Kernel.GeoDataFallbackDirs, []string{want}) {
			t.Errorf("node %d fallback = %v, want [%s]", n.Panel.NodeID, n.Kernel.GeoDataFallbackDirs, want)
		}
		if n.Kernel.ConfigDir != want {
			t.Errorf("node %d config_dir changed: %q", n.Panel.NodeID, n.Kernel.ConfigDir)
		}
	}
	if err := ValidateStartupLayout(mustInstances(t, path)); err != nil {
		t.Fatal(err)
	}
}

// An explicit top-level geo_data_dir is used by every instance as is.
func TestGeoDirExplicitTopLevel(t *testing.T) {
	path := writeTemp(t, "")
	geo := filepath.Join(filepath.Dir(path), "mygeo")
	path = writeTempAt(t, path, `
kernel: {geo_data_dir: "`+geo+`"}
instances:
  - panel: {url: "https://p.example", token: "t5", node_id: 5}
  - panel: {url: "https://p.example", token: "t6", node_id: 6}
`)
	for _, n := range expandAll(t, path) {
		if n.Kernel.GeoDataDir != geo || len(n.Kernel.GeoDataFallbackDirs) != 0 {
			t.Errorf("node %d geo = %q fallback = %v, want %q and none", n.Panel.NodeID, n.Kernel.GeoDataDir, n.Kernel.GeoDataFallbackDirs, geo)
		}
	}
}

// nodes mode: a per-node config_dir no longer moves the geo dir with it, and
// each node keeps its cert backup in its own dir while the ACME store (the
// top-level cert dir) stays shared.
func TestNodesModeGeoSharedAndCertBackupPerNode(t *testing.T) {
	path := writeTemp(t, "")
	base := filepath.Dir(path)
	custom := filepath.Join(base, "custom-2")
	path = writeTempAt(t, path, `
panel: {url: "https://p.example", token: "t"}
cert: {cert_mode: self, domain: a.example.com}
nodes:
  - node_id: 1
  - node_id: 2
    kernel: {config_dir: "`+custom+`"}
`)
	nodes := expandAll(t, path)
	if len(nodes) != 2 {
		t.Fatalf("nodes = %d", len(nodes))
	}
	for _, n := range nodes {
		if n.Kernel.GeoDataDir != base {
			t.Errorf("node %d geo_data_dir = %q, want shared %q", n.Panel.NodeID, n.Kernel.GeoDataDir, base)
		}
		if n.Cert.CertDir != filepath.Join(base, "certs") {
			t.Errorf("node %d cert_dir = %q, want shared %q", n.Panel.NodeID, n.Cert.CertDir, filepath.Join(base, "certs"))
		}
		if want := filepath.Join(n.Kernel.ConfigDir, "certs"); n.Cert.BackupDir != want {
			t.Errorf("node %d backup dir = %q, want %q", n.Panel.NodeID, n.Cert.BackupDir, want)
		}
	}
	if nodes[0].Kernel.ConfigDir != filepath.Join(base, "node-1") || len(nodes[0].Kernel.GeoDataFallbackDirs) != 0 {
		t.Errorf("node 1: config_dir %q fallback %v", nodes[0].Kernel.ConfigDir, nodes[0].Kernel.GeoDataFallbackDirs)
	}
	if nodes[1].Kernel.ConfigDir != custom || !reflect.DeepEqual(nodes[1].Kernel.GeoDataFallbackDirs, []string{custom}) {
		t.Errorf("node 2: config_dir %q fallback %v, want %q and [%s]", nodes[1].Kernel.ConfigDir, nodes[1].Kernel.GeoDataFallbackDirs, custom, custom)
	}
}

func TestMachineNodeCertBackupIsItsCertDir(t *testing.T) {
	path := writeTemp(t, `
panel: {url: "https://p.example"}
machine: {machine_id: 4, token: "m"}
`)
	base := filepath.Dir(path)
	insts := mustInstances(t, path)
	n := insts[0].ExpandMachineNode(7, "vless")
	if n.Kernel.GeoDataDir != base {
		t.Errorf("geo_data_dir = %q, want %q", n.Kernel.GeoDataDir, base)
	}
	if want := filepath.Join(base, "node-7", "certs"); n.Cert.CertDir != want || n.Cert.BackupDir != want {
		t.Errorf("cert_dir %q backup %q, want both %q", n.Cert.CertDir, n.Cert.BackupDir, want)
	}
}

func mustInstances(t *testing.T, path string) []*Config {
	t.Helper()
	rc, err := LoadRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	insts, err := rc.NormalizeInstances()
	if err != nil {
		t.Fatal(err)
	}
	return insts
}

func writeTempAt(t *testing.T, path, content string) string {
	t.Helper()
	if err := writeFileForTest(path, content); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeFileForTest(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
