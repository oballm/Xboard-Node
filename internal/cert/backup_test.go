package cert

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
)

func selfManager(t *testing.T, shared, backup, domain string) *Manager {
	t.Helper()
	m := NewManager(config.CertConfig{CertMode: "self", Domain: domain, CertDir: shared, BackupDir: backup})
	if err := m.startSelfSigned(); err != nil {
		t.Fatal(err)
	}
	return m
}

// nodes mode: two nodes share the cert dir but self-sign different domains.
// Each must get its own cert back after a restart — before per-node backup
// dirs they wrote the same cert.pem and the second loaded the first's cert.
func TestSelfSignedBackupPerNode(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "certs")
	b1, b2 := filepath.Join(root, "node-1", "certs"), filepath.Join(root, "node-2", "certs")

	a := selfManager(t, shared, b1, "a.example.com").TLSCert()
	b := selfManager(t, shared, b2, "b.example.com").TLSCert()
	if bytes.Equal(a.CertPEM, b.CertPEM) {
		t.Fatal("node 2 got node 1's cert")
	}

	// Restart: each reloads its own backup.
	a2 := selfManager(t, shared, b1, "a.example.com").TLSCert()
	b2c := selfManager(t, shared, b2, "b.example.com").TLSCert()
	if !bytes.Equal(a.CertPEM, a2.CertPEM) || !bytes.Equal(b.CertPEM, b2c.CertPEM) {
		t.Fatal("a node did not get its own cert back after restart")
	}
	if _, err := os.Stat(filepath.Join(shared, "cert.pem")); err == nil {
		t.Error("backup written to the shared cert dir")
	}
}

// Upgrading from a version that kept the backup in the shared cert dir: a
// node adopts it only if it is for its own domain, and keeps a copy of its own.
func TestSelfSignedLegacySharedBackup(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "certs")
	// Old version: node 1 (a.example.com) wrote the shared backup.
	old := selfManager(t, shared, "", "a.example.com").TLSCert()

	a := selfManager(t, shared, filepath.Join(root, "node-1", "certs"), "a.example.com").TLSCert()
	if !bytes.Equal(a.CertPEM, old.CertPEM) {
		t.Fatal("node 1 did not keep its cert across the upgrade")
	}
	if _, err := os.Stat(filepath.Join(root, "node-1", "certs", "cert.pem")); err != nil {
		t.Errorf("adopted cert not copied to the node's own backup dir: %v", err)
	}

	b := selfManager(t, shared, filepath.Join(root, "node-2", "certs"), "b.example.com").TLSCert()
	if bytes.Equal(b.CertPEM, old.CertPEM) {
		t.Fatal("node 2 adopted node 1's cert for another domain")
	}
	if err := certCoversHost(b.CertPEM, "b.example.com"); err != nil {
		t.Fatalf("node 2 cert: %v", err)
	}
}

// The panel changed the self-signed domain: the old backup is not reused.
func TestSelfSignedDomainChangeRegenerates(t *testing.T) {
	dir := t.TempDir()
	first := selfManager(t, dir, "", "old.example.com").TLSCert()
	second := selfManager(t, dir, "", "new.example.com").TLSCert()
	if bytes.Equal(first.CertPEM, second.CertPEM) {
		t.Fatal("reused a cert for the old domain")
	}
	if err := certCoversHost(second.CertPEM, "new.example.com"); err != nil {
		t.Fatal(err)
	}
}
