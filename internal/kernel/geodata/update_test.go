package geodata

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// upstream serves one geo file and its .sha256sum, both replaceable.
type upstream struct {
	mu        sync.Mutex
	data      []byte
	sum       string // "" = compute from data
	downloads atomic.Int32
}

func (u *upstream) set(data []byte, sum string) {
	u.mu.Lock()
	u.data, u.sum = data, sum
	u.mu.Unlock()
}

func (u *upstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	data, sum := u.data, u.sum
	u.mu.Unlock()
	if strings.HasSuffix(r.URL.Path, ".sha256sum") {
		if sum == "" {
			h := sha256.Sum256(data)
			sum = hex.EncodeToString(h[:])
		}
		w.Write([]byte(sum + "  geosite.dat\n"))
		return
	}
	u.downloads.Add(1)
	w.Write(data)
}

func resetUpdater(t *testing.T, hook func() error) {
	t.Helper()
	resetChecked()
	updMu.Lock()
	tracked = map[string]string{}
	updStarted = true // tests drive updateOnce themselves
	reloadHook = hook
	updMu.Unlock()
	t.Cleanup(func() {
		updMu.Lock()
		tracked = map[string]string{}
		reloadHook = nil
		updMu.Unlock()
	})
}

func setup(t *testing.T, hook func() error) (*upstream, string) {
	resetUpdater(t, hook)
	up := &upstream{}
	up.set(fakeDat(8, 256), "")
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	// Ensure with a URL from the test server instead of GitHub.
	if err := ensureFile(dir, "geosite.dat", srv.URL+"/geosite.dat", nil); err != nil {
		t.Fatal(err)
	}
	track(filepath.Join(dir, "geosite.dat"), srv.URL+"/geosite.dat")
	return up, filepath.Join(dir, "geosite.dat")
}

func TestUpdateAppliesNewVersion(t *testing.T) {
	var reloads atomic.Int32
	up, path := setup(t, func() error { reloads.Add(1); return nil })
	v2 := fakeDat(9, 256)
	up.set(v2, "")

	updateOnce()
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, v2) {
		t.Fatal("file not updated")
	}
	if reloads.Load() != 1 {
		t.Fatalf("reloads = %d, want 1", reloads.Load())
	}
	if _, err := os.Stat(prevPath(path)); err == nil {
		t.Error("previous file left behind after a successful update")
	}
}

func TestUpdateSkipsUnchanged(t *testing.T) {
	var reloads atomic.Int32
	up, _ := setup(t, func() error { reloads.Add(1); return nil })
	before := up.downloads.Load()
	updateOnce()
	if up.downloads.Load() != before || reloads.Load() != 0 {
		t.Fatalf("unchanged file downloaded/reloaded (downloads %d→%d, reloads %d)", before, up.downloads.Load(), reloads.Load())
	}
}

func TestUpdateRejectsChecksumMismatch(t *testing.T) {
	var reloads atomic.Int32
	up, path := setup(t, func() error { reloads.Add(1); return nil })
	v1, _ := os.ReadFile(path)
	up.set(fakeDat(9, 256), strings.Repeat("ab", 32)) // published sum does not match
	updateOnce()
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, v1) || reloads.Load() != 0 {
		t.Fatal("file replaced although the checksum did not match")
	}
}

func TestUpdateRestoresOnReloadFailure(t *testing.T) {
	up, path := setup(t, func() error { return errors.New("bad geo data") })
	v1, _ := os.ReadFile(path)
	up.set(fakeDat(9, 256), "")
	updateOnce()
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, v1) {
		t.Fatal("previous file not restored after the reload failed")
	}
	if _, err := os.Stat(prevPath(path)); err == nil {
		t.Error("previous-file copy left behind")
	}
}

func TestInitialDownloadVerifiesChecksum(t *testing.T) {
	resetUpdater(t, nil)
	shortRetries(t)
	up := &upstream{}
	up.set(fakeDat(8, 256), strings.Repeat("cd", 32))
	srv := httptest.NewServer(up)
	defer srv.Close()
	dir := t.TempDir()
	if err := ensureFile(dir, "geosite.dat", srv.URL+"/geosite.dat", nil); err == nil {
		t.Fatal("accepted a download whose checksum does not match")
	}
	if _, err := os.Stat(filepath.Join(dir, "geosite.dat")); err == nil {
		t.Fatal("file written despite the checksum mismatch")
	}
}
