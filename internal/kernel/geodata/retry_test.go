package geodata

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flakyServer answers the .dat with 502 for the first failures requests and
// serves data after that. It publishes no checksum.
func flakyServer(data []byte, failures int32) (*httptest.Server, *atomic.Int32) {
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha256sum") {
			http.NotFound(w, r)
			return
		}
		if reqs.Add(1) <= failures {
			http.Error(w, "upstream hiccup", http.StatusBadGateway)
			return
		}
		w.Write(data)
	}))
	return srv, &reqs
}

func shortRetries(t *testing.T) {
	t.Helper()
	old := retryDelays
	retryDelays = []time.Duration{10 * time.Millisecond, 10 * time.Millisecond}
	resetChecked()
	recentFail = sync.Map{}
	t.Cleanup(func() { retryDelays = old; recentFail = sync.Map{} })
}

func TestFirstDownloadRetriesUntilSuccess(t *testing.T) {
	shortRetries(t)
	data := fakeDat(50, 256)
	srv, reqs := flakyServer(data, 2)
	defer srv.Close()

	dir := t.TempDir()
	if err := ensureFile(dir, "geosite.dat", srv.URL+"/geosite.dat", nil); err != nil {
		t.Fatalf("third attempt should succeed: %v", err)
	}
	if n := reqs.Load(); n != 3 {
		t.Fatalf("dat requests = %d, want 3", n)
	}
	got, err := os.ReadFile(filepath.Join(dir, "geosite.dat"))
	if err != nil || len(got) != len(data) {
		t.Fatalf("file not written intact: len=%d err=%v", len(got), err)
	}
}

func TestFirstDownloadGivesUpAfterThreeAttempts(t *testing.T) {
	shortRetries(t)
	srv, reqs := flakyServer(fakeDat(10, 64), 1000)
	defer srv.Close()

	dir := t.TempDir()
	err := ensureFile(dir, "geosite.dat", srv.URL+"/geosite.dat", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if n := reqs.Load(); n != 3 {
		t.Fatalf("dat requests = %d, want 3", n)
	}
	if _, serr := os.Stat(filepath.Join(dir, "geosite.dat")); serr == nil {
		t.Fatal("no file should be left after a failed download")
	}

	// A second node starting right after gets the same error without
	// spending the retries again.
	if err2 := ensureFile(dir, "geosite.dat", srv.URL+"/geosite.dat", nil); err2 == nil {
		t.Fatal("expected the cached error")
	}
	if n := reqs.Load(); n != 3 {
		t.Fatalf("second caller re-downloaded: requests = %d, want 3", n)
	}

	// Once the cached failure is old, the next caller tries again.
	v, _ := recentFail.Load(filepath.Join(dir, "geosite.dat"))
	f := v.(failure)
	f.at = time.Now().Add(-2 * failReuse)
	recentFail.Store(filepath.Join(dir, "geosite.dat"), f)
	_ = ensureFile(dir, "geosite.dat", srv.URL+"/geosite.dat", nil)
	if n := reqs.Load(); n != 6 {
		t.Fatalf("after the reuse window: requests = %d, want 6", n)
	}
}
