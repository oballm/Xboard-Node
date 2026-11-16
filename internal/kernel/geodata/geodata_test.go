package geodata

import (
	"bytes"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDat builds a GeoSiteList-shaped file: n length-delimited entries in
// field 1, each entry chunk bytes long.
func fakeDat(n, chunk int) []byte {
	var b bytes.Buffer
	entry := bytes.Repeat([]byte{0x0A, 0x02, 'c', 'n'}, chunk/4)
	for i := 0; i < n; i++ {
		b.WriteByte(0x0A)
		var l [binary.MaxVarintLen64]byte
		b.Write(l[:binary.PutUvarint(l[:], uint64(len(entry)))])
		b.Write(entry)
	}
	return b.Bytes()
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func resetChecked() { checked = sync.Map{} }

func TestValidateDat(t *testing.T) {
	dir := t.TempDir()
	good := fakeDat(8, 1024)
	cases := []struct {
		name string
		data []byte
		ok   bool
	}{
		{"valid", good, true},
		{"truncated", good[:len(good)-100], false},
		{"empty", nil, false},
		{"html error page", []byte("<!DOCTYPE html><html>rate limited</html>"), false},
		{"zero gap at an entry boundary", append(append(append([]byte{}, good[:1029]...), make([]byte, 64)...), good[1029:]...), false},
		{"trailing garbage", append(append([]byte{}, good...), 0x0A, 0xFF), false},
	}
	for _, c := range cases {
		p := filepath.Join(dir, c.name)
		writeFile(t, p, c.data)
		err := validateDat(p)
		if (err == nil) != c.ok {
			t.Errorf("%s: validateDat err=%v, want ok=%v", c.name, err, c.ok)
		}
	}
}

// Real upstream files, when present locally (set GEO_TEST_FILES to a
// directory of .dat files). Guards against the validator rejecting a real
// source's format.
func TestValidateDatRealFiles(t *testing.T) {
	dir := os.Getenv("GEO_TEST_FILES")
	if dir == "" {
		t.Skip("GEO_TEST_FILES not set")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.dat"))
	if len(files) == 0 {
		t.Skip("no .dat files")
	}
	for _, f := range files {
		if err := validateDat(f); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}

// slowServer serves data in 64 KiB chunks, pausing between chunks: 2ms by
// default, or slow[n] for the n-th request (1-based). Request dieAt (if > 0)
// aborts after dieAfter bytes (half the file when dieAfter is 0).
func slowServer(data []byte, slow map[int32]time.Duration, dieAt int32, dieAfter ...int) (*httptest.Server, *atomic.Int32) {
	var reqs atomic.Int32
	cut := len(data) / 2
	if len(dieAfter) > 0 {
		cut = dieAfter[0]
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha256sum") {
			http.NotFound(w, r) // no checksum published: structure check only
			return
		}
		n := reqs.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		const chunk = 64 << 10
		for i := 0; i < len(data); i += chunk {
			if n == dieAt && i >= cut {
				panic(http.ErrAbortHandler)
			}
			end := min(i+chunk, len(data))
			w.Write(data[i:end])
			w.(http.Flusher).Flush()
			if d, ok := slow[n]; ok {
				time.Sleep(d)
			} else {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}))
	return srv, &reqs
}

// The old code wrote every download to the same "<name>.tmp": two nodes
// downloading at once could rename a half-written file into place, and if the
// slower one then died the file stayed corrupt for good. Now each download
// has its own temp file and nodes in one process download a file once.
func TestConcurrentEnsureDownloadsOnceAndIntact(t *testing.T) {
	resetChecked()
	data := fakeDat(64, 4096)
	srv, reqs := slowServer(data, nil, 0)
	defer srv.Close()
	dir := t.TempDir()

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ensureFile(dir, "geosite.dat", srv.URL, nil)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("node %d: %v", i, err)
		}
	}
	if got := reqs.Load(); got != 1 {
		t.Errorf("downloads = %d, want 1", got)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "geosite.dat"))
	if !bytes.Equal(got, data) {
		t.Fatalf("file corrupt: %d bytes, want %d", len(got), len(data))
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(left) > 0 {
		t.Errorf("temp files left: %v", left)
	}
}

// Two writers into one directory that do not share the in-process lock (two
// processes / containers on one volume). The second starts while the first
// is ~5 chunks in, is much slower, and dies after 2 chunks — after the first
// has finished. This is the case that left a permanently corrupt file with
// the old shared "<name>.tmp" (first writer renamed a file the second had
// truncated; the gap it had not refilled stayed zero). The destination must
// end up complete.
func TestTwoWritersOneDiesNeverCorrupts(t *testing.T) {
	data := fakeDat(32, 64<<10) // ~2 MiB, 32 chunks
	srv, _ := slowServer(data, map[int32]time.Duration{1: 20 * time.Millisecond, 2: 300 * time.Millisecond}, 2, 2*(64<<10))
	defer srv.Close()
	dir := t.TempDir()
	dst := filepath.Join(dir, "geosite.dat")

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 100 * time.Millisecond)
			_ = downloadVerified(dst, srv.URL, "") // bypasses the lock on purpose
		}(i)
	}
	wg.Wait()
	got, _ := os.ReadFile(dst)
	if !bytes.Equal(got, data) {
		t.Fatalf("file corrupt: %d bytes, want %d", len(got), len(data))
	}
}

func TestFailedDownloadKeepsOldFile(t *testing.T) {
	data := fakeDat(16, 4096)
	srv, _ := slowServer(data, nil, 1) // dies half-way
	defer srv.Close()
	dst := filepath.Join(t.TempDir(), "geoip.dat")
	old := fakeDat(2, 64)
	writeFile(t, dst, old)

	if err := downloadVerified(dst, srv.URL, ""); err == nil {
		t.Fatal("want error from a download that died half-way")
	}
	got, _ := os.ReadFile(dst)
	if !bytes.Equal(got, old) {
		t.Fatal("old file was replaced by a failed download")
	}
}

func TestCorruptExistingFileIsReplaced(t *testing.T) {
	resetChecked()
	data := fakeDat(16, 1024)
	srv, reqs := slowServer(data, nil, 0)
	defer srv.Close()
	dir := t.TempDir()
	dst := filepath.Join(dir, "geosite.dat")
	// Left behind by the old in-place download: cut inside an entry. (A cut
	// exactly at an entry boundary is structurally valid and cannot be told
	// from a shorter file; downloads catch that case by Content-Length.)
	writeFile(t, dst, data[:len(data)/2+7])

	if err := ensureFile(dir, "geosite.dat", srv.URL, nil); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(dst)
	if !bytes.Equal(got, data) || reqs.Load() != 1 {
		t.Fatalf("corrupt file not replaced (reqs=%d, %d bytes)", reqs.Load(), len(got))
	}
	// A valid file is checked once per process, then trusted.
	if err := ensureFile(dir, "geosite.dat", srv.URL, nil); err != nil || reqs.Load() != 1 {
		t.Fatalf("valid file re-downloaded (reqs=%d, err=%v)", reqs.Load(), err)
	}
}

func TestAdoptFromFallbackDir(t *testing.T) {
	resetChecked()
	srv, reqs := slowServer(fakeDat(4, 64), nil, 0)
	defer srv.Close()
	shared := t.TempDir()
	oldDir := t.TempDir()
	broken := t.TempDir()
	data := fakeDat(8, 256)
	writeFile(t, filepath.Join(oldDir, "geoip.dat"), data)
	writeFile(t, filepath.Join(broken, "geoip.dat"), data[:100])

	if err := ensureFile(shared, "geoip.dat", srv.URL, []string{broken, oldDir}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(shared, "geoip.dat"))
	if !bytes.Equal(got, data) {
		t.Fatal("did not adopt the valid old copy")
	}
	if reqs.Load() != 0 {
		t.Fatalf("downloaded although a valid old copy existed (reqs=%d)", reqs.Load())
	}
}
