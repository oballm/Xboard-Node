// Package geodata makes sure the xray geo databases (geoip.dat / geosite.dat)
// that routing rules reference are present on disk.
//
// xray reads them from one process-wide directory (XRAY_LOCATION_ASSET) and
// caches the parsed matchers by file name, so every xray node in a process
// shares one geo directory and the files in it. This package is therefore
// written for concurrent use: several nodes may ask for the same file at the
// same time, and other processes (e.g. two containers on one volume) may be
// writing into the same directory.
//
// A file is never written in place: it is downloaded (or copied) into a
// uniquely named temp file next to it, checked, and only then renamed over the
// destination. A failed or partial download therefore never replaces a good
// file, and two writers can never mix their bytes.
//
// Files in use are kept up to date: about once a day the upstream .sha256sum
// is compared with the local file, a changed file is downloaded, verified
// against that checksum and swapped in, and the reload hook (xray: rebuild its
// geo matchers in place) applies it without restarting any kernel. If the
// reload fails the previous files are put back.
package geodata

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

const (
	xrayGeoIPURL   = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat"
	xrayGeoSiteURL = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat"
)

var httpClient = &http.Client{Timeout: 10 * time.Minute}

// fileLocks serialises work on one destination file within the process, so
// nodes starting together download it once: the first holds the lock and
// downloads, the rest wait and then find the finished file.
var (
	fileLocksMu sync.Mutex
	fileLocks   = map[string]*sync.Mutex{}
)

// checked records the files already validated in this process, so a file
// left behind by an older version is checked once, not on every kernel start.
var checked sync.Map // absolute path -> struct{}

func lockFile(path string) func() {
	fileLocksMu.Lock()
	mu, ok := fileLocks[path]
	if !ok {
		mu = &sync.Mutex{}
		fileLocks[path] = mu
	}
	fileLocksMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

// Ensure makes sure the xray geo databases the routes need are present in dir.
//
// fallbackDirs are older locations of the same files (before every node
// shared one geo directory). A valid copy found there is adopted instead of
// downloading again — a node that cannot reach GitHub keeps working after an
// upgrade moves its geo directory.
func Ensure(dir string, needGeoIP, needGeoSite bool, fallbackDirs ...string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create geo_data_dir %q: %w", dir, err)
	}

	type entry struct {
		name string
		url  string
	}
	var files []entry
	if needGeoIP {
		files = append(files, entry{"geoip.dat", xrayGeoIPURL})
	}
	if needGeoSite {
		files = append(files, entry{"geosite.dat", xrayGeoSiteURL})
	}

	var firstErr error
	for _, f := range files {
		if err := ensureFile(dir, f.name, f.url, fallbackDirs); err != nil {
			nlog.Core().Warn("geo database auto-download failed", "file", f.name, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		track(filepath.Join(dir, f.name), f.url)
	}
	return firstErr
}

func ensureFile(dir, name, url string, fallbackDirs []string) error {
	dst := filepath.Join(dir, name)
	if abs, err := filepath.Abs(dst); err == nil {
		dst = abs
	}
	unlock := lockFile(dst)
	defer unlock()

	if _, err := os.Stat(dst); err == nil {
		if _, ok := checked.Load(dst); ok {
			return nil
		}
		verr := validateDat(dst)
		if verr == nil {
			checked.Store(dst, struct{}{})
			return nil
		}
		// A broken file (e.g. left by the old in-place download) would
		// otherwise stay forever: present files were never re-fetched. Replace
		// it; until that succeeds the broken file stays, as nothing better exists.
		nlog.Core().Warn("geo database is corrupt, downloading it again", "file", dst, "error", verr)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat %s: %w", dst, err)
	} else if adoptFromFallback(dst, name, fallbackDirs) {
		checked.Store(dst, struct{}{})
		return nil
	}

	// Nodes starting together wait on the lock above; after a failure they
	// reuse its result instead of each spending the full retry time again.
	if v, ok := recentFail.Load(dst); ok {
		f := v.(failure)
		if time.Since(f.at) < failReuse {
			return f.err
		}
	}

	nlog.Core().Info("geo database missing, downloading automatically", "file", name, "url", url)
	if err := downloadWithRetry(dst, url); err != nil {
		recentFail.Store(dst, failure{err: err, at: time.Now()})
		return err
	}
	recentFail.Delete(dst)
	checked.Store(dst, struct{}{})
	if fi, err := os.Stat(dst); err == nil {
		nlog.Core().Info("geo database ready", "file", name, "size_kb", fi.Size()/1024)
	}
	return nil
}

// adoptFromFallback copies a valid name from one of fallbackDirs to dst.
func adoptFromFallback(dst, name string, fallbackDirs []string) bool {
	for _, fb := range fallbackDirs {
		if fb == "" {
			continue
		}
		src := filepath.Join(fb, name)
		if abs, err := filepath.Abs(src); err == nil && abs == dst {
			continue
		}
		f, err := os.Open(src)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				nlog.Core().Warn("geo database: cannot read old copy", "file", src, "error", err)
			}
			continue
		}
		_, err = writeValidated(dst, f, -1, nil, "")
		f.Close()
		if err != nil {
			nlog.Core().Warn("geo database: old copy not usable", "file", src, "error", err)
			continue
		}
		nlog.Core().Info("geo database copied to the shared geo directory", "from", src, "to", dst)
		return true
	}
	return false
}

// retryDelays are the waits before the second and third download attempts
// when a kernel needs a file it does not have. They cover a brief upstream
// error (timeout, 5xx, rate limit); the daily update has its own schedule.
var retryDelays = []time.Duration{5 * time.Second, 15 * time.Second}

// failReuse is how long a failed download's error is handed to later callers
// for the same file without trying again.
const failReuse = time.Minute

type failure struct {
	err error
	at  time.Time
}

var recentFail sync.Map // absolute path -> failure

// downloadWithRetry tries downloadVerified once, then again after each of
// retryDelays, and returns the last error.
func downloadWithRetry(dst, url string) error {
	err := downloadVerified(dst, url, "")
	for i, d := range retryDelays {
		if err == nil {
			return nil
		}
		nlog.Core().Warn("geo database download failed, retrying", "file", dst, "attempt", i+1, "retry_in", d, "error", err)
		time.Sleep(d)
		err = downloadVerified(dst, url, "")
	}
	return err
}

// downloadVerified downloads url to dst, verified against the upstream
// "<url>.sha256sum" when it can be fetched (a source without one is still
// accepted on the structure check alone). The checksum is fetched again on a
// mismatch, in case upstream published a new release between the two
// requests. prev, when set, keeps the replaced file there (see writeValidated).
func downloadVerified(dst, url, prev string) error {
	sum, sumErr := fetchSum(url)
	if sumErr != nil {
		nlog.Core().Debug("geo database: no checksum, relying on the structure check", "url", url, "error", sumErr)
	}
	verify := func(got string) error {
		if sum == "" || got == sum {
			return nil
		}
		if again, err := fetchSum(url); err == nil && got == again {
			return nil
		}
		return fmt.Errorf("checksum mismatch: got %s, upstream %s", got, sum)
	}

	resp, err := httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("http get %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status from %s: %s", url, resp.Status)
	}
	if _, err := writeValidated(dst, resp.Body, resp.ContentLength, verify, prev); err != nil {
		return fmt.Errorf("download %s: %w", url, err)
	}
	return nil
}

// fetchSum returns the hex SHA-256 published as "<url>.sha256sum"
// ("<hex>  <name>", as Loyalsoldier and XTLS publish it).
func fetchSum(url string) (string, error) {
	resp, err := httpClient.Get(url + ".sha256sum")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("checksum: HTTP %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return "", errors.New("checksum: empty file")
	}
	sum := strings.ToLower(fields[0])
	if _, err := hex.DecodeString(sum); err != nil || len(sum) != 64 {
		return "", fmt.Errorf("checksum: not a sha256: %q", fields[0])
	}
	return sum, nil
}

// writeValidated copies r into a unique temp file next to dst, checks it
// (length when wantLen >= 0, the file structure, then verify with its SHA-256
// when verify is not nil) and renames it over dst. When prev is set and dst
// exists, the replaced file is kept at prev (hard link) so it can be put back.
// On any failure the temp file is removed and dst is left untouched. Returns
// the hex SHA-256 of the written file.
func writeValidated(dst string, r io.Reader, wantLen int64, verify func(sum string) error, prev string) (sum string, err error) {
	f, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".*.tmp")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			os.Remove(tmp)
		}
	}()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		f.Close()
		return "", fmt.Errorf("write geo database: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close temp file: %w", err)
	}
	if wantLen >= 0 && n != wantLen {
		return "", fmt.Errorf("incomplete geo database: got %d of %d bytes", n, wantLen)
	}
	if err := validateDat(tmp); err != nil {
		return "", err
	}
	sum = hex.EncodeToString(h.Sum(nil))
	if verify != nil {
		if err := verify(sum); err != nil {
			return "", err
		}
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		return "", fmt.Errorf("chmod temp file: %w", err)
	}
	if prev != "" {
		os.Remove(prev)
		if _, statErr := os.Stat(dst); statErr == nil {
			if err := os.Link(dst, prev); err != nil {
				return "", fmt.Errorf("keep previous file: %w", err)
			}
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		return "", fmt.Errorf("rename to destination: %w", err)
	}
	return sum, nil
}

// validateDat checks that path is a well-formed protobuf message that ends
// exactly at the end of the file. geoip.dat / geosite.dat are a GeoIPList /
// GeoSiteList (a list of length-delimited entries), so a truncated download,
// an HTML error page or a zero-filled gap at an entry boundary fails here. It
// walks the top-level fields only — no unmarshalling, constant memory — and
// accepts any field numbers, so a source that adds fields still passes.
func validateDat(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	if size == 0 {
		return errors.New("invalid geo database: empty file")
	}

	r := &countingReader{r: bufio.NewReaderSize(f, 64<<10)}
	skip := func(n uint64) error {
		if n > uint64(size-r.n) {
			return fmt.Errorf("invalid geo database: field of %d bytes at offset %d runs past the end (%d bytes)", n, r.n, size)
		}
		d, err := r.r.Discard(int(n))
		r.n += int64(d)
		return err
	}
	for r.n < size {
		at := r.n
		tag, err := binary.ReadUvarint(r)
		if err != nil {
			return fmt.Errorf("invalid geo database: bad field tag at offset %d: %w", at, err)
		}
		if tag>>3 == 0 {
			return fmt.Errorf("invalid geo database: field number 0 at offset %d", at)
		}
		switch tag & 7 {
		case 0: // varint
			if _, err := binary.ReadUvarint(r); err != nil {
				return fmt.Errorf("invalid geo database: bad varint at offset %d: %w", at, err)
			}
		case 1: // 64-bit
			err = skip(8)
		case 2: // length-delimited
			l, lerr := binary.ReadUvarint(r)
			if lerr != nil {
				return fmt.Errorf("invalid geo database: bad length at offset %d: %w", at, lerr)
			}
			err = skip(l)
		case 5: // 32-bit
			err = skip(4)
		default:
			return fmt.Errorf("invalid geo database: unsupported wire type %d at offset %d", tag&7, at)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type countingReader struct {
	r *bufio.Reader
	n int64
}

func (c *countingReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

// ─── Daily update ───────────────────────────────────────────────────────────

const (
	updateInterval = 24 * time.Hour
	updateJitter   = time.Hour
	firstCheck     = 10 * time.Minute
)

var (
	updMu      sync.Mutex
	tracked    = map[string]string{} // files in use: path -> download URL
	updStarted bool
	reloadHook func() error
)

// SetReloadHook registers how running kernels pick up updated files (xray:
// rebuild its geo matchers in place). Without one, updated files take effect
// at the next kernel start.
func SetReloadHook(fn func() error) {
	updMu.Lock()
	reloadHook = fn
	updMu.Unlock()
}

// track adds a file in use to the daily update, starting it on first use.
func track(path, url string) {
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	updMu.Lock()
	defer updMu.Unlock()
	tracked[path] = url
	if !updStarted {
		updStarted = true
		go updateLoop()
	}
}

// updateLoop checks 10–70 minutes after the first file comes into use, then
// about once a day. The jitter spreads many nodes' requests to GitHub.
func updateLoop() {
	wait := firstCheck + rand.N(updateJitter)
	for {
		time.Sleep(wait)
		updateOnce()
		wait = updateInterval + rand.N(updateJitter)
	}
}

// updateOnce brings every file in use up to date with upstream and applies
// the changed ones with the reload hook. If the reload fails, the previous
// files are put back, so the files on disk always match what the running
// kernels use and a later kernel start does not pick up data that failed.
func updateOnce() {
	updMu.Lock()
	files := make([]string, 0, len(tracked))
	urls := make(map[string]string, len(tracked))
	for path, url := range tracked {
		files = append(files, path)
		urls[path] = url
	}
	hook := reloadHook
	updMu.Unlock()
	sort.Strings(files)

	var updated []string
	for _, path := range files {
		changed, err := updateFile(path, urls[path])
		if err != nil {
			nlog.Core().Warn("geo database update failed, keeping the current file", "file", path, "error", err)
			continue
		}
		if changed {
			updated = append(updated, path)
		}
	}
	if len(updated) == 0 {
		nlog.Core().Debug("geo databases are up to date")
		return
	}

	if hook != nil {
		if err := hook(); err != nil {
			nlog.Core().Error("geo database reload failed, restoring the previous files", "files", updated, "error", err)
			for _, path := range updated {
				unlock := lockFile(path)
				if err := os.Rename(prevPath(path), path); err != nil {
					nlog.Core().Error("geo database: restoring the previous file failed", "file", path, "error", err)
				}
				unlock()
			}
			return
		}
	}
	for _, path := range updated {
		os.Remove(prevPath(path))
	}
	nlog.Core().Info("geo databases updated", "files", updated, "applied_without_restart", hook != nil)
}

func prevPath(path string) string {
	return filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".prev")
}

// updateFile replaces path with the upstream version if its checksum differs.
// The replaced file is kept at prevPath(path) for updateOnce to put back.
func updateFile(path, url string) (bool, error) {
	want, err := fetchSum(url)
	if err != nil {
		return false, fmt.Errorf("fetch checksum: %w", err)
	}
	unlock := lockFile(path)
	defer unlock()
	have, err := fileSum(path)
	if err == nil && have == want {
		return false, nil
	}
	if err := downloadVerified(path, url, prevPath(path)); err != nil {
		return false, err
	}
	return true, nil
}

func fileSum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
