package selfupdate

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

const checksumFile = "SHA256SUMS"

var (
	// DownloadBase is where releases come from. Only the build can change it
	// (-ldflags -X); the panel cannot.
	DownloadBase = "https://github.com/oballm/Xboard-Node/releases"

	maxBinarySize = int64(256 << 20)
	diskMargin    = uint64(32 << 20)
	// No overall timeout: a ~70MB binary over a slow link to GitHub can take
	// many minutes. A download is abandoned when no data arrives for
	// stallTimeout; the caller's ctx bounds the whole upgrade.
	httpClient = &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}}
	stallTimeout = 60 * time.Second

	tagPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$`)
	// releasePattern matches fixed version tags: v1.14.0, v1.14.0-rc.1. Their
	// content never changes, unlike rolling tags (next-build, dev).
	releasePattern = regexp.MustCompile(`^v[0-9]+(\.[0-9]+)*(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$`)

	// Seams for tests.
	inContainer = func() bool {
		for _, p := range []string{"/.dockerenv", "/run/.containerenv"} {
			if _, err := os.Stat(p); err == nil {
				return true
			}
		}
		return false
	}
	mountInfoPath = "/proc/self/mountinfo"

	busy atomic.Bool
)

// EnvAllowPrerelease set to 1 lets this node install rolling tags such as
// next-build or dev, which are rebuilt on every push: the same name can mean a
// different binary tomorrow. Meant for test nodes only. Fixed tags, including
// pre-releases like v1.14.0-rc.1, are always accepted.
const EnvAllowPrerelease = "XBOARD_NODE_ALLOW_PRERELEASE"

func allowPrerelease() bool { return os.Getenv(EnvAllowPrerelease) == "1" }

// ErrDisabled is returned when upgrades are not set up on this node.
var ErrDisabled = errors.New("self-upgrade is disabled: " + EnvUpdateDir + " is not set")

// Upgrade downloads release tag, verifies it and installs it as the trial
// version. The caller then restarts the process (through RestartTarget) to
// run it. Failures are recorded as the last result.
func Upgrade(ctx context.Context, tag string) (*Release, error) {
	if !busy.CompareAndSwap(false, true) {
		return nil, errors.New("an upgrade is already in progress")
	}
	defer busy.Store(false)

	info := current()
	if info == nil || info.dir == "" {
		return nil, ErrDisabled
	}
	rel, err := install(ctx, info, tag)
	if err != nil {
		recordFailure(info, tag, err)
		return nil, err
	}
	nlog.Core().Info("selfupdate: installed", "version", rel.Tag, "binary_version", rel.BinaryVersion)
	return rel, nil
}

func install(ctx context.Context, info *runtimeInfo, tag string) (*Release, error) {
	if !tagPattern.MatchString(tag) {
		return nil, fmt.Errorf("invalid version %q", tag)
	}
	if !releasePattern.MatchString(tag) && !allowPrerelease() {
		return nil, fmt.Errorf("%s is a rolling tag, not a fixed version like v1.14.0 or v1.14.0-rc.1; this node accepts only fixed versions (set %s=1 to allow rolling tags)", tag, EnvAllowPrerelease)
	}
	if runtime.GOOS != "linux" || (runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64") {
		return nil, fmt.Errorf("unsupported platform %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	if why := setupProblem(info); why != "" {
		return nil, errors.New(why)
	}
	if err := checkNotOnTrial(info); err != nil {
		return nil, err
	}
	vdir := versionsDir(info.dir)
	if err := os.MkdirAll(vdir, 0o755); err != nil {
		return nil, err
	}

	artifact := "xboard-node-linux-" + runtime.GOARCH
	want, err := fetchChecksum(ctx, tag, artifact)
	if err != nil {
		return nil, err
	}
	tmp, err := download(ctx, releaseURL(tag, artifact), vdir, want)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			os.Remove(tmp)
		}
	}()

	binVersion, err := binaryVersion(ctx, tmp)
	if err != nil {
		return nil, err
	}
	if releasePattern.MatchString(tag) && binVersion != tag {
		return nil, fmt.Errorf("release %s contains a binary of version %s", tag, binVersion)
	}
	if binVersion == info.ownVersion {
		return nil, fmt.Errorf("already running %s", binVersion)
	}
	rel := &Release{Tag: tag, BinaryVersion: binVersion, SHA256: want, File: fmt.Sprintf("xboard-node-%s-%s", tag, want[:12])}

	launcher := info.exe
	if info.child {
		launcher = info.launcher
	}
	err = withLock(info.dir, func() error {
		st, err := loadState(info.dir)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		next := &State{Current: rel, Trial: true}
		if st != nil {
			if st.Trial {
				return fmt.Errorf("version %s is still on trial", st.Current.Tag)
			}
			next.LauncherSHA256 = st.LauncherSHA256
			next.Previous = st.Current
			next.Last = st.Last
		}
		if next.LauncherSHA256 == "" {
			if next.LauncherSHA256, err = fileSHA256(launcher); err != nil {
				return fmt.Errorf("hash launcher: %w", err)
			}
		}
		if err := os.Rename(tmp, releasePath(info.dir, rel)); err != nil {
			return err
		}
		committed = true
		if err := syncDir(vdir); err != nil {
			return err
		}
		return saveState(info.dir, next)
	})
	if err != nil {
		return nil, fmt.Errorf("install: %w", err)
	}
	return rel, nil
}

// setupProblem says why upgrades cannot work here, or "". In a container the
// update dir must be a volume: installed versions would otherwise vanish when
// the container is recreated.
func setupProblem(info *runtimeInfo) string {
	if info == nil || info.dir == "" {
		return EnvUpdateDir + " is not set"
	}
	if inContainer() {
		if mp := mountPointOf(info.dir); mp == "" || mp == "/" {
			return info.dir + " is not a volume; mount one there, or upgrades are lost when the container is recreated"
		}
	}
	return ""
}

func checkNotOnTrial(info *runtimeInfo) error {
	st, err := loadState(info.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if st.Trial {
		return fmt.Errorf("version %s is still on trial; wait until it is confirmed or rolled back", st.Current.Tag)
	}
	return nil
}

func recordFailure(info *runtimeInfo, tag string, cause error) {
	if errors.Is(cause, ErrDisabled) {
		return
	}
	err := updateState(info.dir, func(st *State) (bool, error) {
		if st == nil {
			return false, nil // nothing installed yet; the log has it
		}
		st.Last = &Result{Tag: tag, Outcome: OutcomeFailed, Reason: cause.Error(), At: now().UTC()}
		return true, nil
	})
	if err != nil {
		nlog.Core().Warn("selfupdate: cannot record the failed upgrade", "error", err)
	}
	lastFailure.Store(&Result{Tag: tag, Outcome: OutcomeFailed, Reason: cause.Error(), At: now().UTC()})
}

// lastFailure keeps the latest failure in memory for Status when there is no
// state file to record it in.
var lastFailure atomic.Pointer[Result]

// pruneVersions deletes installed versions other than Current and Previous.
func pruneVersions(dir string, st *State) {
	keep := map[string]bool{}
	for _, r := range []*Release{st.Current, st.Previous} {
		if r != nil {
			keep[r.File] = true
		}
	}
	entries, err := os.ReadDir(versionsDir(dir))
	if err != nil {
		return
	}
	for _, e := range entries {
		if !keep[e.Name()] {
			os.Remove(filepath.Join(versionsDir(dir), e.Name()))
		}
	}
}

func fetchChecksum(ctx context.Context, tag, artifact string) (string, error) {
	resp, err := get(ctx, releaseURL(tag, checksumFile))
	if err != nil {
		return "", fmt.Errorf("fetch %s: %w", checksumFile, err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(io.LimitReader(resp.Body, 64<<10))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 2 && strings.TrimPrefix(f[1], "*") == artifact {
			sum := strings.ToLower(f[0])
			if _, err := hex.DecodeString(sum); err != nil || len(sum) != 64 {
				return "", fmt.Errorf("%s has a malformed checksum for %s", checksumFile, artifact)
			}
			return sum, nil
		}
	}
	return "", fmt.Errorf("%s has no checksum for %s", checksumFile, artifact)
}

// download writes src into dir, checks its SHA-256 and returns the temp file.
func download(ctx context.Context, src, dir, wantSHA string) (string, error) {
	resp, err := get(ctx, src)
	if err != nil {
		return "", fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()
	if resp.ContentLength > maxBinarySize {
		return "", fmt.Errorf("download: binary too large (%d bytes)", resp.ContentLength)
	}
	if resp.ContentLength > 0 {
		if free, err := freeBytes(dir); err == nil && free < uint64(resp.ContentLength)+diskMargin {
			return "", fmt.Errorf("download: not enough disk space in %s (%d bytes free, need %d)", dir, free, uint64(resp.ContentLength)+diskMargin)
		}
	}
	f, err := os.CreateTemp(dir, ".download-*")
	if err != nil {
		return "", fmt.Errorf("download: %w", err)
	}
	fail := func(e error) (string, error) {
		f.Close()
		os.Remove(f.Name())
		return "", e
	}
	h := sha256.New()
	body := &stallReader{r: resp.Body, timeout: stallTimeout, close: func() { resp.Body.Close() }}
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(body, maxBinarySize+1))
	body.stop()
	if err != nil {
		return fail(fmt.Errorf("download: %w", err))
	}
	if n > maxBinarySize {
		return fail(errors.New("download: binary too large"))
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != wantSHA {
		return fail(fmt.Errorf("checksum mismatch: got %s, want %s", got, wantSHA))
	}
	if err := f.Chmod(0o755); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// stallReader closes the body when no data arrives for timeout, so a stuck
// download fails instead of hanging until the overall deadline.
type stallReader struct {
	r       io.Reader
	timeout time.Duration
	close   func()
	timer   *time.Timer
	once    sync.Once
}

func (s *stallReader) Read(p []byte) (int, error) {
	s.once.Do(func() { s.timer = time.AfterFunc(s.timeout, s.close) })
	n, err := s.r.Read(p)
	if n > 0 {
		s.timer.Reset(s.timeout)
	}
	return n, err
}

func (s *stallReader) stop() {
	if s.timer != nil {
		s.timer.Stop()
	}
}

func get(ctx context.Context, src string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, src)
	}
	return resp, nil
}

// binaryVersion runs the new binary with -v: this proves it runs on this
// machine and tells which version it is.
func binaryVersion(ctx context.Context, bin string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-v")
	cmd.Env = withoutEnv(withoutEnv(os.Environ(), EnvUpdateDir), EnvLauncher)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("new binary does not run: %w", err)
	}
	f := strings.Fields(string(out))
	if len(f) < 2 || f[0] != "xboard-node" {
		return "", fmt.Errorf("new binary printed an unexpected version: %q", strings.TrimSpace(string(out)))
	}
	return f[1], nil
}

func releaseURL(tag, name string) string {
	return DownloadBase + "/download/" + url.PathEscape(tag) + "/" + name
}

func freeBytes(dir string) (uint64, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(dir, &s); err != nil {
		return 0, err
	}
	return s.Bavail * uint64(s.Bsize), nil
}

// mountPointOf returns the mount point holding path, from mountinfo.
func mountPointOf(path string) string {
	b, err := os.ReadFile(mountInfoPath)
	if err != nil {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	best := ""
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 5 {
			continue
		}
		mp := unescapeMount(f[4])
		if (mp == "/" || path == mp || strings.HasPrefix(path, mp+"/")) && len(mp) > len(best) {
			best = mp
		}
	}
	return best
}

// unescapeMount decodes the octal escapes (\040 for space) of mountinfo.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			var v int
			if _, err := fmt.Sscanf(s[i+1:i+4], "%03o", &v); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
