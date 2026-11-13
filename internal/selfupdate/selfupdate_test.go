package selfupdate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- fixtures ---

func script(version string) []byte {
	return []byte(fmt.Sprintf("#!/bin/sh\necho 'xboard-node %s (built test)'\n", version))
}

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

type release struct {
	tag, binVersion string
	badSum          bool
}

// serve publishes releases like GitHub does: /download/<tag>/{SHA256SUMS,binary}.
func serve(t *testing.T, releases ...release) {
	t.Helper()
	artifact := "xboard-node-linux-" + runtime.GOARCH
	mux := http.NewServeMux()
	for _, r := range releases {
		bin := script(r.binVersion)
		s := sum(bin)
		if r.badSum {
			s = strings.Repeat("0", 64)
		}
		mux.HandleFunc("/download/"+r.tag+"/"+checksumFile, func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintf(w, "%s  xbctl-linux-amd64\n%s  %s\n", strings.Repeat("a", 64), s, artifact)
		})
		mux.HandleFunc("/download/"+r.tag+"/"+artifact, func(w http.ResponseWriter, _ *http.Request) { w.Write(bin) })
	}
	srv := httptest.NewServer(mux)
	old := DownloadBase
	DownloadBase = srv.URL
	t.Cleanup(func() { srv.Close(); DownloadBase = old })
}

type node struct {
	t        *testing.T
	dir      string
	launcher string
	execs    []string
	envs     [][]string
}

// newNode sets up a launcher of version v and an update dir.
func newNode(t *testing.T, v string) *node {
	t.Helper()
	root := t.TempDir()
	n := &node{t: t, dir: filepath.Join(root, "state"), launcher: filepath.Join(root, "bin", "xboard-node")}
	os.MkdirAll(filepath.Dir(n.launcher), 0o755)
	if err := os.WriteFile(n.launcher, script(v), 0o755); err != nil {
		t.Fatal(err)
	}
	oldExec, oldContainer := execBinary, inContainer
	execBinary = func(path string, _ []string, env []string) error {
		n.execs = append(n.execs, path)
		n.envs = append(n.envs, env)
		return nil
	}
	inContainer = func() bool { return false }
	t.Cleanup(func() { execBinary, inContainer = oldExec, oldContainer; setRuntime(nil) })
	return n
}

// boot starts the launcher (as the image or systemd would) and returns the
// path it exec'd, or "" when it ran itself.
func (n *node) boot(launcherVersion string) string {
	n.t.Helper()
	before := len(n.execs)
	start(&runtimeInfo{dir: n.dir, exe: n.launcher, ownVersion: launcherVersion})
	if len(n.execs) == before {
		return ""
	}
	return n.execs[len(n.execs)-1]
}

// child becomes the process the launcher exec'd.
func (n *node) child(path, version string) {
	setRuntime(&runtimeInfo{dir: n.dir, exe: path, ownVersion: version, child: true, launcher: n.launcher})
}

func (n *node) state() *State {
	n.t.Helper()
	st, err := loadState(n.dir)
	if err != nil {
		n.t.Fatal(err)
	}
	return st
}

func (n *node) upgrade(tag string) *Release {
	n.t.Helper()
	rel, err := Upgrade(context.Background(), tag)
	if err != nil {
		n.t.Fatalf("Upgrade(%s): %v", tag, err)
	}
	return rel
}

func fastHealth(t *testing.T, soak, timeout time.Duration) {
	oldSoak, oldTimeout, oldPoll := SoakPeriod, TrialTimeout, healthPoll
	SoakPeriod, TrialTimeout, healthPoll = soak, timeout, 5*time.Millisecond
	t.Cleanup(func() { SoakPeriod, TrialTimeout, healthPoll = oldSoak, oldTimeout, oldPoll })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

// --- the happy path ---

func TestUpgradeTrialConfirm(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	fastHealth(t, 50*time.Millisecond, 2*time.Second)
	n := newNode(t, "v1.0")

	if got := n.boot("v1.0"); got != "" {
		t.Fatalf("nothing installed, launcher must run itself, exec'd %s", got)
	}
	rel := n.upgrade("v2.0")
	st := n.state()
	if !st.Trial || st.Current.Tag != "v2.0" || st.Previous != nil || st.LauncherSHA256 != sum(script("v1.0")) {
		t.Fatalf("state after upgrade: %+v", st)
	}

	// Restart: the launcher execs v2 and tells it where the launcher is.
	path := n.boot("v1.0")
	if path != releasePath(n.dir, rel) {
		t.Fatalf("exec'd %q", path)
	}
	if !contains(n.envs[len(n.envs)-1], EnvLauncher+"="+n.launcher) {
		t.Fatal("launcher path not passed on")
	}
	if n.state().Starts != 1 {
		t.Fatalf("starts = %d", n.state().Starts)
	}

	n.child(path, "v2.0")
	if !InTrial() || RestartTarget() != n.launcher {
		t.Fatalf("InTrial=%v RestartTarget=%q", InTrial(), RestartTarget())
	}
	NodeStarted("a")
	Watch(func(string) { t.Error("must not restart") })
	NodeReported("a")
	waitFor(t, "confirmation", Confirmed)

	st = n.state()
	if st.Trial || st.Last == nil || st.Last.Outcome != OutcomeConfirmed || st.Last.Tag != "v2.0" {
		t.Fatalf("state after confirm: %+v", st)
	}
	if InTrial() {
		t.Fatal("still in trial")
	}
	if s := CurrentStatus(); !s.Enabled || s.Running != "v2.0" || s.Trial || s.Last.Outcome != OutcomeConfirmed {
		t.Fatalf("status: %+v", s)
	}
	// Later starts run the confirmed version without counting.
	if n.boot("v1.0") != path || n.state().Starts != 0 {
		t.Fatal("confirmed version not run as is")
	}
}

func TestSecondUpgradeKeepsLastKnownGoodAndPrunes(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"}, release{tag: "v3.0", binVersion: "v3.0"})
	fastHealth(t, 10*time.Millisecond, 2*time.Second)
	n := newNode(t, "v1.0")
	n.boot("v1.0")

	confirmAs := func(path, v string) {
		n.child(path, v)
		NodeStarted("a")
		NodeReported("a")
		Watch(func(string) {})
		waitFor(t, "confirm "+v, Confirmed)
	}
	r2 := n.upgrade("v2.0")
	confirmAs(n.boot("v1.0"), "v2.0")
	r3 := n.upgrade("v3.0")
	st := n.state()
	if st.Current.Tag != "v3.0" || st.Previous.Tag != "v2.0" {
		t.Fatalf("expected v2.0 kept as fallback: %+v", st)
	}
	confirmAs(n.boot("v1.0"), "v3.0")
	if !exists(releasePath(n.dir, r2)) || !exists(releasePath(n.dir, r3)) {
		t.Fatal("current and last known good must be kept")
	}
}

// --- health ---

func TestHealthNeedsEveryNodeAndTheSoak(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	fastHealth(t, 150*time.Millisecond, 5*time.Second)
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	n.upgrade("v2.0")
	n.child(n.boot("v1.0"), "v2.0")

	NodeStarted("node 1")
	NodeStarted("node 2")
	Watch(func(string) { t.Error("must not restart") })
	NodeReported("node 1")
	time.Sleep(300 * time.Millisecond) // past the soak, one node silent
	if Confirmed() {
		t.Fatal("confirmed with a node that never reported")
	}
	if ok, why := healthy(); ok || !strings.Contains(why, "node 2") {
		t.Fatalf("healthy() = %v, %q", ok, why)
	}
	NodeReported("node 2")
	waitFor(t, "confirmation", Confirmed)
}

func TestHealthWaitsForTheSoak(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	fastHealth(t, 300*time.Millisecond, 5*time.Second)
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	n.upgrade("v2.0")
	n.child(n.boot("v1.0"), "v2.0")
	NodeStarted("a")
	NodeReported("a")
	begin := time.Now()
	Watch(func(string) {})
	waitFor(t, "confirmation", Confirmed)
	if d := time.Since(begin); d < 250*time.Millisecond {
		t.Fatalf("confirmed after %s, before the soak", d)
	}
}

func TestUnhealthyVersionRollsBack(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	fastHealth(t, 10*time.Millisecond, 100*time.Millisecond)
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	r2 := n.upgrade("v2.0")
	n.child(n.boot("v1.0"), "v2.0")
	NodeStarted("a") // never reports

	var mu sync.Mutex
	var reason string
	Watch(func(r string) { mu.Lock(); reason = r; mu.Unlock() })
	waitFor(t, "restart", func() bool { mu.Lock(); defer mu.Unlock(); return reason != "" })
	if !strings.Contains(reason, "no successful report yet from a") {
		t.Fatalf("reason %q", reason)
	}

	NoteCleanExit() // the restart is planned; rollback still wins
	if got := n.boot("v1.0"); got != "" {
		t.Fatalf("expected the launcher (no previous version), exec'd %s", got)
	}
	st := n.state()
	if st.Current != nil || st.Trial || st.Last.Outcome != OutcomeRolledBack || !strings.Contains(st.Last.Reason, "not healthy") {
		t.Fatalf("state after rollback: %+v", st)
	}
	if exists(releasePath(n.dir, r2)) {
		t.Fatal("failed version should be deleted")
	}
}

func TestFailedNodeRollsBackInsteadOfConfirming(t *testing.T) {
	// Machine mode: one node exits with an error while the others keep
	// reporting. Nobody restarts the failed node, so the trial must fail
	// rather than be confirmed on the remaining nodes.
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	fastHealth(t, 50*time.Millisecond, 5*time.Second)
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	n.upgrade("v2.0")
	n.child(n.boot("v1.0"), "v2.0")

	NodeStarted("node 1")
	NodeStarted("node 2")
	NodeReported("node 1")
	NodeFailed("node 2", fmt.Errorf("listen udp :500: address already in use"))

	var mu sync.Mutex
	var reason string
	Watch(func(r string) { mu.Lock(); reason = r; mu.Unlock() })
	waitFor(t, "rollback", func() bool { mu.Lock(); defer mu.Unlock(); return reason != "" })
	if Confirmed() {
		t.Fatal("confirmed with a failed node")
	}
	if !strings.Contains(reason, "node 2") || !strings.Contains(reason, "address already in use") {
		t.Fatalf("reason %q", reason)
	}
	NoteCleanExit()
	if got := n.boot("v1.0"); got != "" {
		t.Fatalf("expected rollback to the launcher, exec'd %s", got)
	}
	if st := n.state(); st.Last == nil || st.Last.Outcome != OutcomeRolledBack {
		t.Fatalf("state: %+v", st)
	}
}

func TestStoppedNodeDoesNotFailTheTrial(t *testing.T) {
	// A node unbound from its machine (or stopped on purpose) is not a failure.
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	fastHealth(t, 50*time.Millisecond, 5*time.Second)
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	n.upgrade("v2.0")
	n.child(n.boot("v1.0"), "v2.0")
	NodeStarted("node 1")
	NodeStarted("node 2")
	NodeReported("node 1")
	NodeStopped("node 2")
	Watch(func(string) { t.Error("must not restart") })
	waitFor(t, "confirmation", Confirmed)
}

// --- crash loops and clean exits ---

func TestCrashLoopRollsBack(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"}, release{tag: "v3.0", binVersion: "v3.0"})
	fastHealth(t, 10*time.Millisecond, 2*time.Second)
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	n.upgrade("v2.0")
	p2 := n.boot("v1.0")
	n.child(p2, "v2.0")
	NodeStarted("a")
	NodeReported("a")
	Watch(func(string) {})
	waitFor(t, "confirm", Confirmed)

	r3 := n.upgrade("v3.0")
	for i := 1; i <= maxStarts; i++ { // v3 dies each time, before doing anything
		if got := n.boot("v1.0"); got != releasePath(n.dir, r3) {
			t.Fatalf("start %d exec'd %s", i, got)
		}
	}
	if got := n.boot("v1.0"); got != p2 {
		t.Fatalf("expected rollback to v2.0, exec'd %s", got)
	}
	st := n.state()
	if st.Current.Tag != "v2.0" || st.Trial || st.Last.Outcome != OutcomeRolledBack || !strings.Contains(st.Last.Reason, "abnormally") {
		t.Fatalf("state: %+v", st)
	}
}

func TestCleanExitsDoNotCountAsFailedStarts(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	r2 := n.upgrade("v2.0")
	for i := 0; i < 3*maxStarts; i++ { // panel restarts, docker stop/start ...
		p := n.boot("v1.0")
		if p != releasePath(n.dir, r2) {
			t.Fatalf("restart %d exec'd %q", i, p)
		}
		n.child(p, "v2.0")
		NoteCleanExit()
	}
	if st := n.state(); !st.Trial || st.Starts > 1 {
		t.Fatalf("clean restarts were counted: %+v", st)
	}
}

// --- integrity ---

func TestTamperedVersionIsNotRun(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	r2 := n.upgrade("v2.0")
	os.WriteFile(releasePath(n.dir, r2), []byte("#!/bin/sh\necho evil\n"), 0o755)
	if got := n.boot("v1.0"); got != "" {
		t.Fatalf("tampered binary exec'd: %s", got)
	}
	if st := n.state(); st.Current != nil || !strings.Contains(st.Last.Reason, "verification") {
		t.Fatalf("state: %+v", st)
	}
}

func TestReplacedLauncherWins(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	n.upgrade("v2.0")
	os.WriteFile(n.launcher, script("v1.5"), 0o755) // new image / installer / xbctl upgrade
	if got := n.boot("v1.5"); got != "" {
		t.Fatalf("must run the new launcher, exec'd %s", got)
	}
	if exists(statePath(n.dir)) || exists(versionsDir(n.dir)) {
		t.Fatal("installed versions should be dropped")
	}
}

func TestNewerStateSchemaIsNotTouched(t *testing.T) {
	n := newNode(t, "v1.0")
	os.MkdirAll(n.dir, 0o755)
	os.WriteFile(statePath(n.dir), []byte(`{"schema": 99}`), 0o644)
	if got := n.boot("v1.0"); got != "" {
		t.Fatal("must run the launcher")
	}
	if b, _ := os.ReadFile(statePath(n.dir)); string(b) != `{"schema": 99}` {
		t.Fatal("a newer state must not be rewritten")
	}
}

// --- refusals ---

func TestUpgradeRefusals(t *testing.T) {
	serve(t,
		release{tag: "v2.0", binVersion: "v2.0", badSum: true},
		release{tag: "v3.0", binVersion: "v9.9"},
		release{tag: "v1.0", binVersion: "v1.0"},
		release{tag: "v4.0", binVersion: "v4.0"},
		release{tag: "next-build", binVersion: "v1.13-5-gabc"},
		release{tag: "v4.0-rc.1", binVersion: "v4.0-rc.1"},
	)
	n := newNode(t, "v1.0")
	n.boot("v1.0")
	cases := []struct{ tag, want string }{
		{"v2.0", "checksum mismatch"},
		{"v3.0", "contains a binary of version v9.9"},
		{"v1.0", "already running"},
		{"v5.0", "HTTP 404"},
		{"../../etc", "invalid version"},
	}
	for _, c := range cases {
		if _, err := Upgrade(context.Background(), c.tag); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("Upgrade(%q) = %v, want %q", c.tag, err, c.want)
		}
	}
	if entries, _ := os.ReadDir(versionsDir(n.dir)); len(entries) != 0 {
		t.Fatalf("leftovers: %v", entries)
	}
	if s := CurrentStatus(); s.Last == nil || s.Last.Outcome != OutcomeFailed {
		t.Fatalf("failure not reported: %+v", s)
	}

	// A rolling tag is refused unless the node opts in; then it is accepted
	// whatever version its binary prints.
	if _, err := Upgrade(context.Background(), "next-build"); err == nil || !strings.Contains(err.Error(), "rolling tag") {
		t.Fatalf("rolling tag without opt-in: %v", err)
	}
	if _, err := Upgrade(context.Background(), "latest"); err == nil || !strings.Contains(err.Error(), "rolling tag") {
		t.Fatalf("latest: %v", err)
	}
	// A fixed pre-release tag needs no opt-in.
	if _, err := Upgrade(context.Background(), "v4.0-rc.1"); err != nil {
		t.Fatalf("pre-release tag: %v", err)
	}
	if err := updateState(n.dir, func(st *State) (bool, error) { st.Trial = false; return true, nil }); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvAllowPrerelease, "1")
	if !CurrentStatus().AllowPrerelease {
		t.Fatal("status must report the opt-in")
	}
	n.upgrade("next-build")
	// While it is on trial, no further upgrade.
	if _, err := Upgrade(context.Background(), "v4.0"); err == nil || !strings.Contains(err.Error(), "still on trial") {
		t.Fatalf("upgrade during trial: %v", err)
	}
	if st := n.state(); st.Last == nil || st.Last.Outcome != OutcomeFailed || st.Last.Tag != "v4.0" {
		t.Fatalf("refusal not recorded: %+v", st.Last)
	}
}

func TestUpgradeDisabledWithoutDir(t *testing.T) {
	n := newNode(t, "v1.0")
	start(&runtimeInfo{exe: n.launcher, ownVersion: "v1.0"})
	if _, err := Upgrade(context.Background(), "v2.0"); err != ErrDisabled {
		t.Fatalf("got %v", err)
	}
	if s := CurrentStatus(); s.Enabled {
		t.Fatal("status must say disabled")
	}
}

func TestContainerNeedsVolume(t *testing.T) {
	serve(t, release{tag: "v2.0", binVersion: "v2.0"})
	n := newNode(t, "v1.0")
	inContainer = func() bool { return true }
	mi := filepath.Join(t.TempDir(), "mountinfo")
	old := mountInfoPath
	mountInfoPath = mi
	t.Cleanup(func() { mountInfoPath = old })
	n.boot("v1.0")

	os.WriteFile(mi, []byte("1 0 0:1 / / rw - overlay overlay rw\n"), 0o644)
	if _, err := Upgrade(context.Background(), "v2.0"); err == nil || !strings.Contains(err.Error(), "not a volume") {
		t.Fatalf("got %v", err)
	}
	if st := CurrentStatus(); st.Enabled || !strings.Contains(st.DisabledReason, "not a volume") {
		t.Fatalf("status must report upgrades as disabled, with the reason: %+v", st)
	}
	esc := strings.ReplaceAll(n.dir, " ", `\040`)
	os.WriteFile(mi, []byte("1 0 0:1 / / rw - overlay overlay rw\n2 1 8:1 / "+esc+" rw - ext4 /dev/sda1 rw\n"), 0o644)
	if st := CurrentStatus(); !st.Enabled || st.DisabledReason != "" {
		t.Fatalf("with a volume upgrades are enabled: %+v", st)
	}
	n.upgrade("v2.0")
}

func TestUnescapeMount(t *testing.T) {
	if got := unescapeMount(`/a\040b/c`); got != "/a b/c" {
		t.Fatalf("got %q", got)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestStalledDownloadIsAbandoned(t *testing.T) {
	// A server that sends a little and then stops: the download must fail
	// after stallTimeout, not hang until the overall deadline.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000000")
		w.Write(make([]byte, 1000))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	old := stallTimeout
	stallTimeout = 200 * time.Millisecond
	defer func() { stallTimeout = old }()

	begin := time.Now()
	_, err := download(context.Background(), srv.URL, t.TempDir(), strings.Repeat("0", 64))
	if err == nil {
		t.Fatal("expected the stalled download to fail")
	}
	if d := time.Since(begin); d > 5*time.Second {
		t.Fatalf("took %s; should give up after the stall timeout", d)
	}
}
