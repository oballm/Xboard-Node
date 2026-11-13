package selfupdate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

const (
	// EnvUpdateDir is where upgrades and their state are kept. Unset disables
	// upgrades.
	EnvUpdateDir = "XBOARD_NODE_UPDATE_DIR"
	// EnvLauncher is set by the launcher for the version it starts, so that
	// version restarts through the launcher.
	EnvLauncher = "XBOARD_NODE_LAUNCHER"
)

var (
	// maxStarts unclean exits of a trial version trigger a rollback.
	maxStarts = 3

	// Seams for tests.
	execBinary = syscall.Exec
	now        = time.Now
)

type runtimeInfo struct {
	dir        string // "" when upgrades are disabled
	exe        string
	ownVersion string
	child      bool   // started by the launcher from <dir>/versions
	launcher   string // set when child
}

var (
	rtMu sync.Mutex
	rt   *runtimeInfo
)

func current() *runtimeInfo {
	rtMu.Lock()
	defer rtMu.Unlock()
	return rt
}

func setRuntime(info *runtimeInfo) {
	rtMu.Lock()
	rt = info
	rtMu.Unlock()
	resetHealth()
}

// Launch must run first thing in main, before the config is read. When this
// binary is the launcher and a version is installed, it execs that version
// and does not return.
func Launch(ownVersion string) {
	exe, err := os.Executable()
	if err == nil {
		exe, err = filepath.EvalSymlinks(exe)
	}
	if err != nil {
		nlog.Core().Warn("selfupdate: cannot locate own executable; upgrades disabled", "error", err)
		setRuntime(&runtimeInfo{ownVersion: ownVersion})
		return
	}
	info := &runtimeInfo{exe: exe, ownVersion: ownVersion}
	if dir := os.Getenv(EnvUpdateDir); dir != "" {
		info.dir = filepath.Clean(dir)
		if isUnder(exe, versionsDir(info.dir)) {
			info.child = true
			info.launcher = os.Getenv(EnvLauncher)
		}
	}
	start(info)
}

// start records the runtime and, for the launcher, execs the selected version.
func start(info *runtimeInfo) {
	setRuntime(info)
	if info.dir == "" || info.child {
		return
	}
	target, err := selectVersion(info)
	if err != nil {
		nlog.Core().Error("selfupdate: cannot read upgrade state; running the launcher binary", "error", err)
		return
	}
	if target == "" {
		return
	}
	env := append(withoutEnv(os.Environ(), EnvLauncher), EnvLauncher+"="+info.exe)
	args := append([]string{target}, os.Args[1:]...)
	if err := execBinary(target, args, env); err != nil {
		// The state is unchanged; the next start tries again, and while on
		// trial each such start counts as a failed one.
		nlog.Core().Error("selfupdate: cannot start the installed version; running the launcher binary", "path", target, "error", err)
	}
}

// selectVersion applies pending transitions and returns the binary to exec,
// or "" for the launcher itself.
func selectVersion(info *runtimeInfo) (string, error) {
	var target string
	err := updateState(info.dir, func(st *State) (bool, error) {
		if st == nil {
			return false, nil
		}
		sum, err := fileSHA256(info.exe)
		if err != nil {
			return false, err
		}
		if st.LauncherSHA256 != sum {
			nlog.Core().Warn("selfupdate: the launcher binary was replaced by hand; dropping installed versions",
				"running", tagOf(st.Current))
			return false, wipe(info.dir)
		}
		changed := false
		if st.Trial {
			changed = true
			switch {
			case st.RollbackReason != "":
				rollBack(info.dir, st, st.RollbackReason)
			case st.CleanExit:
				st.CleanExit = false
			default:
				st.Starts++
				if st.Starts > maxStarts {
					rollBack(info.dir, st, fmt.Sprintf("exited abnormally %d times in a row", maxStarts))
				}
			}
		}
		for st.Current != nil {
			p := releasePath(info.dir, st.Current)
			err := verifyFile(p, st.Current.SHA256)
			if err == nil {
				target = p
				break
			}
			rollBack(info.dir, st, "installed binary failed verification: "+err.Error())
			changed = true
		}
		if target != "" {
			nlog.Core().Info("selfupdate: starting installed version", "version", st.Current.Tag, "trial", st.Trial, "start", st.Starts)
		}
		return changed, nil
	})
	return target, err
}

// rollBack makes the last known good version current and deletes the failed
// one.
func rollBack(dir string, st *State, reason string) {
	failed := st.Current
	st.Current, st.Previous = st.Previous, nil
	st.Trial, st.Starts, st.CleanExit, st.RollbackReason = false, 0, false, ""
	if failed == nil {
		return
	}
	st.Last = &Result{Tag: failed.Tag, Outcome: OutcomeRolledBack, Reason: reason, At: now().UTC()}
	nlog.Core().Error("selfupdate: rolling back", "version", failed.Tag, "reason", reason, "back_to", tagOf(st.Current))
	if st.Current == nil || st.Current.File != failed.File {
		os.Remove(releasePath(dir, failed))
	}
}

func wipe(dir string) error {
	if err := os.RemoveAll(versionsDir(dir)); err != nil {
		return err
	}
	if err := os.Remove(statePath(dir)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(dir)
}

// RestartTarget is the binary a process restart should exec: the launcher
// when this process was started by it, so a trial version restarts through
// the launcher; otherwise "" (the caller's own executable).
func RestartTarget() string {
	info := current()
	if info != nil && info.child && info.launcher != "" {
		return info.launcher
	}
	return ""
}

// NoteCleanExit records that the trial version is exiting on purpose, so the
// next start is not counted as a failed one. Call it right before a planned
// exit or re-exec.
func NoteCleanExit() {
	info := current()
	if info == nil || !info.child {
		return
	}
	err := updateState(info.dir, func(st *State) (bool, error) {
		if st == nil || !st.Trial || !isCandidate(info, st) {
			return false, nil
		}
		st.CleanExit = true
		return true, nil
	})
	if err != nil {
		nlog.Core().Warn("selfupdate: cannot record clean exit", "error", err)
	}
}

// isCandidate reports whether this process runs st.Current.
func isCandidate(info *runtimeInfo, st *State) bool {
	return info.child && st.Current != nil && releasePath(info.dir, st.Current) == info.exe
}

func tagOf(r *Release) string {
	if r == nil {
		return "launcher"
	}
	return r.Tag
}

func isUnder(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != "." && !strings.HasPrefix(rel, "..")
}

func withoutEnv(env []string, key string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		if !strings.HasPrefix(kv, key+"=") {
			out = append(out, kv)
		}
	}
	return out
}
