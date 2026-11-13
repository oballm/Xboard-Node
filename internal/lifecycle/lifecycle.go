// Package lifecycle lets the panel ask the whole process to restart.
//
// The restart reuses the SIGTERM shutdown path (each node collects its kernel
// counters and sends a final report; the kernel stops) and then re-executes the binary in place
// (same PID, fresh process image). That works the same under systemd, Docker
// (also as PID 1) and a hand-started process, does not count against systemd's
// start-rate limit, and picks up a binary replaced on disk. Only if exec fails
// does it exit with RestartExitCode for the service manager to restart it; a
// non-zero code is deliberate: Docker "on-failure" and systemd
// "Restart=on-failure" ignore exit 0.
package lifecycle

import (
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/selfupdate"
)

// RestartExitCode is EX_TEMPFAIL: "try again later".
const RestartExitCode = 75

// RestartGrace ignores restart requests this soon after process start, so a
// directive replayed to a freshly started process cannot cause a restart loop.
const RestartGrace = 60 * time.Second

var (
	startedAt        = time.Now()
	restartRequested atomic.Bool
	signalSelf       = func() error { return syscall.Kill(os.Getpid(), syscall.SIGTERM) }
	exitProcess      = os.Exit
	// execSelf replaces the process image with the binary at our own path.
	// os.Executable strips Linux's " (deleted)" suffix, so after an on-disk
	// upgrade this runs the new binary. Go opens every fd close-on-exec, so
	// listen sockets and file locks are released by the exec itself.
	// A version started by the self-upgrade launcher restarts through it, so
	// the launcher decides what runs next (and can roll back).
	execSelf = func() error {
		exe := selfupdate.RestartTarget()
		if exe == "" {
			var err error
			if exe, err = os.Executable(); err != nil {
				return err
			}
		}
		return syscall.Exec(exe, append([]string{exe}, os.Args[1:]...), os.Environ())
	}
)

// WithinGrace reports whether the process started too recently to restart.
func WithinGrace() bool { return time.Since(startedAt) < RestartGrace }

// RequestRestart starts a graceful shutdown that ends in RestartExitCode.
// It returns immediately; the shutdown runs on the signal handler in main.
func RequestRestart(reason string) bool {
	if up := time.Since(startedAt); up < RestartGrace {
		nlog.Core().Warn("restart request ignored: process just started", "reason", reason, "uptime", up.Round(time.Second))
		return false
	}
	if !restartRequested.CompareAndSwap(false, true) {
		return false
	}
	nlog.Core().Warn("process restart requested, shutting down to restart", "reason", reason)
	// A shutdown that hangs is bounded by main's 15s shutdown timeout, which
	// re-execs when RestartRequested. Not covered (accepted, 2026-11-11): a
	// SIGTERM landing in the few ms while main swaps services on a local
	// config.yml reload is dropped when we run as PID 1 in a container; the
	// panel then shows the restart as unanswered and the admin retries.
	if err := signalSelf(); err != nil {
		nlog.Core().Error("restart: signal self failed, restarting now", "error", err)
		Restart()
	}
	return true
}

// Restart replaces the process with a fresh copy of itself, falling back to
// exiting with RestartExitCode for the service manager. It does not return.
// main calls it once the graceful shutdown requested by RequestRestart is done.
func Restart() {
	nlog.Core().Info("restarting process in place (re-exec)")
	// A planned restart is not a failed start of a version on trial.
	selfupdate.NoteCleanExit()
	if err := execSelf(); err != nil {
		nlog.Core().Error("re-exec failed, exiting for the service manager to restart", "error", err, "exit_code", RestartExitCode)
	}
	exitProcess(RestartExitCode)
}

// RestartRequested reports whether the current shutdown was a restart request.
func RestartRequested() bool { return restartRequested.Load() }
