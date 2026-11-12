package lifecycle

import (
	"errors"
	"testing"
	"time"
)

type fakes struct {
	signals int
	execs   int
	exits   []int
	execErr error
}

func withFakes(t *testing.T, uptime time.Duration) *fakes {
	t.Helper()
	f := &fakes{}
	oldStarted, oldSignal, oldExit, oldExec := startedAt, signalSelf, exitProcess, execSelf
	startedAt = time.Now().Add(-uptime)
	signalSelf = func() error { f.signals++; return nil }
	// The backstop timer outlives the test; it must never end the test binary.
	exitProcess = func(code int) { f.exits = append(f.exits, code) }
	execSelf = func() error { f.execs++; return f.execErr }
	restartRequested.Store(false)
	t.Cleanup(func() {
		startedAt, signalSelf, exitProcess, execSelf = oldStarted, oldSignal, oldExit, oldExec
		restartRequested.Store(false)
	})
	return f
}

func TestRequestRestartSignalsOnce(t *testing.T) {
	f := withFakes(t, 2*RestartGrace)

	if !RequestRestart("test") {
		t.Fatal("expected first request to be accepted")
	}
	if RequestRestart("test") {
		t.Fatal("expected duplicate request to be ignored")
	}
	if f.signals != 1 {
		t.Fatalf("signal calls = %d, want 1", f.signals)
	}
	if !RestartRequested() {
		t.Fatal("RestartRequested should report true")
	}
	if f.execs != 0 || len(f.exits) != 0 {
		t.Fatal("RequestRestart must only start the graceful shutdown, not restart yet")
	}
}

func TestRequestRestartIgnoredDuringGrace(t *testing.T) {
	f := withFakes(t, time.Second)

	if RequestRestart("test") {
		t.Fatal("expected request during grace period to be ignored")
	}
	if f.signals != 0 || RestartRequested() {
		t.Fatal("grace-period request must not start a shutdown")
	}
}

func TestRestartReexecsFirst(t *testing.T) {
	f := withFakes(t, 2*RestartGrace)
	// A successful exec never returns; the fake does, so Restart then falls
	// through to the exit. What matters is that exec is tried first.
	Restart()
	if f.execs != 1 {
		t.Fatalf("exec attempts = %d, want 1", f.execs)
	}
}

func TestRestartFallsBackToExitWhenExecFails(t *testing.T) {
	f := withFakes(t, 2*RestartGrace)
	f.execErr = errors.New("exec failed")
	Restart()
	if f.execs != 1 || len(f.exits) != 1 || f.exits[0] != RestartExitCode {
		t.Fatalf("execs=%d exits=%v, want exec then exit(%d)", f.execs, f.exits, RestartExitCode)
	}
}

func TestRequestRestartRestartsWhenSignalFails(t *testing.T) {
	f := withFakes(t, 2*RestartGrace)
	signalSelf = func() error { return errors.New("signal failed") }
	RequestRestart("test")
	if !RestartRequested() || f.execs != 1 {
		t.Fatalf("failed signal must restart directly: requested=%v execs=%d", RestartRequested(), f.execs)
	}
}
