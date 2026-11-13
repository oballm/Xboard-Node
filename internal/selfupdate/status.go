package selfupdate

import (
	"fmt"
	"os"
	"path/filepath"
)

// Status is reported to the panel in the report metrics under "upgrade".
// Panels that do not know the key ignore it.
type Status struct {
	Enabled bool `json:"enabled"`
	// DisabledReason says what to fix when Enabled is false.
	DisabledReason string `json:"disabled_reason,omitempty"`
	// AllowPrerelease: the node accepts rolling tags (next-build, dev).
	AllowPrerelease bool    `json:"allow_prerelease"`
	Running         string  `json:"running"` // installed tag, or "launcher"
	Trial           bool    `json:"trial"`
	Upgrading       bool    `json:"upgrading"`
	Last            *Result `json:"last,omitempty"`
}

// DescribeInstalled is a note for -v: when this binary is the launcher and a
// panel-installed version is selected, -v alone would print the launcher's
// version, not the one actually running. Read-only; "" when nothing applies.
func DescribeInstalled() string {
	dir := os.Getenv(EnvUpdateDir)
	if dir == "" {
		return ""
	}
	dir = filepath.Clean(dir)
	exe, err := os.Executable()
	if err != nil || isUnder(exe, versionsDir(dir)) {
		return ""
	}
	st, err := loadState(dir)
	if err != nil || st.Current == nil {
		return ""
	}
	note := fmt.Sprintf("panel-installed version %s (%s) is what runs; this binary is the launcher", st.Current.Tag, st.Current.BinaryVersion)
	if st.Trial {
		note += ", on trial"
	}
	return note
}

// CurrentStatus describes this process's upgrade state.
func CurrentStatus() Status {
	s := Status{Running: "launcher", Upgrading: busy.Load(), AllowPrerelease: allowPrerelease()}
	info := current()
	if why := setupProblem(info); why != "" {
		s.DisabledReason = why
		s.Last = lastFailure.Load()
		return s
	}
	s.Enabled = true
	st, err := loadState(info.dir)
	if err == nil {
		if info.child && st.Current != nil {
			s.Running = st.Current.Tag
		}
		s.Trial = st.Trial && isCandidate(info, st)
		s.Last = st.Last
	}
	if f := lastFailure.Load(); f != nil && (s.Last == nil || f.At.After(s.Last.At)) {
		s.Last = f
	}
	return s
}
