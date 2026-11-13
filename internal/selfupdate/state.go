package selfupdate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const schemaVersion = 1

// Outcome values recorded in Result.
const (
	OutcomeConfirmed  = "confirmed"
	OutcomeRolledBack = "rolled_back"
	OutcomeFailed     = "failed"
)

// Release is an installed version.
type Release struct {
	Tag           string `json:"tag"`            // release tag, as the panel named it
	BinaryVersion string `json:"binary_version"` // what the binary prints for -v
	SHA256        string `json:"sha256"`
	File          string `json:"file"` // name under <dir>/versions
}

// Result is the outcome of the most recent upgrade attempt.
type Result struct {
	Tag     string    `json:"tag"`
	Outcome string    `json:"outcome"`
	Reason  string    `json:"reason,omitempty"`
	At      time.Time `json:"at"`
}

// State is persisted in <dir>/state.json.
type State struct {
	Schema         int    `json:"schema"`
	LauncherSHA256 string `json:"launcher_sha256"`
	// Current is the version the launcher runs; nil runs the launcher itself.
	Current *Release `json:"current,omitempty"`
	// Previous is the last known good version to fall back to; nil is the
	// launcher itself.
	Previous *Release `json:"previous,omitempty"`
	Trial    bool     `json:"trial"`
	// Starts counts launches of the trial version, not counting those after a
	// clean exit.
	Starts         int     `json:"starts"`
	CleanExit      bool    `json:"clean_exit,omitempty"`
	RollbackReason string  `json:"rollback_reason,omitempty"`
	Last           *Result `json:"last,omitempty"`
}

func statePath(dir string) string   { return filepath.Join(dir, "state.json") }
func versionsDir(dir string) string { return filepath.Join(dir, "versions") }

func releasePath(dir string, r *Release) string {
	return filepath.Join(versionsDir(dir), r.File)
}

func loadState(dir string) (*State, error) {
	b, err := os.ReadFile(statePath(dir))
	if err != nil {
		return nil, err
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("parse %s: %w", statePath(dir), err)
	}
	if st.Schema > schemaVersion {
		return nil, fmt.Errorf("%s has schema %d, newer than this binary understands (%d)", statePath(dir), st.Schema, schemaVersion)
	}
	return &st, nil
}

// saveState writes the state so that a crash or power loss leaves either the
// old or the new file, never a partial one.
func saveState(dir string, st *State) error {
	st.Schema = schemaVersion
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(statePath(dir), b, 0o644)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// withLock serialises state changes between the launcher, the running process
// and anything else sharing the directory.
func withLock(dir string, fn func() error) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock %s: %w", dir, err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

// updateState loads the state (nil when there is none), lets fn change it and
// saves it when fn returns save=true.
func updateState(dir string, fn func(st *State) (save bool, err error)) error {
	return withLock(dir, func() error {
		st, err := loadState(dir)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		save, err := fn(st)
		if err != nil || !save {
			return err
		}
		if st == nil {
			return nil
		}
		return saveState(dir, st)
	})
}

func fileSHA256(path string) (string, error) {
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

func verifyFile(path, wantSHA string) error {
	got, err := fileSHA256(path)
	if err != nil {
		return err
	}
	if got != wantSHA {
		return fmt.Errorf("SHA-256 is %s, expected %s", got, wantSHA)
	}
	return nil
}
