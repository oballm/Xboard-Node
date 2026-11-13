package selfupdate

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

var (
	// SoakPeriod is how long a new version must stay up before it can be
	// confirmed: it catches versions that fail shortly after start.
	SoakPeriod = 60 * time.Second
	// TrialTimeout is how long a new version has to become healthy.
	TrialTimeout = 5 * time.Minute

	healthPoll = 2 * time.Second
)

// healthState tracks, per node service in this process, whether it has
// reported to the panel since start.
var health struct {
	sync.Mutex
	nodes     map[string]bool
	failed    map[string]string // node -> error, for nodes that exited with an error
	confirmed bool
}

func resetHealth() {
	health.Lock()
	health.nodes = map[string]bool{}
	health.failed = map[string]string{}
	health.confirmed = false
	health.Unlock()
}

// NodeStarted registers a node service that reports to the panel. Every
// registered node must report once before a new version is confirmed.
func NodeStarted(id string) {
	health.Lock()
	defer health.Unlock()
	if health.nodes == nil {
		health.nodes = map[string]bool{}
	}
	if _, ok := health.nodes[id]; !ok {
		health.nodes[id] = false
	}
}

// NodeReported records a successful report of a node service.
func NodeReported(id string) {
	health.Lock()
	defer health.Unlock()
	if _, ok := health.nodes[id]; ok {
		health.nodes[id] = true
	}
}

// NodeStopped unregisters a node service that stopped on purpose (shutdown,
// restart, unbound from its machine).
func NodeStopped(id string) {
	health.Lock()
	defer health.Unlock()
	delete(health.nodes, id)
}

// NodeFailed records a node service that exited with an error. A version on
// trial with a failed node is not healthy: in machine mode the other nodes
// keep running and nobody restarts the failed one, so without this the
// version would be confirmed with a node down.
func NodeFailed(id string, err error) {
	health.Lock()
	defer health.Unlock()
	delete(health.nodes, id)
	if health.failed == nil {
		health.failed = map[string]string{}
	}
	health.failed[id] = err.Error()
}

// failure returns a description of a failed node, or "".
func failure() string {
	health.Lock()
	defer health.Unlock()
	if len(health.failed) == 0 {
		return ""
	}
	var ids []string
	for id := range health.failed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return fmt.Sprintf("%s exited with an error: %s", ids[0], health.failed[ids[0]])
}

// healthy reports whether every registered node has reported, and if not,
// which have not.
func healthy() (bool, string) {
	health.Lock()
	defer health.Unlock()
	if len(health.nodes) == 0 {
		return false, "no node service is running"
	}
	var waiting []string
	for id, ok := range health.nodes {
		if !ok {
			waiting = append(waiting, id)
		}
	}
	if len(waiting) == 0 {
		return true, ""
	}
	sort.Strings(waiting)
	return false, "no successful report yet from " + strings.Join(waiting, ", ")
}

// Confirmed reports whether this process confirmed its upgrade.
func Confirmed() bool {
	health.Lock()
	defer health.Unlock()
	return health.confirmed
}

// InTrial reports whether this process is a new version not yet confirmed.
func InTrial() bool {
	info := current()
	if info == nil || !info.child {
		return false
	}
	st, err := loadState(info.dir)
	return err == nil && st.Trial && isCandidate(info, st)
}

// Watch starts the trial of a new version: it confirms the version once
// healthy, or asks for a rollback and calls restart. It does nothing outside
// a trial. restart must restart the process (through RestartTarget).
func Watch(restart func(reason string)) {
	if !InTrial() {
		return
	}
	nlog.Core().Info("selfupdate: new version on trial",
		"soak", SoakPeriod, "timeout", TrialTimeout, "healthy_when", "up for the soak period and every node has reported")
	go watch(now(), restart)
}

func watch(started time.Time, restart func(reason string)) {
	t := time.NewTicker(healthPoll)
	defer t.Stop()
	for range t.C {
		if f := failure(); f != "" {
			reason := "a node failed during the trial: " + f
			if err := requestRollback(reason); err != nil {
				nlog.Core().Error("selfupdate: cannot record rollback; restarting anyway", "error", err)
			}
			nlog.Core().Error("selfupdate: new version is not healthy, restarting to roll back", "reason", reason)
			restart(reason)
			return
		}
		up := now().Sub(started)
		ok, why := healthy()
		if ok && up >= SoakPeriod {
			confirm()
			return
		}
		if up >= TrialTimeout {
			reason := fmt.Sprintf("not healthy within %s: %s", TrialTimeout, why)
			if err := requestRollback(reason); err != nil {
				nlog.Core().Error("selfupdate: cannot record rollback; restarting anyway", "error", err)
			}
			nlog.Core().Error("selfupdate: new version is not healthy, restarting to roll back", "reason", reason)
			restart(reason)
			return
		}
	}
}

func confirm() {
	info := current()
	var tag string
	err := updateState(info.dir, func(st *State) (bool, error) {
		if st == nil || !st.Trial || !isCandidate(info, st) {
			return false, nil
		}
		tag = st.Current.Tag
		st.Trial, st.Starts, st.CleanExit = false, 0, false
		st.Last = &Result{Tag: tag, Outcome: OutcomeConfirmed, At: now().UTC()}
		pruneVersions(info.dir, st)
		return true, nil
	})
	if err != nil {
		nlog.Core().Error("selfupdate: cannot confirm the new version", "error", err)
		return
	}
	health.Lock()
	health.confirmed = true
	health.Unlock()
	if tag != "" {
		nlog.Core().Info("selfupdate: new version confirmed", "version", tag)
	}
}

func requestRollback(reason string) error {
	info := current()
	return updateState(info.dir, func(st *State) (bool, error) {
		if st == nil || !st.Trial || !isCandidate(info, st) {
			return false, nil
		}
		st.RollbackReason = reason
		return true, nil
	})
}
