// Package selfupdate lets the panel upgrade xboard-node to a release and rolls
// the upgrade back when the new version is not healthy.
//
// # Launcher and versions
//
// The binary installed by the image or the installer is the launcher. It is
// never modified. Upgrades are kept in <dir>/versions, and at start the
// launcher execs the selected one. Every decision (which version to run,
// whether to roll back) is made by the launcher before any code of the new
// version runs, so a version that fails at any point, even before main, is
// rolled back. The launcher is the one part that cannot be upgraded remotely,
// so its job is kept small: read the state, verify the binary, pick, exec. It
// does not touch the network or the config.
//
// Docker and systemd use the same layout. <dir> comes from
// XBOARD_NODE_UPDATE_DIR, set by the Dockerfile and the systemd unit. Without
// it, upgrades are refused.
//
// This is the A/B update scheme of Android and ChromeOS: the launcher plays
// the bootloader, counting attempts and falling back, and the new version
// marks itself good once healthy.
//
// # State machine
//
// state.json is written atomically (temp file, fsync, rename, fsync dir) under
// an flock:
//
//	               Upgrade                      healthy
//	stable(A) ───────────────► trial(B) ───────────────► stable(B), last=confirmed
//	                              │
//	                              │ unclean exits > maxStarts, or not healthy
//	                              │ within TrialTimeout, or B fails verification
//	                              ▼
//	                        stable(A), last=rolled_back, B deleted
//
// Only unclean exits count as failed starts: a restart asked by the panel, a
// clean stop (docker stop, systemctl stop) and the trial's own rollback
// restart mark the exit as clean first.
//
// # Health
//
// A new version is healthy when it has been up for SoakPeriod and every node
// service in the process has reported to the panel at least once since it
// started. Nodes that cannot report (standalone mode) are not counted. If it
// is not healthy within TrialTimeout, it asks for a rollback and restarts.
//
// # Trust
//
// The panel only names a version. The binary always comes from DownloadBase,
// fixed at build time, and must match the release's SHA256SUMS. A compromised
// panel can therefore choose only among official releases. The launcher checks
// the SHA-256 again before each exec, so a binary damaged or changed on disk
// is not run.
//
// # The admin wins
//
// The state records the launcher's SHA-256. A different launcher means someone
// installed a binary by hand (new image, installer, xbctl upgrade). Then the
// installed versions are dropped and the new launcher runs.
package selfupdate
