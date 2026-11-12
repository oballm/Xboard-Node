package singbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/auth"
	singJSON "github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"
	"golang.org/x/time/rate"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/nlog"
)

// SingBox implements kernel.Kernel by embedding sing-box as a Go library.
//
// User operations (AddUsers/RemoveUsers/UpdateUsers) use sing-box's native
// UpdatableInbound interface, which hot-swaps user credentials without
// restarting listeners — zero connection disruption. Every other change
// (inbound settings, certificate, outbounds, routes, DNS) restarts the box:
// sing-box 1.14 has no runtime inbound/outbound replace, and components cache
// the outbound and DNS objects they were built with, so a restart is the only
// change that is applied cleanly everywhere.
type SingBox struct {
	cfg config.KernelConfig

	mu     sync.RWMutex
	box    *box.Box
	ctx    context.Context
	cancel context.CancelFunc

	users      []model.UserSpec
	nodeConfig *model.NodeSpec
	tls        kernel.TLSCert

	// connTracker is our lightweight in-process byte/IP tracker.
	// Created fresh on every Start (full restart).
	// Survives Reload (hot-swap) since live connections persist.
	connTracker *ConnTracker

	// speedLimitFunc resolves a user UUID to a *rate.Limiter.
	// Set once by SetSpeedLimitFunc and forwarded to every new ConnTracker.
	speedLimitFunc func(string) *rate.Limiter

	// deviceLimitFunc resolves a user UUID to (limit, hasLimit) for gate-keeping.
	// Set once by SetDeviceLimitFunc and forwarded to every new ConnTracker.
	deviceLimitFunc func(string) (int, bool)

	// trackerRegistered prevents duplicate AppendTracker calls on the same
	// Router instance during Reload. Reset to false on full restart.
	trackerRegistered bool
}

func New(cfg config.KernelConfig) *SingBox {
	return &SingBox{cfg: cfg}
}

var _ kernel.Kernel = (*SingBox)(nil)

func (s *SingBox) Name() string { return "sing-box" }

func (s *SingBox) Capabilities() kernel.Capabilities {
	return kernel.Capabilities{
		PerUserSpeedLimit:    true,
		DeviceLimit:          true,
		BuiltInTrafficStats:  false,
		AliveIPTracking:      true,
		ForceCloseConnection: false,
		ForceCloseUser:       true,
	}
}

func (s *SingBox) Protocols() []string {
	return []string{
		"vmess", "vless", "trojan", "shadowsocks",
		"hysteria", "hysteria2", "tuic", "naive", "socks", "http", "anytls", "mieru",
	}
}

func (s *SingBox) Start(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startLocked(nodeConfig, users, tls)
}

// startLocked builds and starts a new box. A running box is stopped before
// the new one starts: the new one binds the same ports and cache file, so it
// cannot start while the old one still holds them. Must be called with s.mu
// held.
func (s *SingBox) startLocked(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	cfgMap := buildConfig(s.cfg, nodeConfig, users, tls)
	data, err := json.Marshal(cfgMap)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	nlog.Core().Debug("sing-box config generated", "len", len(data))

	ctx, cancel := context.WithCancel(context.Background())
	ctx = include.Context(ctx)

	opts, err := singJSON.UnmarshalExtendedContext[option.Options](ctx, data)
	if err != nil {
		cancel()
		return fmt.Errorf("parse sing-box options: %w", err)
	}

	// box.New only builds objects (no listeners, no cache file), so a config
	// it rejects is reported while the running box keeps serving.
	instance, err := box.New(box.Options{
		Context: ctx,
		Options: opts,
	})
	if err != nil {
		cancel()
		return fmt.Errorf("create sing-box instance: %w", err)
	}

	// Some errors only show at start (e.g. a selector member that does not
	// exist). The running box is already gone by then, so bring the last
	// config back up rather than leave the node down.
	running := s.box != nil
	prevConfig, prevUsers, prevTLS := s.nodeConfig, s.users, s.tls

	s.stop()

	if err := instance.Start(); err != nil {
		instance.Close()
		cancel()
		if running && prevConfig != nil {
			if rbErr := s.startLocked(prevConfig, prevUsers, prevTLS); rbErr != nil {
				nlog.Core().Error("sing-box: new config failed and the previous one did not come back", "error", err, "rollback_error", rbErr)
			} else {
				nlog.Core().Warn("sing-box: new config failed to start, previous config restored", "error", err)
			}
		}
		return fmt.Errorf("start sing-box: %w", err)
	}

	s.box = instance
	s.ctx = ctx
	s.cancel = cancel
	s.users = users
	s.nodeConfig = nodeConfig
	s.tls = tls

	// Fresh tracker on full restart.
	s.connTracker = NewConnTracker(0)
	s.connTracker.SetUserMap(buildUserMap(users))
	if s.speedLimitFunc != nil {
		s.connTracker.SetSpeedLimitFunc(s.speedLimitFunc)
	}
	if s.deviceLimitFunc != nil {
		s.connTracker.SetDeviceLimitFunc(s.deviceLimitFunc)
	}

	s.trackerRegistered = false
	s.registerTracker(ctx)

	nlog.Core().Debug("sing-box started", "users", len(users))
	return nil
}

// Reload applies a new node config. When only the user list differs, users
// are hot-swapped and connections are untouched; any other difference
// restarts the box.
func (s *SingBox) Reload(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.box == nil {
		return fmt.Errorf("not running")
	}

	same, err := s.sameExceptUsersLocked(nodeConfig, tls)
	if err != nil {
		return err
	}
	if !same {
		nlog.Core().Info("sing-box config changed, restarting kernel")
		return s.startLocked(nodeConfig, users, tls)
	}

	if err := s.reloadInboundsLocked(users); err != nil {
		return err
	}
	s.users = users
	s.nodeConfig = nodeConfig
	s.tls = tls
	nlog.Core().Debug("sing-box reloaded", "users", len(users))
	return nil
}

// sameExceptUsersLocked reports whether nodeConfig and tls produce the same
// sing-box config as the running one, ignoring users: both sides are built
// with the current user list and compared as canonical sing-box JSON.
func (s *SingBox) sameExceptUsersLocked(nodeConfig *model.NodeSpec, tls kernel.TLSCert) (bool, error) {
	if s.nodeConfig == nil {
		return false, nil
	}
	if !bytes.Equal(s.tls.CertPEM, tls.CertPEM) || !bytes.Equal(s.tls.KeyPEM, tls.KeyPEM) {
		return false, nil
	}
	cur, err := s.canonicalConfig(s.nodeConfig, s.tls)
	if err != nil {
		return false, err
	}
	next, err := s.canonicalConfig(nodeConfig, tls)
	if err != nil {
		return false, err
	}
	return bytes.Equal(cur, next), nil
}

func (s *SingBox) canonicalConfig(nodeConfig *model.NodeSpec, tls kernel.TLSCert) ([]byte, error) {
	data, err := json.Marshal(buildConfig(s.cfg, nodeConfig, s.users, tls))
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	opts, err := singJSON.UnmarshalExtendedContext[option.Options](s.ctx, data)
	if err != nil {
		return nil, fmt.Errorf("parse options: %w", err)
	}
	return singJSON.MarshalContext(s.ctx, opts)
}

// registerTracker wires the ConnTracker to the current Router exactly once.
// The ConnTracker handles both byte counting and optional rate limiting
// in a single wrapper, so no additional trackers are needed.
func (s *SingBox) registerTracker(ctx context.Context) {
	if s.trackerRegistered {
		return
	}
	router := service.FromContext[adapter.Router](ctx)
	if router == nil {
		return
	}
	if s.connTracker != nil {
		router.AppendTracker(s.connTracker)
	}
	s.trackerRegistered = true
}

func (s *SingBox) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stop()
}

func (s *SingBox) stop() {
	if s.box == nil {
		return
	}
	s.box.Close()
	s.box = nil
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.ctx = nil
}

func (s *SingBox) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.box != nil
}

// connTrackerLocked returns the connTracker under a read-optimised lock.
func (s *SingBox) connTrackerSafe() *ConnTracker {
	s.mu.Lock()
	ct := s.connTracker
	s.mu.Unlock()
	return ct
}

// SetSpeedLimitFunc configures per-user bandwidth throttling.
func (s *SingBox) SetSpeedLimitFunc(fn func(uuid string) *rate.Limiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.speedLimitFunc = fn
	if s.connTracker != nil {
		s.connTracker.SetSpeedLimitFunc(fn)
	}
}

// SetDeviceLimitFunc configures per-user device limit gate-keeping.
// Connections exceeding the limit are rejected at connect time.
func (s *SingBox) SetDeviceLimitFunc(fn func(uuid string) (int, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deviceLimitFunc = fn
	if s.connTracker != nil {
		s.connTracker.SetDeviceLimitFunc(fn)
	}
}

// UpdateGlobalDevices updates the global device state from panel (for multi-node).
func (s *SingBox) UpdateGlobalDevices(users map[int][]string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.connTracker != nil {
		s.connTracker.UpdateGlobalDevices(users)
	}
}

// ClearGlobalDevices clears the global device state (on WS disconnect).
func (s *SingBox) ClearGlobalDevices() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.connTracker != nil {
		s.connTracker.ClearGlobalDevices()
	}
}

// ─── User management (non-disruptive) ───────────────────────────────────────

// AddUsers hot-swaps users into running inbounds. Zero connection disruption.
func (s *SingBox) AddUsers(users []model.UserSpec) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.box == nil {
		return 0, fmt.Errorf("not running")
	}

	existing := make(map[int]struct{}, len(s.users))
	for _, u := range s.users {
		existing[u.ID] = struct{}{}
	}
	var toAdd []model.UserSpec
	for _, u := range users {
		if _, dup := existing[u.ID]; !dup {
			toAdd = append(toAdd, u)
		}
	}
	if len(toAdd) == 0 {
		return 0, nil
	}

	merged := append(append([]model.UserSpec{}, s.users...), toAdd...)
	if err := s.reloadInboundsLocked(merged); err != nil {
		return 0, err
	}
	s.users = merged
	return len(toAdd), nil
}

// RemoveUsers hot-swaps users out of running inbounds. Zero connection disruption
// for remaining users.
func (s *SingBox) RemoveUsers(users []model.UserSpec) (int, error) {
	var (
		ct      *ConnTracker
		toClose []model.UserSpec
	)
	// Runs after the deferred Unlock below (defers are LIFO).
	defer func() { closeRemovedUsers(ct, toClose) }()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.box == nil {
		return 0, fmt.Errorf("not running")
	}

	removeSet := make(map[int]struct{}, len(users))
	for _, u := range users {
		removeSet[u.ID] = struct{}{}
	}
	var kept, gone []model.UserSpec
	for _, u := range s.users {
		if _, rm := removeSet[u.ID]; rm {
			gone = append(gone, u)
		} else {
			kept = append(kept, u)
		}
	}
	if len(gone) == 0 {
		return 0, nil
	}

	if err := s.reloadInboundsLocked(kept); err != nil {
		return 0, err
	}
	s.users = kept
	ct, toClose = s.connTracker, gone
	return len(gone), nil
}

// UpdateUsers replaces the entire user set atomically via hot-swap.
func (s *SingBox) UpdateUsers(users []model.UserSpec) (added, removed int, err error) {
	var (
		ct      *ConnTracker
		toClose []model.UserSpec
	)
	// Runs after the deferred Unlock below (defers are LIFO).
	defer func() { closeRemovedUsers(ct, toClose) }()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.box == nil {
		return 0, 0, fmt.Errorf("not running")
	}

	toAdd, toRemove := kernel.UserDiff(s.users, users)
	added, removed = len(toAdd), len(toRemove)

	if added == 0 && removed == 0 {
		// Only limits may have changed — update tracker map.
		if s.connTracker != nil {
			s.connTracker.SetUserMap(buildUserMap(users))
		}
		s.users = users
		return 0, 0, nil
	}

	if err = s.reloadInboundsLocked(users); err != nil {
		return 0, 0, err
	}
	s.users = users
	ct, toClose = s.connTracker, toRemove
	return
}

// reloadInboundsLocked hot-swaps inbound users using UpdatableInbound.
// Must be called with s.mu held.
func (s *SingBox) reloadInboundsLocked(users []model.UserSpec) error {
	cfgMap := buildConfig(s.cfg, s.nodeConfig, users, s.tls)
	data, err := json.Marshal(cfgMap)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	opts, err := singJSON.UnmarshalExtendedContext[option.Options](s.ctx, data)
	if err != nil {
		return fmt.Errorf("parse options: %w", err)
	}

	im := service.FromContext[adapter.InboundManager](s.ctx)
	if im == nil {
		return fmt.Errorf("inbound manager not available")
	}

	// While inbounds are swapped one by one, a user being added may already
	// authenticate on an updated inbound. The tracker rejects users it does not
	// know, so widen its map to old ∪ new first; it is narrowed to exactly the
	// new set once every inbound is updated (below).
	if s.connTracker != nil {
		s.connTracker.SetUserMap(buildUserMap(append(append([]model.UserSpec(nil), s.users...), users...)))
	}

	for _, inb := range opts.Inbounds {
		tag := inb.Tag
		existing, ok := im.Get(tag)
		if !ok || existing.Type() != inb.Type {
			return s.restartForUsersLocked(users, fmt.Errorf("inbound %s missing or changed type", tag))
		}
		var (
			err     error
			updated bool
		)
		switch v := existing.(type) {
		case adapter.UpdatableInbound[option.VMessUser]:
			if opts, ok := inb.Options.(*option.VMessInboundOptions); ok {
				err, updated = v.UpdateUsers(opts.Users), true
			}
		case adapter.UpdatableInbound[option.VLESSUser]:
			if opts, ok := inb.Options.(*option.VLESSInboundOptions); ok {
				err, updated = v.UpdateUsers(opts.Users), true
			}
		case adapter.UpdatableInbound[option.TrojanUser]:
			if opts, ok := inb.Options.(*option.TrojanInboundOptions); ok {
				err, updated = v.UpdateUsers(opts.Users), true
			}
		case adapter.UpdatableInbound[option.Hysteria2User]:
			if opts, ok := inb.Options.(*option.Hysteria2InboundOptions); ok {
				err, updated = v.UpdateUsers(opts.Users), true
			}
		case adapter.UpdatableShadowsocksInbound:
			if opts, ok := inb.Options.(*option.ShadowsocksInboundOptions); ok {
				err, updated = v.UpdateUsersByOptions(opts.Users), true
			}
		case adapter.UpdatableInbound[option.TUICUser]:
			if opts, ok := inb.Options.(*option.TUICInboundOptions); ok {
				err, updated = v.UpdateUsers(opts.Users), true
			}
		case adapter.UpdatableInbound[option.AnyTLSUser]:
			if opts, ok := inb.Options.(*option.AnyTLSInboundOptions); ok {
				err, updated = v.UpdateUsers(opts.Users), true
			}
		case adapter.UpdatableInbound[option.MieruUser]:
			if opts, ok := inb.Options.(*option.MieruInboundOptions); ok {
				err, updated = v.UpdateUsers(opts.Users), true
			}
		case adapter.UpdatableInbound[auth.User]:
			switch opts := inb.Options.(type) {
			case *option.NaiveInboundOptions:
				err, updated = v.UpdateUsers(opts.Users), true
			case *option.SocksInboundOptions:
				err, updated = v.UpdateUsers(opts.Users), true
			case *option.HTTPMixedInboundOptions:
				err, updated = v.UpdateUsers(opts.Users), true
			}
		}
		if !updated {
			err = fmt.Errorf("inbound %s (%s) has no user hot update", tag, inb.Type)
		}
		if err != nil {
			return s.restartForUsersLocked(users, err)
		}
	}

	if s.connTracker != nil {
		s.connTracker.SetUserMap(buildUserMap(users))
	}

	nlog.Core().Debug("sing-box users hot-swapped", "users", len(users))
	return nil
}

// restartForUsersLocked applies users by restarting the box, for an inbound
// that cannot take them in place (no user hot update for its protocol, or
// the update failed). Must be called with s.mu held.
func (s *SingBox) restartForUsersLocked(users []model.UserSpec, cause error) error {
	nlog.Core().Warn("user hot update not possible, restarting kernel", "error", cause)
	return s.startLocked(s.nodeConfig, users, s.tls)
}

// ─── Observability ──────────────────────────────────────────────────────────
func (s *SingBox) GetUserTraffic(_ context.Context) (traffic map[int][2]int64, aliveIPs map[int]map[string]bool, connCount int, err error) {
	ct := s.connTrackerSafe()
	if ct == nil {
		return nil, nil, 0, nil
	}
	traffic, aliveIPs, connCount = ct.GetUserTraffic()
	return traffic, aliveIPs, connCount, nil
}

// CloseConnection force-closes a specific connection by its ID.
func (s *SingBox) CloseConnection(_ context.Context, connID string) error {
	ct := s.connTrackerSafe()
	if ct == nil {
		return fmt.Errorf("not running")
	}
	if !ct.CloseByID(connID) {
		nlog.Core().Debug("CloseConnection: connection not found (already closed?)", "id", connID)
	}
	return nil
}

// CloseUserConnections terminates all connections for a user UUID.
func (s *SingBox) CloseUserConnections(_ context.Context, uuid string) error {
	ct := s.connTrackerSafe()
	if ct == nil {
		return nil
	}
	ct.CloseByUUID(uuid)
	return nil
}

// buildUserMap creates a UUID→userID mapping used by ConnTracker to attribute
// connections to the correct user. All sing-box protocols use the user's UUID
// as the inbound name/username, so this covers every protocol.
func buildUserMap(users []model.UserSpec) map[string]int {
	m := make(map[string]int, 2*len(users))
	for _, u := range users {
		m[u.UUID] = u.ID
		// Naive authenticates with username = decimal user ID (buildNaive), so
		// its connections carry that as metadata.User. Without this key the
		// tracker would reject every naive connection as an unknown user.
		// UUIDs are never purely numeric, so the keys cannot collide.
		m[strconv.Itoa(u.ID)] = u.ID
	}
	return m
}

// closeRemovedUsers ends every open connection of the given users. Call it
// after the inbound no longer accepts them and WITHOUT s.mu held: closing a
// tracked conn re-enters the tracker, never this struct, but keeping kernel
// locks out of socket teardown is the cheap safe choice.
func closeRemovedUsers(ct *ConnTracker, removed []model.UserSpec) {
	if ct == nil || len(removed) == 0 {
		return
	}
	total := 0
	for _, u := range removed {
		total += ct.CloseByUUID(u.UUID)
	}
	if total > 0 {
		nlog.Core().Info("sing-box: closed connections of removed users", "users", len(removed), "connections", total)
	}
}
