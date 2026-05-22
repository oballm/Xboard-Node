package singbox

import (
	"bytes"
	"context"

	"github.com/sagernet/sing-box/option"
	singJSON "github.com/sagernet/sing/common/json"
)

// dnsOptionsChanged returns true if next differs from prev. Comparison is done
// via the canonical sing-box JSON marshal so all sub-fields (servers, rules,
// final, strategy, fakeip, …) are compared together. Nil-safe: both nil → no
// change; one nil → change.
//
// Background — why we don't hot-reload DNS at the transport level:
//
// sing-box exposes DNSTransportManager.Create/Remove, which looked like it
// would let us hot-swap DNS transports the same way outboundReconcile does
// for outbounds. Empirical testing (2026-05-22) proved this unsafe:
//
//   - Manager.Create correctly closes the previous transport and installs
//     the new one; "updated default server to <tag>" log confirms.
//   - BUT dialers (common/dialer/dialer.go:73 and :102) cache the resolved
//     adapter.DNSTransport *pointer* into dnsQueryOptions at dialer
//     construction time, not at query time.
//   - Result: after Create swaps the tag → new transport, the new transport
//     is in the manager and Default() returns it, but every existing
//     outbound's dialer still holds the OLD pointer. Queries through those
//     dialers hit the closed transport → "file already closed" → DNS dies
//     entirely until full restart.
//
// Until sing-box upstream patches dialers to look up transports per-query
// (or until xboard-node also recreates every outbound on each DNS change,
// which defeats the "hot" goal), the safe path is: detect the DNS diff
// here and return an error from Reload. service.go applyChanges treats
// Reload errors as fallback signals and calls startKernel for a full
// sing-box restart, which rebuilds dialers from scratch — new DNS applied
// cleanly, brief inbound interruption is the price.
func dnsOptionsChanged(ctx context.Context, prev, next *option.DNSOptions) bool {
	if prev == nil && next == nil {
		return false
	}
	prevJSON, _ := singJSON.MarshalContext(ctx, prev)
	nextJSON, _ := singJSON.MarshalContext(ctx, next)
	return !bytes.Equal(prevJSON, nextJSON)
}
