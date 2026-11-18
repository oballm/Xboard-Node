// Package publicip finds the host's public IPv4 and IPv6 addresses by asking
// public "what is my IP" endpoints, for the panel to show which address a node
// serves on. It probes once at start and then every Interval; one prober per
// process, shared by every node it runs (machine and nodes mode).
//
// A failed probe changes nothing: the last good value is kept, and before the
// first success the value is empty and the report goes out without it.
package publicip

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

// Interval between probes. Each probe makes at most one successful request per
// address family; the endpoints serve far higher rates than this.
const Interval = 5 * time.Minute

const requestTimeout = 5 * time.Second

// Endpoints per family, tried in turn until one answers with a valid address.
// Each round starts one further along, spreading the requests across them.
// The Cloudflare trace endpoints answer "ip=<addr>" among other lines; the
// others answer the bare address.
var (
	endpointsV4 = []string{
		"https://1.1.1.1/cdn-cgi/trace",
		"https://api.ipify.org",
		"https://ipv4.icanhazip.com",
	}
	endpointsV6 = []string{
		"https://[2606:4700:4700::1111]/cdn-cgi/trace",
		"https://api6.ipify.org",
		"https://ipv6.icanhazip.com",
	}
)

var (
	startOnce sync.Once
	mu        sync.RWMutex
	curV4     string
	curV6     string
)

// Start launches the background prober once per process; later calls are
// no-ops. It runs for the life of the process.
func Start() {
	startOnce.Do(func() { go run() })
}

// Get returns the last probed addresses; empty when not known (yet), or when
// the host has no address of that family.
func Get() (v4, v6 string) {
	mu.RLock()
	defer mu.RUnlock()
	return curV4, curV6
}

func run() {
	defer func() {
		if r := recover(); r != nil {
			nlog.Core().Error("public ip prober stopped", "panic", r)
		}
	}()
	p := &prober{v4: newClient("tcp4"), v6: newClient("tcp6")}
	for round := 0; ; round++ {
		p.probe(round)
		time.Sleep(Interval)
	}
}

type prober struct {
	v4, v6 *http.Client
	// Whether the last probe of each family succeeded, to log transitions only:
	// a host without IPv6 fails that probe every round.
	okV4, okV6 bool
}

func (p *prober) probe(round int) {
	v4, err4 := lookup(p.v4, endpointsV4, round, false)
	v6, err6 := lookup(p.v6, endpointsV6, round, true)

	mu.Lock()
	oldV4, oldV6 := curV4, curV6
	if err4 == nil {
		curV4 = v4
	}
	if err6 == nil {
		curV6 = v6
	}
	mu.Unlock()

	log := nlog.Core()
	if err4 == nil && v4 != oldV4 {
		log.Info("public IPv4", "ip", v4, "was", oldV4)
	}
	if err6 == nil && v6 != oldV6 {
		log.Info("public IPv6", "ip", v6, "was", oldV6)
	}
	if err4 != nil && (p.okV4 || round == 0) {
		log.Info("public IPv4 probe failed, keeping last value", "error", err4, "kept", oldV4)
	}
	if err6 != nil && (p.okV6 || round == 0) {
		log.Debug("public IPv6 probe failed (normal without IPv6)", "error", err6)
	}
	p.okV4, p.okV6 = err4 == nil, err6 == nil
}

// newClient pins the address family so a dual-stack host reports both
// addresses instead of whichever family the resolver prefers. It ignores proxy
// environment variables: the host's own address is wanted.
func newClient(network string) *http.Client {
	d := &net.Dialer{Timeout: requestTimeout}
	return &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				return d.DialContext(ctx, network, addr)
			},
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout: requestTimeout,
			DisableKeepAlives:   true,
		},
	}
}

func lookup(c *http.Client, endpoints []string, round int, v6 bool) (string, error) {
	var lastErr error
	for i := range endpoints {
		url := endpoints[(round+i)%len(endpoints)]
		ip, err := fetch(c, url, v6)
		if err == nil {
			return ip, nil
		}
		lastErr = fmt.Errorf("%s: %w", url, err)
	}
	return "", lastErr
}

func fetch(c *http.Client, url string, v6 bool) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	return parse(string(body), v6)
}

// parse accepts a bare address or a Cloudflare trace body ("ip=<addr>" line)
// and returns the address only if it is a public one of the wanted family.
func parse(body string, v6 bool) (string, error) {
	s := strings.TrimSpace(body)
	for _, line := range strings.Split(s, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "ip="); ok {
			s = v
			break
		}
	}
	ip := net.ParseIP(strings.TrimSpace(s))
	if ip == nil {
		return "", fmt.Errorf("not an address: %.40q", s)
	}
	if is4 := ip.To4() != nil; is4 == v6 {
		return "", fmt.Errorf("wrong family: %s", ip)
	}
	if !IsPublic(ip) {
		return "", fmt.Errorf("not public: %s", ip)
	}
	return ip.String(), nil
}

var cgnat = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

// IsPublic reports whether ip is a globally routable unicast address: not
// private (RFC 1918, fc00::/7), carrier-grade NAT (100.64.0.0/10), loopback,
// link-local, multicast or unspecified.
func IsPublic(ip net.IP) bool {
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !cgnat.Contains(ip)
}
