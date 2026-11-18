package publicip

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestParse(t *testing.T) {
	cases := []struct {
		body string
		v6   bool
		want string // "" = error
	}{
		{"203.0.113.5\n", false, "203.0.113.5"},
		{"fl=1\nh=1.1.1.1\nip=198.51.100.7\nts=1\n", false, "198.51.100.7"},
		{"ip=2001:db8::5\n", true, "2001:db8::5"},
		{"2001:db8::5", false, ""}, // wrong family
		{"203.0.113.5", true, ""},  // wrong family
		{"10.1.2.3", false, ""},    // private
		{"100.64.1.1", false, ""},  // CGNAT
		{"127.0.0.1", false, ""},   // loopback
		{"169.254.1.1", false, ""}, // link-local
		{"fd00::1", true, ""},      // ULA
		{"fe80::1", true, ""},      // link-local
		{"<html>blocked</html>", false, ""},
		{"", false, ""},
	}
	for _, c := range cases {
		got, err := parse(c.body, c.v6)
		if c.want == "" {
			if err == nil {
				t.Errorf("parse(%q, v6=%v) = %q, want error", c.body, c.v6, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("parse(%q, v6=%v) = %q, %v; want %q", c.body, c.v6, got, err, c.want)
		}
	}
}

func TestIsPublic(t *testing.T) {
	for s, want := range map[string]bool{
		"8.8.8.8": true, "172.239.70.253": true, "2606:4700::1": true,
		"172.16.0.1": false, "192.168.1.1": false, "100.127.255.255": false,
		"100.128.0.1": true, "0.0.0.0": false, "224.0.0.1": false, "::1": false,
	} {
		if got := IsPublic(net.ParseIP(s)); got != want {
			t.Errorf("IsPublic(%s) = %v, want %v", s, got, want)
		}
	}
}

// lookup fails over to the next endpoint and starts each round one further.
func TestLookupFailoverAndRotation(t *testing.T) {
	var hitsBad, hitsGood atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsBad.Add(1)
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsGood.Add(1)
		w.Write([]byte("203.0.113.9\n"))
	}))
	defer good.Close()

	c := newClient("tcp4")
	eps := []string{bad.URL, good.URL}

	ip, err := lookup(c, eps, 0, false)
	if err != nil || ip != "203.0.113.9" || hitsBad.Load() != 1 || hitsGood.Load() != 1 {
		t.Fatalf("round 0: ip=%q err=%v bad=%d good=%d", ip, err, hitsBad.Load(), hitsGood.Load())
	}
	// Round 1 starts at the good endpoint: the bad one is not asked.
	ip, err = lookup(c, eps, 1, false)
	if err != nil || ip != "203.0.113.9" || hitsBad.Load() != 1 || hitsGood.Load() != 2 {
		t.Fatalf("round 1: ip=%q err=%v bad=%d good=%d", ip, err, hitsBad.Load(), hitsGood.Load())
	}

	if _, err := lookup(c, []string{bad.URL}, 0, false); err == nil {
		t.Fatal("all endpoints failing must be an error")
	}
}

// A failed probe keeps the last good value; a success replaces it.
func TestProbeKeepsLastValue(t *testing.T) {
	var answer atomic.Value // read by the handler goroutine
	answer.Store("203.0.113.1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a := answer.Load().(string)
		if a == "" {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		w.Write([]byte(a))
	}))
	defer srv.Close()

	saved4, saved6 := endpointsV4, endpointsV6
	defer func() { endpointsV4, endpointsV6 = saved4, saved6 }()
	endpointsV4 = []string{srv.URL}
	endpointsV6 = []string{srv.URL} // answers IPv4: wrong family, always fails

	p := &prober{v4: newClient("tcp4"), v6: newClient("tcp4")}
	p.probe(0)
	if v4, v6 := Get(); v4 != "203.0.113.1" || v6 != "" {
		t.Fatalf("first probe: %q %q", v4, v6)
	}
	answer.Store("")
	p.probe(1)
	if v4, _ := Get(); v4 != "203.0.113.1" {
		t.Fatalf("failed probe dropped the value: %q", v4)
	}
	answer.Store("203.0.113.2")
	p.probe(2)
	if v4, _ := Get(); v4 != "203.0.113.2" {
		t.Fatalf("change not picked up: %q", v4)
	}
}
