package xray

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xgeodata "github.com/xtls/xray-core/common/geodata"
	xrayCore "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
	xproxy "golang.org/x/net/proxy"
	"google.golang.org/protobuf/proto"
)

func writeGeoSite(t *testing.T, path, code string, domains ...string) {
	t.Helper()
	site := &xgeodata.GeoSite{Code: code}
	for _, d := range domains {
		site.Domain = append(site.Domain, &xgeodata.Domain{Type: xgeodata.Domain_Domain, Value: d})
	}
	b, err := proto.Marshal(&xgeodata.GeoSiteList{Entry: []*xgeodata.GeoSite{site}})
	if err != nil {
		t.Fatal(err)
	}
	// Replace the file the way the updater does: write elsewhere, rename over.
	tmp := path + ".new"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// Note: xray caches parsed geo data process-wide by "file:code", so each test
// uses its own code.

// The routing matchers xray builds (app/router uses DomainReg) are rebuilt in
// place by reloadGeoData from the files on disk: a matcher built before the
// update answers with the new list afterwards, with no restart.
func TestGeoReloadUpdatesLiveMatchers(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XRAY_LOCATION_ASSET", dir)
	path := filepath.Join(dir, "geosite.dat")
	writeGeoSite(t, path, "XBNTEST", "old.example")

	rules, err := xgeodata.ParseDomainRules([]string{"geosite:xbntest"}, xgeodata.Domain_Substr)
	if err != nil {
		t.Fatal(err)
	}
	m, err := xgeodata.DomainReg.BuildDomainMatcher(rules)
	if err != nil {
		t.Fatal(err)
	}
	if !m.MatchAny("www.old.example") || m.MatchAny("www.new.example") {
		t.Fatal("initial matcher wrong")
	}

	writeGeoSite(t, path, "XBNTEST", "new.example")
	if err := reloadGeoData(); err != nil {
		t.Fatal(err)
	}
	if m.MatchAny("www.old.example") || !m.MatchAny("www.new.example") {
		t.Fatal("live matcher not updated by reload")
	}
}

// End to end through a real xray instance: a routing rule blocks
// geosite:xbntest. After the file is replaced and reloadGeoData runs, the
// running instance routes by the new list — no restart.
func TestGeoReloadAppliesToRunningXrayRouting(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XRAY_LOCATION_ASSET", dir)
	path := filepath.Join(dir, "geosite.dat")
	writeGeoSite(t, path, "XBNROUTE", "old.example")

	// Every allowed request is redirected here by freedom.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()
	backendAddr := strings.TrimPrefix(backend.URL, "http://")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	socksPort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cfgJSON := fmt.Sprintf(`{
	  "log": {"loglevel": "none"},
	  "inbounds": [{"listen": "127.0.0.1", "port": %d, "protocol": "socks", "settings": {"udp": false}}],
	  "outbounds": [
	    {"tag": "direct", "protocol": "freedom", "settings": {"redirect": %q}},
	    {"tag": "block", "protocol": "blackhole"}
	  ],
	  "routing": {"rules": [{"type": "field", "domain": ["geosite:xbnroute"], "outboundTag": "block"}]}
	}`, socksPort, backendAddr)
	pb, err := serial.LoadJSONConfig(strings.NewReader(cfgJSON))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := xrayCore.New(pb)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	defer inst.Close()

	dialer, err := xproxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort), nil, xproxy.Direct)
	if err != nil {
		t.Fatal(err)
	}
	get := func(host string) int {
		client := &http.Client{
			Timeout:   3 * time.Second,
			Transport: &http.Transport{Dial: dialer.Dial, DisableKeepAlives: true},
		}
		resp, err := client.Get("http://" + host + "/")
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if o, n := get("www.old.example"), get("www.new.example"); o != 0 || n != http.StatusNoContent {
		t.Fatalf("before reload: old.example=%d new.example=%d, want blocked(0) and 204", o, n)
	}

	writeGeoSite(t, path, "XBNROUTE", "new.example")
	if err := reloadGeoData(); err != nil {
		t.Fatal(err)
	}
	if o, n := get("www.old.example"), get("www.new.example"); o != http.StatusNoContent || n != 0 {
		t.Fatalf("after reload: old.example=%d new.example=%d, want 204 and blocked(0)", o, n)
	}
}
