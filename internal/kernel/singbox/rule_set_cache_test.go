package singbox

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/common/srs"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/model"
)

func remoteRuleSetConfig(url string) map[string]any {
	return map[string]any{"route": map[string]any{
		"rule_set": []any{map[string]any{"tag": "rs", "type": "remote", "format": "binary", "url": url, "download_detour": "direct"}},
		"rules":    []any{map[string]any{"rule_set": "rs", "outbound": "direct"}},
	}}
}

func cacheFileOf(t *testing.T, cfg M) (M, bool) {
	t.Helper()
	exp, _ := cfg["experimental"].(M)
	cf, ok := exp["cache_file"].(M)
	return cf, ok
}

func TestRuleSetCacheEnabledForRemoteRuleSets(t *testing.T) {
	kcfg := config.KernelConfig{ConfigDir: "/etc/xboard-node/node-3"}
	users := []model.UserSpec{{ID: 1, UUID: e2eUUIDA}}
	build := func(custom map[string]any, routes []model.RouteRule) M {
		nc := &model.NodeSpec{Protocol: "vless", ServerPort: 1, CustomConfig: custom, Routes: routes}
		return buildConfig(kcfg, nc, users, selfSignedCert(t))
	}

	// Remote rule-set delivered in custom_config (how the panel sends them).
	cf, ok := cacheFileOf(t, build(remoteRuleSetConfig("https://example.com/rs.srs"), nil))
	if !ok || cf["enabled"] != true || cf["path"] != filepath.Join(kcfg.ConfigDir, "cache.db") {
		t.Fatalf("remote rule-set: cache_file = %v", cf)
	}

	// No rule-sets, or only a local one: no cache.
	if _, ok := cacheFileOf(t, build(nil, nil)); ok {
		t.Fatal("cache_file enabled without rule-sets")
	}
	local := map[string]any{"route": map[string]any{"rule_set": []any{map[string]any{"tag": "l", "type": "local", "format": "binary", "path": "/x.srs"}}}}
	if _, ok := cacheFileOf(t, build(local, nil)); ok {
		t.Fatal("cache_file enabled for a local rule-set")
	}

	// geoip:/geosite: route entries no longer enable it: sing-box dropped
	// them (1.12), they compile to literal domain matches.
	if _, ok := cacheFileOf(t, build(nil, []model.RouteRule{{Match: []string{"geosite:cn"}, Action: "block"}})); ok {
		t.Fatal("cache_file enabled for a geosite: route entry")
	}

	// A cache_file set in custom_config is left as it is.
	custom := remoteRuleSetConfig("https://example.com/rs.srs")
	custom["experimental"] = map[string]any{"cache_file": map[string]any{"enabled": false}}
	if cf, _ := cacheFileOf(t, build(custom, nil)); cf["enabled"] != false || cf["path"] != nil {
		t.Fatalf("custom cache_file overridden: %v", cf)
	}

	// Other experimental settings from custom_config are kept.
	custom = remoteRuleSetConfig("https://example.com/rs.srs")
	custom["experimental"] = map[string]any{"clash_api": map[string]any{"external_controller": "127.0.0.1:9090"}}
	cfg := build(custom, nil)
	if _, ok := cacheFileOf(t, cfg); !ok {
		t.Fatal("cache_file not added next to custom experimental settings")
	}
	if cfg["experimental"].(M)["clash_api"] == nil {
		t.Fatal("custom experimental settings dropped")
	}
}

// With the cache, a node restarts while its rule-set source is unreachable;
// without it, the start fails and the kernel stays down.
func TestE2ERestartWithRuleSetSourceDown(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	var data bytes.Buffer
	err := srs.Write(&data, option.PlainRuleSet{Rules: []option.HeadlessRule{{
		Type:           C.RuleTypeDefault,
		DefaultOptions: option.DefaultHeadlessRule{Domain: []string{"example.com"}},
	}}}, C.RuleSetVersionCurrent)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(data.Bytes()) }))
	url := srv.URL + "/rs.srs"

	p := e2eProtos[1]
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	port := freePort(t, "tcp")
	node := func(custom map[string]any) *model.NodeSpec {
		n := p.node(port)
		n.CustomConfig = custom
		return n
	}
	s := New(testKernelConfig(t))
	t.Cleanup(s.Stop)
	if err := s.Start(node(remoteRuleSetConfig(url)), []model.UserSpec{userA}, selfSignedCert(t)); err != nil {
		t.Fatalf("first start (downloads the rule-set): %v", err)
	}

	srv.Close()
	if err := s.Start(node(remoteRuleSetConfig(url)), []model.UserSpec{userA}, selfSignedCert(t)); err != nil {
		t.Fatalf("restart with the source down and the cache in place: %v", err)
	}

	noCache := remoteRuleSetConfig(url)
	noCache["experimental"] = map[string]any{"cache_file": map[string]any{"enabled": false}}
	if err := s.Start(node(noCache), []model.UserSpec{userA}, selfSignedCert(t)); err == nil {
		t.Fatal("start without a cache succeeded although the rule-set source is down")
	}
}
