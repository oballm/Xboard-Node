package xray

import (
	"encoding/json"
	"testing"

	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/panel"
	ss2022 "github.com/xtls/xray-core/proxy/shadowsocks_2022"
)

// A user added by hot reload must get the same SS2022 key as one written into
// the initial config; before, the hot-reload path passed the raw UUID, which the
// kernel could not decode, so added users could not connect until a restart.
func TestSS2022HotReloadUserKeyMatchesConfig(t *testing.T) {
	for method := range ss2022Methods {
		t.Run(method, func(t *testing.T) {
			nc := panel.NodeConfig{Protocol: "shadowsocks", ServerPort: 8388, Cipher: method, ServerKey: "server-key"}
			spec := testNodeSpec(&nc)
			data, _ := json.Marshal(buildConfig(testKernelCfg, spec, testUsers, kernel.TLSCert{}))
			var parsed struct {
				Inbounds []struct {
					Settings struct {
						Clients []struct{ Password string } `json:"clients"`
					} `json:"settings"`
				} `json:"inbounds"`
			}
			if err := json.Unmarshal(data, &parsed); err != nil {
				t.Fatal(err)
			}
			clients := parsed.Inbounds[0].Settings.Clients
			if len(clients) != len(testUsers) {
				t.Fatalf("clients = %d, want %d", len(clients), len(testUsers))
			}
			for i, u := range testUsers {
				fromConfig, err := (&ss2022.Account{Key: clients[i].Password}).AsAccount()
				if err != nil {
					t.Fatal(err)
				}
				mu, err := toMemoryUser("shadowsocks", spec, u)
				if err != nil {
					t.Fatalf("toMemoryUser: %v", err)
				}
				if !mu.Account.Equals(fromConfig) {
					t.Fatalf("user %d: hot-reload key differs from config key", u.ID)
				}
				if got, want := len(mu.Account.(*ss2022.MemoryAccount).Key), ss2022Methods[method].size; got != want {
					t.Fatalf("key length %d, want %d", got, want)
				}
			}
		})
	}
}

// Unencrypted Shadowsocks was removed from Xray v26: refuse it rather than
// silently switching the user to another cipher.
func TestShadowsocksNoneCipherRejected(t *testing.T) {
	nc := panel.NodeConfig{Protocol: "shadowsocks", ServerPort: 8388, Cipher: "none"}
	if _, err := toMemoryUser("shadowsocks", testNodeSpec(&nc), testUsers[0]); err == nil {
		t.Fatal("none cipher accepted")
	}
}
