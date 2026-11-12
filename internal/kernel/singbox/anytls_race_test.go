package singbox

import "testing"

// skipAnyTLSUnderRace skips an anytls end-to-end case when the race
// detector is on. sing-anytls v0.0.11 (the version sing-box 1.14.4 pins)
// races in Service.UpdateUsers/NewConnection (the user map is swapped
// without synchronisation) and in Stream close/write; the detector reports
// them whenever the timing lines up, failing the test for code that is not
// ours. make test runs these cases again without -race, so they keep their
// coverage. Remove once sing-box moves to sagernet/sing-anytls, which
// publishes the user map atomically.
func skipAnyTLSUnderRace(t *testing.T, proto string) {
	t.Helper()
	if raceEnabled && proto == "anytls" {
		t.Skip("sing-anytls v0.0.11 races (not ours); covered by the run without -race")
	}
}
