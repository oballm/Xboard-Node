package cert

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
)

func TestMarkRenewedNotifiesOnceAndCoalesces(t *testing.T) {
	m := NewManager(config.CertConfig{})

	m.markRenewed()
	m.markRenewed() // must not block with the buffer already full

	select {
	case <-m.RenewalNotify():
	default:
		t.Fatal("expected a renewal notification")
	}
	select {
	case <-m.RenewalNotify():
		t.Fatal("renewals must coalesce into one notification")
	default:
	}
	if !m.CertRenewed() {
		t.Fatal("CertRenewed should report the pending renewal")
	}
	if m.CertRenewed() {
		t.Fatal("CertRenewed should clear the flag")
	}
}
