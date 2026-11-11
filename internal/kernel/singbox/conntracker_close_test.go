package singbox

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

func TestCloseByUUIDClosesOnlyThatUser(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1, "uuid-2": 2})
	a1, a2, b := &testConn{}, &testConn{}, &testConn{}
	tracker.RoutedConnection(context.Background(), a1, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	tracker.RoutedConnection(context.Background(), a2, testInboundContext("uuid-1", "2.2.2.2"), nil, nil)
	tracker.RoutedConnection(context.Background(), b, testInboundContext("uuid-2", "3.3.3.3"), nil, nil)

	if n := tracker.CloseByUUID("uuid-1"); n != 2 {
		t.Fatalf("CloseByUUID closed %d, want 2", n)
	}
	if !a1.closed || !a2.closed {
		t.Fatal("target user's connections not closed")
	}
	if b.closed {
		t.Fatal("other user's connection closed")
	}
	// Closing the wrapper releases bookkeeping: the entries are gone.
	if n := tracker.CloseByUUID("uuid-1"); n != 0 {
		t.Fatalf("second CloseByUUID closed %d, want 0", n)
	}
	_, alive, conns := tracker.GetUserTraffic()
	if alive[1] != nil || conns != 1 {
		t.Fatalf("after close: alive[1]=%v conns=%d, want nil/1", alive[1], conns)
	}
	if tracker.CloseByUUID("") != 0 {
		t.Fatal("empty uuid must close nothing")
	}
}

func TestRoutedConnectionRejectsUnknownUser(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	c := &testConn{}
	got := tracker.RoutedConnection(context.Background(), c, testInboundContext("removed-uuid", "1.1.1.1"), nil, nil)
	if got != c || !c.closed {
		t.Fatal("unknown user's connection must be closed and returned unwrapped")
	}
	traffic, alive, conns := tracker.GetUserTraffic()
	if len(traffic) != 0 || len(alive) != 0 || conns != 0 {
		t.Fatalf("rejected connection left state: traffic=%v alive=%v conns=%d", traffic, alive, conns)
	}
}

func TestBuildUserMapKeepsNaiveNumericUsername(t *testing.T) {
	m := buildUserMap([]model.UserSpec{{ID: 8753, UUID: "11111111-1111-4111-8111-111111111111"}})
	if m["11111111-1111-4111-8111-111111111111"] != 8753 || m["8753"] != 8753 {
		t.Fatalf("user map = %v", m)
	}
}

// safeConn is a testConn whose Close may be called from several goroutines
// (a tracked conn can be closed by CloseByUUID and by its own copy loop).
type safeConn struct {
	testConn
	closedN atomic.Int32
}

func (c *safeConn) Close() error { c.closedN.Add(1); return nil }

// Concurrent connect / close-by-uuid / map swaps must not race or panic.
func TestCloseByUUIDConcurrent(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1, "uuid-2": 2})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				w := tracker.RoutedConnection(context.Background(), &safeConn{}, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
				if j%3 == 0 {
					w.Close()
				}
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				tracker.CloseByUUID("uuid-1")
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if j%2 == 0 {
					tracker.SetUserMap(map[string]int{"uuid-2": 2})
				} else {
					tracker.SetUserMap(map[string]int{"uuid-1": 1, "uuid-2": 2})
				}
			}
		}()
	}
	wg.Wait()
}
