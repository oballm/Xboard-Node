package singbox

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

// A mieru UDP download must arrive complete. The mieru server used to drop
// its unacknowledged segments when the application closed the session, and
// the client closed as soon as the close request arrived, so a bulk
// download lost its tail about half the time.
func TestE2EMieruUDPBulkComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
	const size = 50 << 20
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	chunk := make([]byte, 32*1024)
	go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(size))
		for sent := 0; sent < size; sent += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer l.Close()
	p := e2eSnellMieruProtos[1] // mieru-udp
	userA := model.UserSpec{ID: 1, UUID: e2eUUIDA}
	_, port := startServer(t, p, []model.UserSpec{userA})
	fails := 0
	for i := 0; i < 6; i++ {
		cp := startClient(t, p.outbound(port, e2eUUIDA))
		pu, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", cp))
		hc := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(pu), DisableKeepAlives: true}}
		resp, err := hc.Get("http://" + l.Addr().String() + "/")
		if err != nil {
			t.Logf("run %d: %v", i, err)
			fails++
			continue
		}
		n, err := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if n != size {
			fails++
		}
		if n != size {
			t.Logf("run %d: got %d / %d err=%v", i, n, size, err)
		}
	}
	if fails > 0 {
		t.Fatalf("%d/6 mieru UDP downloads incomplete", fails)
	}
}
