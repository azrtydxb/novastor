//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/metadata"
)

// allocAddr allocates a random TCP port on localhost and returns the address
// string. The port is released immediately so it can be reused by the caller.
func allocAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to allocate address: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// waitForLeader polls the RaftStore until it becomes leader or the timeout
// expires. It fails the test if the leader is not elected in time.
func waitForLeader(t *testing.T, store *metadata.RaftStore, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Raft leader was not elected within %v", timeout)
}
