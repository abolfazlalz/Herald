package herald

import (
	"context"
	"sync"
	"testing"
	"time"
)

type reconnectTransport struct {
	mu          sync.Mutex
	broadcasts  int
	onReconnect func()
	broadcastCh chan []byte
	directCh    chan []byte
}

func newReconnectTransport() *reconnectTransport {
	return &reconnectTransport{
		broadcastCh: make(chan []byte),
		directCh:    make(chan []byte),
	}
}

func (t *reconnectTransport) PublishBroadcast(context.Context, []byte) error {
	t.mu.Lock()
	t.broadcasts++
	t.mu.Unlock()
	return nil
}

func (t *reconnectTransport) PublishDirect(context.Context, string, []byte) error {
	return nil
}

func (t *reconnectTransport) SubscribeBroadcast(context.Context) (<-chan []byte, error) {
	return t.broadcastCh, nil
}

func (t *reconnectTransport) SubscribeDirect(context.Context, string) (<-chan []byte, error) {
	return t.directCh, nil
}

func (t *reconnectTransport) OnReconnect(callback func()) {
	t.mu.Lock()
	t.onReconnect = callback
	t.mu.Unlock()
}

func (t *reconnectTransport) Close() error { return nil }

func (t *reconnectTransport) reconnect() {
	t.mu.Lock()
	callback := t.onReconnect
	t.mu.Unlock()
	callback()
}

func (t *reconnectTransport) broadcastCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.broadcasts
}

func TestStartBroadcastsHandshakeAfterTransportReconnect(t *testing.T) {
	transport := newReconnectTransport()
	h, err := New(transport, &Option{Logger: NewDiscardLogs()})
	if err != nil {
		t.Fatalf("create Herald: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.Start(ctx)
	}()

	waitForBroadcastCount(t, transport, 1)
	transport.reconnect()
	waitForBroadcastCount(t, transport, 2)

	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Start returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not stop after context cancellation")
	}
}

func waitForBroadcastCount(t *testing.T, transport *reconnectTransport, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if transport.broadcastCount() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("broadcast count = %d, want at least %d", transport.broadcastCount(), want)
}
