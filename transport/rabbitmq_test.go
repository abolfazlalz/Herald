package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQNotifyReconnected(t *testing.T) {
	mq := &RabbitMQ{}
	called := 0

	mq.OnReconnect(func() { called++ })
	mq.OnReconnect(nil)
	mq.notifyReconnected()

	if called != 1 {
		t.Fatalf("reconnect callback called %d times, want 1", called)
	}
}

func TestRabbitMQSetDisconnectedIgnoresStaleConnection(t *testing.T) {
	current := &amqp.Connection{}
	stale := &amqp.Connection{}
	mq := &RabbitMQ{
		conn:        current,
		ready:       make(chan struct{}),
		readyClosed: true,
	}

	if mq.setDisconnected(stale) {
		t.Fatal("stale connection started a reconnect")
	}
	if mq.conn != current || !mq.readyClosed {
		t.Fatal("stale connection changed the current connection state")
	}
}

func TestWaitForRabbitMQReadyHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitForRabbitMQReady(ctx, make(chan struct{}), make(chan struct{}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForRabbitMQReady error = %v, want context.Canceled", err)
	}
}

func TestNextRabbitMQBackoff(t *testing.T) {
	if got := nextRabbitMQBackoff(500 * time.Millisecond); got != time.Second {
		t.Fatalf("next backoff = %s, want 1s", got)
	}
	if got := nextRabbitMQBackoff(20 * time.Second); got != rabbitMQMaxReconnectBackoff {
		t.Fatalf("capped backoff = %s, want %s", got, rabbitMQMaxReconnectBackoff)
	}
}
