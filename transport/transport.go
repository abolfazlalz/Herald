package transport

import "context"

type Transport interface {
	PublishBroadcast(ctx context.Context, data []byte) error
	PublishDirect(ctx context.Context, routingKey string, data []byte) error

	SubscribeBroadcast(ctx context.Context) (<-chan []byte, error)
	SubscribeDirect(ctx context.Context, peerID string) (<-chan []byte, error)

	Close() error
}

// ReconnectNotifier is implemented by transports that can recover their
// connection. Callbacks run after the transport is ready to publish and
// consume again.
type ReconnectNotifier interface {
	OnReconnect(func())
}
