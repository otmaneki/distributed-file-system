package p2p

import "errors"

// ErrInvalidHandshake is returned if the handshake between the local and remote nodes
// could not be established.
var ErrInvalidHandshake = errors.New("invalid handshake")

// HandShakeFunc is the function that will do the handshake between peer and server.
type HandShakeFunc func(Peer) error

// NoOpHandshake is a dummy handshake that does nothing, use it in case you don't want to do handshakes.
func NoOpHandshake(Peer) error {
	return nil
}
