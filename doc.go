// Package zerobus is a small D-Bus client that does not allocate in steady
// state. It speaks the D-Bus wire protocol over a Unix socket and decodes
// messages in place, so strings and arrays point into the receive buffer and
// stay valid only until the next read.
//
// # Clients and servers
//
// A Conn has one encoder and one receive buffer. A client may block in Call,
// which sends a method call and waits for its reply, and may set Handle to see
// the messages that arrive meanwhile.
//
// A server loop uses only Send and ReadMessage, never Call or Handle. It
// answers a method call with NewReply or NewError, emits NewSignal or the
// directed NewSignalTo, and sends its own requests with NewMethodCall, which
// returns the serial. The reply comes back later through ReadMessage as a
// message whose ReplySerial is that serial. Decode a message completely before
// the next Send is built and the next read: both reuse the same buffers.
//
// # Polling
//
// A loop that also waits for other descriptors gets the socket from
// SyscallConn and waits for it to be readable. ReadMessage reads all the bytes
// available, so call it while Buffered reports true before waiting again.
// After the descriptor turns readable, ReadMessage may still block for the
// rest of a message that has only partly arrived.
package zerobus
