// Package zerobus is a small D-Bus client that does not allocate in steady
// state. It speaks the D-Bus wire protocol over a Unix socket and decodes
// messages in place, so strings and arrays point into the receive buffer and
// stay valid only until the next read.
//
// DialContext, SessionBusContext and SystemBusContext bound socket connection,
// authentication and Hello with a context. Cancellation does not affect a
// successfully returned Conn. Dial, SessionBus and SystemBus use a background
// context and impose no setup timeout.
//
// # Concurrent clients
//
// Dispatcher owns a Conn and runs one reader goroutine for concurrent,
// context-cancellable Call, asynchronous Go, and bounded Subscribe streams.
// NewCall or NewSignal locks the encoder: follow it with Call, Go or Send on
// the same goroutine, with no intervening Dispatcher methods. Delivered Msgs
// own their storage and remain valid across reads. Close each Msg to release
// untaken descriptors; abandoning a Pending with a delivered reply can leak
// descriptors. Signals never carry descriptors. A warm successful Call costs
// four allocations, while unwanted messages are not detached.
//
// Match.Sender must be empty or a unique name starting with ':'. Resolve
// well-known names with org.freedesktop.DBus.GetNameOwner and pass the unique
// name; local sender filtering isolates overlapping subscriptions. Subscribe
// checks cancellation before AddMatch and, if cancellation races its reply,
// attempts RemoveMatch with a fresh one-second context before returning.
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
// # Unix file descriptors
//
// On Linux, UnixFDs reports negotiated descriptor support. Encoder.UnixFD
// borrows an outgoing descriptor until Send or Call returns. Message.TakeFD
// takes ownership of a received descriptor indexed by Reader.UnixFD; the caller
// must close it. Untaken descriptors are closed on the next read, Call or Close.
// Other platforms do not negotiate descriptor support.
//
// # Polling
//
// A loop that also waits for other descriptors gets the socket from
// SyscallConn and waits for it to be readable. ReadMessage reads all the bytes
// available, so call it while Buffered reports true before waiting again.
// After the descriptor turns readable, ReadMessage may still block for the
// rest of a message that has only partly arrived.
package zerobus
