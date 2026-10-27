# zerobus

A small D-Bus client for Go with a zero-allocation `Conn` and a concurrent `Dispatcher`.

```sh
go get bnema.dev/zerobus   # Go 1.27 or newer
```

- **Zero allocations per message with Conn.** After the first few messages warm up its buffers, a call, a reply or a signal costs no heap allocation. Dispatcher adds concurrency with four allocations per warm successful call.
- **Zero-copy reads.** Strings and byte arrays point straight into the receive buffer.
- **Standard library only.**
- **Small.** It connects over a Unix socket, authenticates with `EXTERNAL`, calls methods, emits signals, answers calls, and adds match rules. Nothing else.

**Status: v0.x.** The API may change between minor versions.

## Call a method

```go
c, err := zerobus.SessionBus()
if err != nil {
    log.Fatal(err)
}
defer c.Close()

e := c.NewCall("org.freedesktop.DBus", "/org/freedesktop/DBus",
    "org.freedesktop.DBus", "GetNameOwner", "s")
e.Str("org.freedesktop.Notifications")

m, err := c.Call()
if err != nil {
    log.Fatal(err) // a D-Bus error reply is a *zerobus.Error
}
owner := m.Body().Str()
```

Build a message with `NewCall`, `NewSignal`, `NewReply` or `NewError`, write its body in the order of its signature, then send it with `Call` (wait for the reply) or `Send` (do not wait). Before sending, zerobus reads the whole body back with its signature and checks names, paths and strings, so an invalid message returns `ErrInvalid` instead of getting the connection dropped by the bus.

To bound connection setup, use `DialContext(ctx, address)`,
`SessionBusContext(ctx)`, or `SystemBusContext(ctx)`. Cancellation and deadlines
cover socket connection, authentication, and Hello; they do not affect the
returned connection. Context-caused failures wrap `context.Canceled` or
`context.DeadlineExceeded` for `errors.Is`. The original functions use
`context.Background()` and have no setup timeout.

## Concurrent calls and signal streams

`NewDispatcher(c)` takes exclusive ownership of a `Conn` with a nil `Handle`.
`DialDispatcher(ctx, address)`, `SessionBusDispatcher(ctx)` and
`SystemBusDispatcher(ctx)` also connect and authenticate. The dispatcher runs
one reader goroutine and is safe for concurrent callers:

```go
d, err := zerobus.SessionBusDispatcher(ctx)
if err != nil {
    return err
}
defer d.Close()

d.NewCall("org.freedesktop.DBus", "/org/freedesktop/DBus",
    "org.freedesktop.DBus", "GetNameOwner", "s").Str("org.freedesktop.Notifications")
m, err := d.Call(ctx)
if m != nil {
    defer m.Close()
}
if err != nil {
    return err
}
owner := m.Body().Str()
```

`NewCall`/`NewSignal` locks the encoder until the following `Call`, `Go` or
`Send`: keep that bracket on one goroutine, with no other dispatcher method
in between. `Go` returns a `Pending`; wait for `Done()` then call `Result()`.
`Cancel()` discards even a reply that raced cancellation. `Call(ctx)` returns
`ctx.Err()` without a reply when cancelled.

Delivered `Msg` values own their storage, so strings and decoded slices remain
valid across reads. **Close every received Msg**, including error replies, to
release untaken file descriptors. `TakeFD` transfers ownership to the caller.
An abandoned Pending that already received descriptors can leak them.

`Subscribe(ctx, zerobus.Match{Path: path, Interface: iface}, 32)` returns a
bounded signal channel `C`. `Match.Sender` must be empty or a unique name
(starting with `:`); well-known names are rejected with `ErrInvalid`. Resolve
well-known names through `org.freedesktop.DBus.GetNameOwner` and pass the
returned unique name. Sender filtering is also enforced locally, even when
other subscriptions install broader connection-wide bus rules.

An already-cancelled subscription sends no AddMatch. If cancellation races
AddMatch's reply, Subscribe attempts RemoveMatch with a fresh one-second
context before returning the context error (cleanup is best-effort).

Delivery never blocks the reader: a full buffer
drops the newest signal and increments `Dropped()`. Each subscriber gets its
own Msg; signals never carry file descriptors. `Subscription.Close(ctx)`
stops delivery and removes its bus match. `Dispatcher.Close()` releases
waiters, closes subscription channels, and joins the reader; `Done()` and
`Err()` report terminal state. Incoming method calls receive UnknownMethod;
use a raw Conn for servers.

## Read values

Read values in the order of the signature. Errors are sticky: check `Err()` once at the end.

```go
r := m.Body()             // signature "a{sv}"
end := r.Array('{')
for r.More(end) {
    r.Struct()
    key := r.Str()
    switch sig := r.Variant(); sig {
    case "s":
        fmt.Println(key, r.Str())
    default:
        r.Skip(sig)       // skip values you do not need
    }
}
if err := r.Err(); err != nil {
    return err
}
```

**A received message is valid only until the next `ReadMessage` or `Call`.** The strings and byte slices it returns point into the receive buffer and are overwritten by the next message. Copy what you keep:

```go
name := strings.Clone(r.Str())
icon := bytes.Clone(r.ByteArray())
```

## Receive signals

```go
c.AddMatch("type='signal',interface='org.kde.StatusNotifierItem'")
for {
    m, err := c.ReadMessage()
    if err != nil {
        return err
    }
    if m.Type == zerobus.TypeSignal {
        fmt.Println(m.Sender, m.Member)
    }
}
```

`Call` reads messages until its reply arrives. Set `Conn.Handle` to receive the messages that arrive meanwhile; they are dropped otherwise. `Handle` may send (answer a call, emit a signal), but not wait: `Call` and `ReadMessage` return `ErrNested` inside it.

## Answer calls

```go
c.RequestName("org.example.Echo", zerobus.NameFlagDoNotQueue)
for {
    m, err := c.ReadMessage()
    if err != nil {
        return err
    }
    if m.Type == zerobus.TypeMethodCall && m.Member == "Echo" {
        c.NewReply(m, "s").Str(m.Body().Str())
        c.Send()
    }
}
```

## Concurrency

A `Conn` is not safe for concurrent use. Use it from one goroutine, usually an event loop around `ReadMessage`. `Close` may be called from any goroutine: a blocked `ReadMessage` then returns `ErrClosed`.

## Not supported

- Transports other than `unix:path` and `unix:abstract` (no TCP, no `launchd`, no `autolaunch`).
- File-descriptor passing on platforms other than Linux.
- Authentication other than `EXTERNAL`.
- Reflection-based marshalling, introspection and code generation.

## Example

[`examples/trayitems`](examples/trayitems) lists the system tray items on the session bus and prints their changes.

## License

MIT

## Unix file descriptors (Linux)

`c.UnixFDs()` reports whether the bus agreed to descriptor passing. Encode an
`h` with `e.UnixFD(int(file.Fd()))`; the caller retains ownership and must keep
the descriptor open until `Send` or `Call` returns. Sending descriptors without
negotiation fails before anything is written.

For a received `h`, use `file, err := m.TakeFD(m.Body().UnixFD())` and close the
returned file when finished. `m.NumFDs()` reports the attached descriptor count.
Taking an index twice is an error. Untaken descriptors are closed at the start
of the next `ReadMessage` or `Call`, or when the connection is closed.
