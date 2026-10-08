# zerobus

A small D-Bus client for Go that does not allocate once it is running.

```sh
go get github.com/bnema/zerobus   # Go 1.24 or newer
```

- **Zero allocations per message.** After the first few messages warm up its buffers, a call, a reply or a signal costs no heap allocation.
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
- Passing file descriptors (`UNIX_FD` values can be read, but no descriptors are received).
- Authentication other than `EXTERNAL`.
- Reflection-based marshalling, introspection and code generation.

## Example

[`examples/trayitems`](examples/trayitems) lists the system tray items on the session bus and prints their changes.

## License

MIT
