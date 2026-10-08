# zerobus

A small D-Bus client for Go that does not allocate once it is running.

**Status: early development.** The API is not usable yet and will change.

## Goals

- **Zero allocations in steady state.** Reading signals and making calls reuses buffers; decoded strings and arrays point into the receive buffer.
- **Only the standard library** (plus `golang.org/x/sys` if needed).
- **A small client.** It connects to the session or system bus over a Unix socket, authenticates with `EXTERNAL`, calls methods, reads properties, matches signals and exports simple objects.

## Not a goal

- A full D-Bus implementation: no TCP transports, no reflection-based marshalling, no code generation.

## License

MIT
