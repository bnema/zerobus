// Package zerobus is a small D-Bus client that does not allocate in steady
// state. It speaks the D-Bus wire protocol over a Unix socket and decodes
// messages in place, so strings and arrays point into the receive buffer and
// stay valid only until the next read.
package zerobus
