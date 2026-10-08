package zerobus

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	busName      = "org.freedesktop.DBus"
	busPath      = "/org/freedesktop/DBus"
	maxAuthLine  = 512
	defaultBufSz = 16 << 10
)

// ErrClosed reports a read or send on a closed or broken connection.
var ErrClosed = errors.New("zerobus: connection closed")

// Error is a D-Bus error reply.
type Error struct {
	Name    string // such as org.freedesktop.DBus.Error.ServiceUnknown
	Message string // the first string of the body, if any
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Name
	}
	return e.Name + ": " + e.Message
}

// Conn is a connection to a message bus.
//
// A Conn is not safe for concurrent use: read and send from one goroutine,
// usually an event loop around ReadMessage. Close may be called from any
// goroutine to stop a blocked read.
type Conn struct {
	c      *net.UnixConn
	serial uint32
	name   string

	rbuf       []byte // received bytes; rbuf[start:end] are not read yet
	start, end int
	msg        Message
	enc        Encoder
	err        error // sticky: the connection is unusable after it

	// Handle receives the messages that arrive while Call waits for its
	// reply. It may be nil; those messages are then dropped. It must not
	// call Call, Send or ReadMessage.
	Handle func(*Message)
}

// SessionBus connects to the session bus named by
// DBUS_SESSION_BUS_ADDRESS, or $XDG_RUNTIME_DIR/bus when it is not set.
func SessionBus() (*Conn, error) {
	addr := os.Getenv("DBUS_SESSION_BUS_ADDRESS")
	if addr == "" {
		dir := os.Getenv("XDG_RUNTIME_DIR")
		if dir == "" {
			return nil, errors.New("zerobus: no session bus: DBUS_SESSION_BUS_ADDRESS and XDG_RUNTIME_DIR are not set")
		}
		addr = "unix:path=" + dir + "/bus"
	}
	return Dial(addr)
}

// SystemBus connects to the system bus named by DBUS_SYSTEM_BUS_ADDRESS, or
// the standard socket when it is not set.
func SystemBus() (*Conn, error) {
	addr := os.Getenv("DBUS_SYSTEM_BUS_ADDRESS")
	if addr == "" {
		addr = "unix:path=/run/dbus/system_bus_socket"
	}
	return Dial(addr)
}

// Dial connects to a bus address such as "unix:path=/run/user/1000/bus",
// authenticates, and registers with the bus. Only unix:path and
// unix:abstract addresses are supported; with several addresses separated by
// ';', the first that connects is used.
func Dial(address string) (*Conn, error) {
	var errs []error
	for a := range strings.SplitSeq(address, ";") {
		if a == "" {
			continue
		}
		c, err := dialOne(a)
		if err == nil {
			return c, nil
		}
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("zerobus: empty bus address")
	}
	return nil, errors.Join(errs...)
}

func dialOne(addr string) (*Conn, error) {
	transport, params, ok := strings.Cut(addr, ":")
	if !ok || transport != "unix" {
		return nil, fmt.Errorf("zerobus: unsupported address %q", addr)
	}
	var sock string
	for kv := range strings.SplitSeq(params, ",") {
		k, v, _ := strings.Cut(kv, "=")
		v, err := unescape(v)
		if err != nil {
			return nil, fmt.Errorf("zerobus: address %q: %w", addr, err)
		}
		switch k {
		case "path":
			sock = v
		case "abstract":
			sock = "@" + v
		}
	}
	if sock == "" {
		return nil, fmt.Errorf("zerobus: address %q has no path or abstract socket", addr)
	}
	uc, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("zerobus: %w", err)
	}
	c := &Conn{c: uc, rbuf: make([]byte, defaultBufSz)}
	c.enc.buf = make([]byte, 0, defaultBufSz)
	if err := c.auth(); err != nil {
		return nil, errors.Join(err, uc.Close())
	}
	if err := c.hello(); err != nil {
		return nil, errors.Join(err, uc.Close())
	}
	return c, nil
}

// unescape decodes the %xx escapes of a D-Bus address value.
func unescape(s string) (string, error) {
	if !strings.Contains(s, "%") {
		return s, nil
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			b.WriteByte(s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", errors.New("bad escape")
		}
		v, err := strconv.ParseUint(s[i+1:i+3], 16, 8)
		if err != nil {
			return "", errors.New("bad escape")
		}
		b.WriteByte(byte(v))
		i += 2
	}
	return b.String(), nil
}

// auth runs the SASL EXTERNAL handshake. The bus checks the peer
// credentials of the socket against the user id sent here.
func (c *Conn) auth() error {
	uid := strconv.Itoa(os.Getuid())
	line := fmt.Appendf(nil, "\x00AUTH EXTERNAL %x\r\n", uid)
	if _, err := c.c.Write(line); err != nil {
		return fmt.Errorf("zerobus: auth: %w", err)
	}
	reply, err := c.authLine()
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(reply, []byte("OK ")) {
		return fmt.Errorf("zerobus: auth rejected: %q", reply)
	}
	if _, err := c.c.Write([]byte("BEGIN\r\n")); err != nil {
		return fmt.Errorf("zerobus: auth: %w", err)
	}
	return nil
}

// authLine reads one line of the handshake. Bytes after it stay in rbuf for
// the message reader.
func (c *Conn) authLine() ([]byte, error) {
	for {
		if i := bytes.Index(c.rbuf[c.start:c.end], []byte("\r\n")); i >= 0 {
			line := c.rbuf[c.start : c.start+i]
			c.start += i + 2
			return line, nil
		}
		if c.end-c.start >= maxAuthLine {
			return nil, errors.New("zerobus: auth: line too long")
		}
		n, err := c.c.Read(c.rbuf[c.end:])
		if err != nil {
			return nil, fmt.Errorf("zerobus: auth: %w", err)
		}
		c.end += n
	}
}

func (c *Conn) hello() error {
	c.NewCall(busName, busPath, busName, "Hello", "")
	m, err := c.Call()
	if err != nil {
		return fmt.Errorf("zerobus: hello: %w", err)
	}
	body := m.Body()
	name := body.Str()
	if body.Err() != nil {
		return fmt.Errorf("zerobus: hello: %w", body.Err())
	}
	c.name = strings.Clone(name)
	return nil
}

// UniqueName returns the name the bus gave this connection, such as ":1.42".
func (c *Conn) UniqueName() string { return c.name }

// Close closes the connection. A blocked ReadMessage returns ErrClosed.
func (c *Conn) Close() error { return c.c.Close() }

func (c *Conn) nextSerial() uint32 {
	c.serial++
	if c.serial == 0 {
		c.serial = 1
	}
	return c.serial
}

// NewCall starts a method call and returns the encoder for its body, which
// must match sig. Send it with Send or Call. Empty dest and iface are left
// out of the header.
//
// The connection has one encoder: send a message before starting another.
func (c *Conn) NewCall(dest, path, iface, member, sig string) *Encoder {
	c.enc.begin(TypeMethodCall, c.nextSerial(), 0, dest, path, iface, member, "", sig)
	return &c.enc
}

// NewSignal starts a signal and returns the encoder for its body.
func (c *Conn) NewSignal(path, iface, member, sig string) *Encoder {
	c.enc.begin(TypeSignal, c.nextSerial(), 0, "", path, iface, member, "", sig)
	return &c.enc
}

// NewReply starts the reply to the method call m and returns the encoder for
// its body.
func (c *Conn) NewReply(m *Message, sig string) *Encoder {
	c.enc.begin(TypeMethodReturn, c.nextSerial(), m.Serial, m.Sender, "", "", "", "", sig)
	return &c.enc
}

// NewError starts an error reply to the method call m. Write a message
// string as the body when sig is "s".
func (c *Conn) NewError(m *Message, name, sig string) *Encoder {
	c.enc.begin(TypeError, c.nextSerial(), m.Serial, m.Sender, "", "", "", name, sig)
	return &c.enc
}

// Send sends the message built since the last NewCall, NewSignal, NewReply
// or NewError and returns its serial.
func (c *Conn) Send() (uint32, error) {
	if c.err != nil {
		return 0, c.err
	}
	b, err := c.enc.finish()
	c.enc.body = 0
	if err != nil {
		return 0, err
	}
	if _, err := c.c.Write(b); err != nil {
		c.err = closedErr(err)
		return 0, c.err
	}
	return readSerial(b), nil
}

func readSerial(b []byte) uint32 {
	return uint32(b[8]) | uint32(b[9])<<8 | uint32(b[10])<<16 | uint32(b[11])<<24
}

// Call sends the method call being built and waits for its reply. Messages
// that arrive meanwhile go to Handle. An error reply returns an *Error.
//
// The reply is valid until the next read, like ReadMessage's.
func (c *Conn) Call() (*Message, error) {
	serial, err := c.Send()
	if err != nil {
		return nil, err
	}
	for {
		m, err := c.ReadMessage()
		if err != nil {
			return nil, err
		}
		if m.ReplySerial == serial && (m.Type == TypeMethodReturn || m.Type == TypeError) {
			if m.Type == TypeError {
				return m, replyError(m)
			}
			return m, nil
		}
		if c.Handle != nil {
			c.Handle(m)
		}
	}
}

// replyError builds an *Error from an error reply. It copies the strings so
// the error stays valid; this is the only allocation of an error path.
func replyError(m *Message) *Error {
	e := &Error{Name: strings.Clone(m.ErrorName)}
	if len(m.Signature) > 0 && m.Signature[0] == 's' {
		if s := m.Body().Str(); s != "" {
			e.Message = strings.Clone(s)
		}
	}
	return e
}

// ReadMessage blocks until the next message arrives. The message and its
// strings are valid until the next call to ReadMessage or Call.
func (c *Conn) ReadMessage() (*Message, error) {
	if c.err != nil {
		return nil, c.err
	}
	if err := c.fill(headerLen); err != nil {
		return nil, err
	}
	n, err := frameLen(c.rbuf[c.start:])
	if err != nil {
		c.err = err
		return nil, err
	}
	if err := c.fill(n); err != nil {
		return nil, err
	}
	b := c.rbuf[c.start : c.start+n : c.start+n]
	c.start += n
	if err := parse(b, &c.msg); err != nil {
		c.err = err
		return nil, err
	}
	return &c.msg, nil
}

// fill reads until rbuf[start:] holds at least n bytes. It moves unread
// bytes to the front, which is safe because the previous message is no
// longer valid once a new read starts.
func (c *Conn) fill(n int) error {
	if c.end-c.start >= n {
		return nil
	}
	if c.start > 0 {
		c.end = copy(c.rbuf, c.rbuf[c.start:c.end])
		c.start = 0
	}
	if n > len(c.rbuf) {
		grown := make([]byte, max(n, 2*len(c.rbuf)))
		copy(grown, c.rbuf[:c.end])
		c.rbuf = grown
	}
	for c.end < n {
		k, err := c.c.Read(c.rbuf[c.end:])
		c.end += k
		if err != nil && c.end < n {
			c.err = closedErr(err)
			return c.err
		}
	}
	return nil
}

func closedErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return ErrClosed
	}
	return fmt.Errorf("zerobus: %w", err)
}

// AddMatch asks the bus to send this connection the messages that match
// rule, such as "type='signal',interface='org.kde.StatusNotifierItem'".
func (c *Conn) AddMatch(rule string) error {
	c.NewCall(busName, busPath, busName, "AddMatch", "s").Str(rule)
	_, err := c.Call()
	return err
}

// RemoveMatch removes a rule added by AddMatch.
func (c *Conn) RemoveMatch(rule string) error {
	c.NewCall(busName, busPath, busName, "RemoveMatch", "s").Str(rule)
	_, err := c.Call()
	return err
}

// Reply codes of RequestName.
const (
	NamePrimaryOwner = 1
	NameInQueue      = 2
	NameExists       = 3
	NameAlreadyOwner = 4
)

// Flags of RequestName.
const (
	NameFlagAllowReplacement = 0x1
	NameFlagReplaceExisting  = 0x2
	NameFlagDoNotQueue       = 0x4
)

// RequestName asks the bus for a well-known name and returns its reply code,
// such as NamePrimaryOwner.
func (c *Conn) RequestName(name string, flags uint32) (uint32, error) {
	e := c.NewCall(busName, busPath, busName, "RequestName", "su")
	e.Str(name)
	e.Uint32(flags)
	m, err := c.Call()
	if err != nil {
		return 0, err
	}
	body := m.Body()
	code := body.Uint32()
	return code, body.Err()
}
