package zerobus

import "encoding/binary"

// Type is the kind of a message.
type Type byte

// Message types.
const (
	TypeMethodCall   Type = 1
	TypeMethodReturn Type = 2
	TypeError        Type = 3
	TypeSignal       Type = 4
)

// Flags are the header flags of a message.
type Flags byte

// Message flags.
const (
	// FlagNoReplyExpected tells the receiver not to answer a method call.
	FlagNoReplyExpected Flags = 0x1
	// FlagNoAutoStart tells the bus not to start the destination service.
	FlagNoAutoStart Flags = 0x2
	// FlagAllowInteractiveAuth allows the receiver to prompt the user.
	FlagAllowInteractiveAuth Flags = 0x4
)

// Header field codes.
const (
	fieldPath        = 1
	fieldInterface   = 2
	fieldMember      = 3
	fieldErrorName   = 4
	fieldReplySerial = 5
	fieldDestination = 6
	fieldSender      = 7
	fieldSignature   = 8
	fieldUnixFDs     = 9
)

const headerLen = 16 // the fixed part, up to the length of the field array

// Message is a received message.
//
// Its strings point into the connection's receive buffer: they and the
// message itself are valid only until the next read on the connection.
type Message struct {
	Type        Type
	Flags       Flags
	Serial      uint32
	ReplySerial uint32
	Path        string
	Interface   string
	Member      string
	ErrorName   string
	Destination string
	Sender      string
	Signature   string // the signature of the body

	body []byte
	big  bool
	r    Reader
}

// Body returns a reader at the start of the message body. Each call starts
// over from the beginning.
func (m *Message) Body() *Reader {
	m.r = Reader{buf: m.body, big: m.big}
	return &m.r
}

// frameLen returns the full length of the message whose fixed header is b,
// which must hold at least headerLen bytes.
func frameLen(b []byte) (int, error) {
	var order binary.ByteOrder
	switch b[0] {
	case 'l':
		order = binary.LittleEndian
	case 'B':
		order = binary.BigEndian
	default:
		return 0, ErrMalformed
	}
	if b[3] != 1 {
		return 0, ErrMalformed // protocol version
	}
	body := uint64(order.Uint32(b[4:]))
	fields := uint64(order.Uint32(b[12:]))
	if body > maxMessage || fields > maxArray {
		return 0, ErrTooLarge
	}
	n := uint64(pad(headerLen+int(fields), 8)) + body
	if n > maxMessage {
		return 0, ErrTooLarge
	}
	return int(n), nil
}

// parse decodes the message b, whose length frameLen returned, into m. The
// strings of m point into b.
func parse(b []byte, m *Message) error {
	*m = Message{big: b[0] == 'B', Type: Type(b[1]), Flags: Flags(b[2])}
	r := Reader{buf: b, big: m.big, off: 4}
	bodyLen := int(r.Uint32())
	m.Serial = r.Uint32()
	if m.Serial == 0 {
		return ErrMalformed
	}
	var fds uint32
	end := r.Array('(')
	for r.More(end) {
		r.Struct()
		code := r.Byte()
		sig := r.Variant()
		if r.err != nil {
			break
		}
		want := byte('s')
		var dst *string
		switch code {
		case fieldPath:
			want, dst = 'o', &m.Path
		case fieldInterface:
			dst = &m.Interface
		case fieldMember:
			dst = &m.Member
		case fieldErrorName:
			dst = &m.ErrorName
		case fieldDestination:
			dst = &m.Destination
		case fieldSender:
			dst = &m.Sender
		case fieldSignature:
			want, dst = 'g', &m.Signature
		case fieldReplySerial, fieldUnixFDs:
			if sig != "u" {
				return ErrMalformed
			}
			if code == fieldReplySerial {
				m.ReplySerial = r.Uint32()
			} else {
				fds = r.Uint32()
			}
			continue
		default:
			r.Skip(sig) // unknown fields must be ignored
			continue
		}
		if len(sig) != 1 || sig[0] != want {
			return ErrMalformed
		}
		if want == 'g' {
			*dst = r.Signature()
		} else {
			*dst = r.Str()
		}
	}
	if r.err != nil {
		return r.err
	}
	if fds != 0 {
		return ErrMalformed // fd passing is not negotiated, so none may come
	}
	start := pad(r.off, 8)
	if start+bodyLen != len(b) || start > len(b) {
		return ErrMalformed
	}
	for _, c := range b[r.off:start] {
		if c != 0 {
			return ErrMalformed
		}
	}
	m.body = b[start:]
	// No signature means an empty body.
	if m.Signature == "" && bodyLen != 0 || !validSignature(m.Signature) || m.Path != "" && !validPath(m.Path) {
		return ErrMalformed
	}
	switch m.Type {
	case TypeMethodCall:
		if m.Path == "" || m.Member == "" {
			return ErrMalformed
		}
	case TypeSignal:
		if m.Path == "" || m.Interface == "" || m.Member == "" {
			return ErrMalformed
		}
	case TypeError:
		if m.ErrorName == "" || m.ReplySerial == 0 {
			return ErrMalformed
		}
	case TypeMethodReturn:
		if m.ReplySerial == 0 {
			return ErrMalformed
		}
	}
	return nil
}

// begin starts a new message in e: the fixed header and the field array, up
// to the body. Empty strings are left out of the header.
func (e *Encoder) begin(t Type, serial, replySerial uint32, dest, path, iface, member, errName, sig string) {
	e.buf = append(e.buf[:0], 'l', byte(t), 0, 1, 0, 0, 0, 0)
	e.buf = binary.LittleEndian.AppendUint32(e.buf, serial)
	e.body, e.sig, e.err = 0, sig, nil
	// A bus disconnects a client that sends an invalid name or leaves out a
	// field its message type requires.
	if dest != "" && !validBusName(dest) || iface != "" && !validInterface(iface) ||
		member != "" && !validMember(member) || errName != "" && !validInterface(errName) {
		e.invalid()
	}
	switch t {
	case TypeMethodCall:
		if path == "" || member == "" {
			e.invalid()
		}
	case TypeSignal:
		if path == "" || iface == "" || member == "" {
			e.invalid()
		}
	case TypeError:
		if errName == "" {
			e.invalid()
		}
	}
	fields := e.BeginArray('(')
	str := func(code byte, typ, v string) {
		if v == "" {
			return
		}
		e.Struct()
		e.Byte(code)
		e.Signature(typ)
		switch typ {
		case "o":
			e.ObjectPath(v)
		case "g":
			e.Signature(v)
		default:
			e.Str(v)
		}
	}
	str(fieldPath, "o", path)
	str(fieldInterface, "s", iface)
	str(fieldMember, "s", member)
	str(fieldErrorName, "s", errName)
	if replySerial != 0 {
		e.Struct()
		e.Byte(fieldReplySerial)
		e.Signature("u")
		e.Uint32(replySerial)
	}
	str(fieldDestination, "s", dest)
	str(fieldSignature, "g", sig)
	e.EndArray(fields)
	e.pad(8)
	e.body = len(e.buf)
}

// finish writes the body length and checks the body against the signature.
// It returns the bytes to send.
func (e *Encoder) finish() ([]byte, error) {
	if e.body == 0 {
		return nil, ErrInvalid
	}
	if e.err != nil {
		return nil, e.err
	}
	if len(e.buf) > maxMessage {
		return nil, ErrTooLarge
	}
	binary.LittleEndian.PutUint32(e.buf[4:], uint32(len(e.buf)-e.body))
	// Reading the whole body back with the signature proves it matches: a
	// bus disconnects a client that sends a body that does not.
	r := Reader{buf: e.buf[e.body:]}
	r.check(e.sig)
	if !r.Done() {
		return nil, ErrInvalid
	}
	return e.buf, nil
}
