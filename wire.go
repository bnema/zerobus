package zerobus

import (
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"unicode/utf8"
	"unsafe"
)

const (
	maxMessage = 128 << 20 // the D-Bus limit for a whole message
	maxArray   = 64 << 20  // the D-Bus limit for one array
	maxNest    = 32        // array or struct levels in one signature
	maxDepth   = 64        // total levels in a value, variants included
)

var (
	// ErrMalformed reports bytes that do not follow the D-Bus wire format.
	ErrMalformed = errors.New("zerobus: malformed message")
	// ErrTooLarge reports a message or array over the D-Bus size limits.
	ErrTooLarge = errors.New("zerobus: message too large")
	// ErrInvalid reports an outgoing value D-Bus does not allow, such as a
	// string with a NUL byte or a body that does not match its signature.
	ErrInvalid = errors.New("zerobus: invalid value")
)

// alignOf returns the alignment of the type that starts with signature byte c.
func alignOf(c byte) int {
	switch c {
	case 'n', 'q':
		return 2
	case 'b', 'i', 'u', 'h', 's', 'o', 'a':
		return 4
	case 'x', 't', 'd', '(', '{':
		return 8
	}
	return 1 // y, g, v
}

// pad rounds off up to a multiple of n, a power of two.
func pad(off, n int) int { return (off + n - 1) &^ (n - 1) }

// sigEnd returns the index just after the single complete type that starts
// at sig[i], or -1 if there is none.
func sigEnd(sig string, i int) int { return typeEnd(sig, i, 0, 0) }

// typeEnd is sigEnd with the array and struct nesting so far. The spec
// limits each to 32 levels; dict entries count as structs.
func typeEnd(sig string, i, arrays, structs int) int {
	if i >= len(sig) || arrays > maxNest || structs > maxNest {
		return -1
	}
	switch sig[i] {
	case 'y', 'b', 'n', 'q', 'i', 'u', 'x', 't', 'd', 'h', 's', 'o', 'g', 'v':
		return i + 1
	case 'a':
		if i+1 < len(sig) && sig[i+1] == '{' {
			return dictEnd(sig, i+1, arrays+1, structs+1)
		}
		return typeEnd(sig, i+1, arrays+1, structs)
	case '(':
		j := i + 1
		if j < len(sig) && sig[j] == ')' {
			return -1 // empty structs are not allowed
		}
		for j < len(sig) && sig[j] != ')' {
			if j = typeEnd(sig, j, arrays, structs+1); j < 0 {
				return -1
			}
		}
		if j >= len(sig) {
			return -1
		}
		return j + 1
	}
	return -1 // includes '{': a dict entry is only valid as an array element
}

// dictEnd returns the index just after the dict entry type at sig[i] ('{').
// Its key must be a basic type and it holds exactly two types.
func dictEnd(sig string, i, arrays, structs int) int {
	j := i + 1
	if j >= len(sig) || !basic(sig[j]) || structs > maxNest {
		return -1
	}
	if j = typeEnd(sig, j+1, arrays, structs); j < 0 || j >= len(sig) || sig[j] != '}' {
		return -1
	}
	return j + 1
}

func basic(c byte) bool {
	switch c {
	case 'y', 'b', 'n', 'q', 'i', 'u', 'x', 't', 'd', 'h', 's', 'o', 'g':
		return true
	}
	return false
}

// validSignature reports whether sig is a list of complete types that fits
// in a D-Bus signature.
func validSignature(sig string) bool {
	if len(sig) > 255 {
		return false
	}
	for i := 0; i < len(sig); {
		if i = sigEnd(sig, i); i < 0 {
			return false
		}
	}
	return true
}

// validPath reports whether p is a valid object path.
func validPath(p string) bool {
	if p == "/" {
		return true
	}
	if len(p) < 2 || p[0] != '/' || p[len(p)-1] == '/' {
		return false
	}
	for i := 1; i < len(p); i++ {
		c := p[i]
		switch {
		case c == '/':
			if p[i-1] == '/' {
				return false
			}
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
		default:
			return false
		}
	}
	return true
}

// validMember reports whether s is a valid member name: letters, digits and
// '_', not starting with a digit.
func validMember(s string) bool {
	if s == "" || len(s) > 255 || s[0] >= '0' && s[0] <= '9' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !nameChar(s[i]) {
			return false
		}
	}
	return true
}

// nameChar reports whether c may appear in a member or interface name.
func nameChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

// validInterface reports whether s is a valid interface or error name: two
// or more member-like elements separated by dots.
func validInterface(s string) bool {
	if len(s) > 255 {
		return false
	}
	n := 0
	for more := true; more; n++ {
		var e string
		e, s, more = strings.Cut(s, ".")
		if !validMember(e) {
			return false
		}
	}
	return n >= 2
}

// validBusName reports whether s is a valid unique (":1.42") or well-known
// ("org.example.App") bus name.
func validBusName(s string) bool {
	if s == "" || len(s) > 255 {
		return false
	}
	unique := s[0] == ':'
	if unique {
		s = s[1:]
	}
	n := 0
	for more := true; more; n++ {
		var e string
		e, s, more = strings.Cut(s, ".")
		if e == "" || !unique && e[0] >= '0' && e[0] <= '9' {
			return false
		}
		for i := 0; i < len(e); i++ {
			if !nameChar(e[i]) && e[i] != '-' {
				return false
			}
		}
	}
	return n >= 2
}

// Reader decodes D-Bus values in place.
//
// Strings and byte slices it returns point into the message buffer: they are
// valid only until the next Conn.ReadMessage. Copy them (strings.Clone,
// bytes.Clone) to keep them longer.
//
// Read values in the order of the message signature. Errors are sticky:
// after the first one, every read returns a zero value and Err reports it.
type Reader struct {
	buf []byte
	off int
	big bool
	err error
}

// Err returns the first error the reader met, or nil.
func (r *Reader) Err() error { return r.err }

// Done reports whether every byte was read without error.
func (r *Reader) Done() bool { return r.err == nil && r.off == len(r.buf) }

func (r *Reader) fail() {
	if r.err == nil {
		r.err = ErrMalformed
	}
}

// take aligns to align and returns the next size bytes.
func (r *Reader) take(align, size int) []byte {
	if r.err != nil {
		return nil
	}
	off := pad(r.off, align)
	if off > len(r.buf)-size {
		r.fail()
		return nil
	}
	r.off = off + size
	return r.buf[off:r.off]
}

// Byte reads a BYTE (y).
func (r *Reader) Byte() byte {
	if b := r.take(1, 1); b != nil {
		return b[0]
	}
	return 0
}

// Bool reads a BOOLEAN (b).
func (r *Reader) Bool() bool {
	v := r.Uint32()
	if v > 1 {
		r.fail()
		return false
	}
	return v == 1
}

// Int16 reads an INT16 (n).
func (r *Reader) Int16() int16 { return int16(r.Uint16()) }

// Uint16 reads a UINT16 (q).
func (r *Reader) Uint16() uint16 {
	b := r.take(2, 2)
	if b == nil {
		return 0
	}
	if r.big {
		return binary.BigEndian.Uint16(b)
	}
	return binary.LittleEndian.Uint16(b)
}

// Int32 reads an INT32 (i).
func (r *Reader) Int32() int32 { return int32(r.Uint32()) }

// Uint32 reads a UINT32 (u).
func (r *Reader) Uint32() uint32 {
	b := r.take(4, 4)
	if b == nil {
		return 0
	}
	if r.big {
		return binary.BigEndian.Uint32(b)
	}
	return binary.LittleEndian.Uint32(b)
}

// UnixFD reads a UNIX_FD (h): an index into the fds sent with the message.
// zerobus does not receive file descriptors, so the index has nothing to
// point to; it is only read so the following values can be.
func (r *Reader) UnixFD() uint32 { return r.Uint32() }

// Int64 reads an INT64 (x).
func (r *Reader) Int64() int64 { return int64(r.Uint64()) }

// Uint64 reads a UINT64 (t).
func (r *Reader) Uint64() uint64 {
	b := r.take(8, 8)
	if b == nil {
		return 0
	}
	if r.big {
		return binary.BigEndian.Uint64(b)
	}
	return binary.LittleEndian.Uint64(b)
}

// Float64 reads a DOUBLE (d).
func (r *Reader) Float64() float64 { return math.Float64frombits(r.Uint64()) }

// Str reads a STRING (s). The result points into the message buffer.
func (r *Reader) Str() string { return r.str(int(r.Uint32())) }

// ObjectPath reads an OBJECT_PATH (o). The result points into the message
// buffer.
func (r *Reader) ObjectPath() string { return r.Str() }

// Signature reads a SIGNATURE (g). The result points into the message buffer.
func (r *Reader) Signature() string { return r.str(int(r.Byte())) }

// str returns the next n bytes as a string and skips the NUL after them.
func (r *Reader) str(n int) string {
	if r.err != nil {
		return ""
	}
	if n < 0 || n > len(r.buf)-r.off-1 || r.buf[r.off+n] != 0 {
		r.fail()
		return ""
	}
	s := unsafe.String(unsafe.SliceData(r.buf[r.off:]), n)
	r.off += n + 1
	return s
}

// Variant reads the signature of a VARIANT (v) and returns it. Read the
// value that follows with the matching method, or skip it with Skip.
func (r *Reader) Variant() string {
	sig := r.Signature()
	if r.err == nil && sigEnd(sig, 0) != len(sig) {
		r.fail()
		return ""
	}
	return sig
}

// Struct aligns the reader to the start of a STRUCT or DICT_ENTRY. Call it
// before reading the first field.
func (r *Reader) Struct() { r.take(8, 0) }

// Array reads the header of an ARRAY whose element type starts with the
// signature byte elem, and returns the offset where the array ends. Read the
// elements while More returns true:
//
//	end := r.Array('s')
//	for r.More(end) {
//		name := r.Str()
//	}
func (r *Reader) Array(elem byte) int {
	n := r.Uint32()
	if r.err != nil {
		return r.off
	}
	if n > maxArray {
		r.fail()
		return r.off
	}
	off := pad(r.off, alignOf(elem))
	if off > len(r.buf)-int(n) {
		r.fail()
		return r.off
	}
	r.off = off
	return off + int(n)
}

// More reports whether the array that ends at end has another element.
func (r *Reader) More(end int) bool {
	if r.err != nil {
		return false
	}
	if r.off < end {
		return true
	}
	if r.off > end {
		r.fail()
	}
	return false
}

// SkipTo moves to end, the value returned by Array. Use it to leave an array
// before its last element.
func (r *Reader) SkipTo(end int) {
	if r.err != nil {
		return
	}
	if end < r.off || end > len(r.buf) {
		r.fail()
		return
	}
	r.off = end
}

// ByteArray reads an ARRAY of BYTE (ay). The result points into the message
// buffer.
func (r *Reader) ByteArray() []byte {
	end := r.Array('y')
	if r.err != nil {
		return nil
	}
	b := r.buf[r.off:end:end]
	r.off = end
	return b
}

// Skip reads past values of the given signature. Arrays are skipped by their
// length, without reading their elements.
func (r *Reader) Skip(sig string) { r.skip(sig, 0, false) }

// check reads every value of sig, array elements included, and fails on any
// value the bus would reject. It proves an outgoing body matches its
// signature.
func (r *Reader) check(sig string) { r.skip(sig, 0, true) }

func (r *Reader) skip(sig string, depth int, strict bool) {
	for i := 0; i < len(sig) && r.err == nil; {
		i = r.skipOne(sig, i, depth, strict)
	}
}

// skipOne skips the value of the type at sig[i] and returns the index of the
// next type. In strict mode it also reads array elements and checks values.
func (r *Reader) skipOne(sig string, i, depth int, strict bool) int {
	if depth > maxDepth {
		r.fail()
		return len(sig)
	}
	switch c := sig[i]; c {
	case 'y':
		r.take(1, 1)
	case 'n', 'q':
		r.take(2, 2)
	case 'b':
		r.Bool()
	case 'i', 'u', 'h':
		r.take(4, 4)
	case 'x', 't', 'd':
		r.take(8, 8)
	case 's', 'o':
		s := r.Str()
		if strict && (!utf8.ValidString(s) || c == 'o' && !validPath(s)) {
			r.fail()
		}
	case 'g':
		if s := r.Signature(); strict && !validSignature(s) {
			r.fail()
		}
	case 'v':
		r.skip(r.Variant(), depth+1, strict)
	case 'a':
		next := sigEnd(sig, i)
		if next < 0 {
			r.fail()
			return len(sig)
		}
		end := r.Array(sig[i+1])
		if !strict {
			r.SkipTo(end)
			return next
		}
		for r.More(end) {
			r.skipOne(sig, i+1, depth+1, true)
		}
		return next
	case '(', '{':
		closer := byte(')')
		if c == '{' {
			closer = '}'
		}
		r.Struct()
		j := i + 1
		for j < len(sig) && sig[j] != closer && r.err == nil {
			j = r.skipOne(sig, j, depth+1, strict)
		}
		if j >= len(sig) {
			r.fail()
			return len(sig)
		}
		return j + 1
	default:
		r.fail()
		return len(sig)
	}
	return i + 1
}

// Encoder appends D-Bus values to an outgoing message. Get one from
// Conn.NewCall, Conn.Signal, Conn.Reply or Conn.Error, write the body values
// in the order of the message signature, then call Conn.Send.
//
// Errors are sticky and reported by Conn.Send.
type Encoder struct {
	buf  []byte
	body int    // offset of the body; 0 when no message is being built
	sig  string // body signature, checked by finish
	err  error
}

func (e *Encoder) invalid() {
	if e.err == nil {
		e.err = ErrInvalid
	}
}

func (e *Encoder) pad(n int) {
	for len(e.buf)&(n-1) != 0 {
		e.buf = append(e.buf, 0)
	}
}

// Byte writes a BYTE (y).
func (e *Encoder) Byte(v byte) { e.buf = append(e.buf, v) }

// Bool writes a BOOLEAN (b).
func (e *Encoder) Bool(v bool) {
	var u uint32
	if v {
		u = 1
	}
	e.Uint32(u)
}

// Int16 writes an INT16 (n).
func (e *Encoder) Int16(v int16) { e.Uint16(uint16(v)) }

// Uint16 writes a UINT16 (q).
func (e *Encoder) Uint16(v uint16) {
	e.pad(2)
	e.buf = binary.LittleEndian.AppendUint16(e.buf, v)
}

// Int32 writes an INT32 (i).
func (e *Encoder) Int32(v int32) { e.Uint32(uint32(v)) }

// Uint32 writes a UINT32 (u).
func (e *Encoder) Uint32(v uint32) {
	e.pad(4)
	e.buf = binary.LittleEndian.AppendUint32(e.buf, v)
}

// Int64 writes an INT64 (x).
func (e *Encoder) Int64(v int64) { e.Uint64(uint64(v)) }

// Uint64 writes a UINT64 (t).
func (e *Encoder) Uint64(v uint64) {
	e.pad(8)
	e.buf = binary.LittleEndian.AppendUint64(e.buf, v)
}

// Float64 writes a DOUBLE (d).
func (e *Encoder) Float64(v float64) { e.Uint64(math.Float64bits(v)) }

// Str writes a STRING (s).
func (e *Encoder) Str(s string) {
	e.Uint32(uint32(len(s)))
	e.text(s)
}

// ObjectPath writes an OBJECT_PATH (o).
func (e *Encoder) ObjectPath(p string) {
	if !validPath(p) {
		e.invalid()
	}
	e.Str(p)
}

// Signature writes a SIGNATURE (g).
func (e *Encoder) Signature(sig string) {
	if !validSignature(sig) {
		e.invalid()
	}
	e.Byte(byte(len(sig)))
	e.text(sig)
}

// text appends s and its NUL terminator.
func (e *Encoder) text(s string) {
	// The bus disconnects a client that sends a NUL or invalid UTF-8.
	if strings.IndexByte(s, 0) >= 0 || !utf8.ValidString(s) {
		e.invalid()
	}
	e.buf = append(e.buf, s...)
	e.buf = append(e.buf, 0)
}

// Variant writes the signature of a VARIANT (v). Write its value next.
func (e *Encoder) Variant(sig string) {
	if sigEnd(sig, 0) != len(sig) {
		e.invalid()
	}
	e.Signature(sig)
}

// Struct aligns the encoder to the start of a STRUCT or DICT_ENTRY. Call it
// before writing the first field.
func (e *Encoder) Struct() { e.pad(8) }

// ArrayMark is an array being written; pass it to EndArray.
type ArrayMark struct{ at, start int }

// BeginArray starts an ARRAY whose element type starts with the signature
// byte elem. Write the elements, then call EndArray:
//
//	a := e.BeginArray('s')
//	e.Str("one")
//	e.Str("two")
//	e.EndArray(a)
func (e *Encoder) BeginArray(elem byte) ArrayMark {
	e.pad(4)
	at := len(e.buf)
	e.buf = append(e.buf, 0, 0, 0, 0)
	e.pad(alignOf(elem))
	return ArrayMark{at: at, start: len(e.buf)}
}

// EndArray finishes the array started by BeginArray.
func (e *Encoder) EndArray(a ArrayMark) {
	n := len(e.buf) - a.start
	if n > maxArray {
		e.invalid()
		return
	}
	binary.LittleEndian.PutUint32(e.buf[a.at:], uint32(n))
}

// ByteArray writes an ARRAY of BYTE (ay).
func (e *Encoder) ByteArray(b []byte) {
	e.Uint32(uint32(len(b)))
	e.buf = append(e.buf, b...)
}

// SetFlags sets the flags of the message being built.
func (e *Encoder) SetFlags(f Flags) {
	if e.body != 0 {
		e.buf[2] = byte(f)
	}
}
