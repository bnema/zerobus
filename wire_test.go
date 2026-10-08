package zerobus

import (
	"errors"
	"strings"
	"testing"
)

func TestSignatures(t *testing.T) {
	valid := []string{"", "s", "su", "a{sv}", "a(iiay)", "(s(ss))", "aa{s(ii)}", "a{oa{sa{sv}}}", "v", "g", "h"}
	invalid := []string{"a", "(", "()", "{sv}", "a{vs}", "a{s}", "a{sss}", "(s", "z", "a{(s)s}", "a{as}"}
	for _, s := range valid {
		if !validSignature(s) {
			t.Errorf("%q should be valid", s)
		}
	}
	for _, s := range invalid {
		if validSignature(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
	// The spec allows 32 nested arrays and 32 nested structs.
	nest := func(open, close string, n int) string {
		return strings.Repeat(open, n) + "y" + strings.Repeat(close, n)
	}
	for _, c := range []struct {
		sig string
		ok  bool
	}{
		{nest("a", "", 32), true},
		{nest("a", "", 33), false},
		{nest("(", ")", 32), true},
		{nest("(", ")", 33), false},
		{nest("a(", ")", 32), true},
		{strings.Repeat("a{s", 33) + "y" + strings.Repeat("}", 33), false},
	} {
		if validSignature(c.sig) != c.ok {
			t.Errorf("validSignature(%d bytes) = %v, want %v", len(c.sig), !c.ok, c.ok)
		}
	}
}

func TestNames(t *testing.T) {
	for _, s := range []string{"org.example", "org.kde.StatusNotifierWatcher", "a_b.C1"} {
		if !validInterface(s) {
			t.Errorf("interface %q should be valid", s)
		}
	}
	for _, s := range []string{"", "org", "org.", ".org.a", "org.1a", "org.a-b"} {
		if validInterface(s) {
			t.Errorf("interface %q should be invalid", s)
		}
	}
	for _, s := range []string{":1.42", "org.freedesktop.StatusNotifierItem-799476-1", "org.a"} {
		if !validBusName(s) {
			t.Errorf("bus name %q should be valid", s)
		}
	}
	for _, s := range []string{"", ":", "org", "org..a", "org.1a", "org.a/b"} {
		if validBusName(s) {
			t.Errorf("bus name %q should be invalid", s)
		}
	}
	for _, s := range []string{"", "1a", "a.b", "a-b"} {
		if validMember(s) {
			t.Errorf("member %q should be invalid", s)
		}
	}
	for name, h := range map[string][4]string{ // dest, path, iface, member
		"bad destination": {"bad name", "/a", "a.b", "M"},
		"call no path":    {"a.b", "", "a.b", "M"},
		"call no member":  {"a.b", "/a", "a.b", ""},
	} {
		var e Encoder
		e.begin(TypeMethodCall, 1, 0, h[0], h[1], h[2], h[3], "", "")
		if _, err := e.finish(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s accepted: %v", name, err)
		}
	}
	var e Encoder
	e.begin(TypeSignal, 1, 0, "", "/a", "", "C", "", "")
	if _, err := e.finish(); !errors.Is(err, ErrInvalid) {
		t.Errorf("signal without interface accepted: %v", err)
	}
	e.begin(TypeError, 1, 1, "", "", "", "", "", "")
	if _, err := e.finish(); !errors.Is(err, ErrInvalid) {
		t.Errorf("error without name accepted: %v", err)
	}
}

func TestPaths(t *testing.T) {
	for _, p := range []string{"/", "/a", "/org/kde/StatusNotifierItem", "/a_1/B2"} {
		if !validPath(p) {
			t.Errorf("%q should be valid", p)
		}
	}
	for _, p := range []string{"", "a", "//", "/a/", "/a//b", "/a-b", "/a.b"} {
		if validPath(p) {
			t.Errorf("%q should be invalid", p)
		}
	}
}

// build encodes a method call with every type, the way Conn does.
func build(e *Encoder) []byte {
	const sig = "ybnqiuxtdsogva{sv}ay(si)"
	e.begin(TypeMethodCall, 7, 0, "org.example", "/org/example", "org.example.I", "M", "", sig)
	e.Byte(0xAB)
	e.Bool(true)
	e.Int16(-2)
	e.Uint16(3)
	e.Int32(-4)
	e.Uint32(5)
	e.Int64(-6)
	e.Uint64(7)
	e.Float64(8.5)
	e.Str("héllo")
	e.ObjectPath("/a/b")
	e.Signature("a{sv}")
	e.Variant("u")
	e.Uint32(42)
	d := e.BeginArray('{')
	for _, k := range []string{"one", "two"} {
		e.Struct()
		e.Str(k)
		e.Variant("s")
		e.Str(k + "!")
	}
	e.EndArray(d)
	e.ByteArray([]byte{1, 2, 3})
	e.Struct()
	e.Str("x")
	e.Int32(9)
	b, err := e.finish()
	if err != nil {
		panic(err)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	var e Encoder
	b := build(&e)
	n, err := frameLen(b)
	if err != nil || n != len(b) {
		t.Fatalf("frameLen = %d, %v; want %d", n, err, len(b))
	}
	var m Message
	if err := parse(b, &m); err != nil {
		t.Fatal(err)
	}
	if m.Type != TypeMethodCall || m.Serial != 7 || m.Path != "/org/example" || m.Interface != "org.example.I" ||
		m.Member != "M" || m.Destination != "org.example" || m.Signature != "ybnqiuxtdsogva{sv}ay(si)" {
		t.Fatalf("header: %+v", m)
	}
	r := m.Body()
	if r.Byte() != 0xAB || !r.Bool() || r.Int16() != -2 || r.Uint16() != 3 || r.Int32() != -4 || r.Uint32() != 5 ||
		r.Int64() != -6 || r.Uint64() != 7 || r.Float64() != 8.5 || r.Str() != "héllo" || r.ObjectPath() != "/a/b" ||
		r.Signature() != "a{sv}" {
		t.Fatalf("basic values: %v", r.Err())
	}
	if sig := r.Variant(); sig != "u" || r.Uint32() != 42 {
		t.Fatal("variant")
	}
	var keys []string
	end := r.Array('{')
	for r.More(end) {
		r.Struct()
		k := r.Str()
		if r.Variant() != "s" || r.Str() != k+"!" {
			t.Fatal("dict value")
		}
		keys = append(keys, k)
	}
	if len(keys) != 2 || keys[0] != "one" || keys[1] != "two" {
		t.Fatalf("keys %v", keys)
	}
	if ay := r.ByteArray(); len(ay) != 3 || ay[2] != 3 {
		t.Fatalf("ay %v", ay)
	}
	r.Struct()
	if r.Str() != "x" || r.Int32() != 9 || !r.Done() {
		t.Fatalf("struct: %v", r.Err())
	}

	// Skip the same body by its signature.
	r = m.Body()
	r.Skip(m.Signature)
	if !r.Done() {
		t.Fatalf("skip: %v off=%d len=%d", r.Err(), r.off, len(r.buf))
	}
}

func TestEmptyArrayAlignment(t *testing.T) {
	// An empty array of 8-aligned elements still pads to the element
	// alignment, and its length excludes that padding.
	var e Encoder
	e.begin(TypeSignal, 1, 0, "", "/a", "a.b", "C", "", "a(ii)u")
	a := e.BeginArray('(')
	e.EndArray(a)
	e.Uint32(1)
	b, err := e.finish()
	if err != nil {
		t.Fatal(err)
	}
	var m Message
	if err := parse(b, &m); err != nil {
		t.Fatal(err)
	}
	r := m.Body()
	end := r.Array('(')
	if r.More(end) || r.Uint32() != 1 || !r.Done() {
		t.Fatalf("got %v", r.Err())
	}
}

func TestBigEndian(t *testing.T) {
	// A signal from a big-endian peer: path /a, interface a.b, member C,
	// body "u" = 0x01020304. The field array is 55 bytes, then 1 byte of
	// padding before the body.
	be := []byte{'B', 4, 0, 1, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 55,
		1, 1, 'o', 0, 0, 0, 0, 2, '/', 'a', 0, 0, 0, 0, 0, 0,
		2, 1, 's', 0, 0, 0, 0, 3, 'a', '.', 'b', 0, 0, 0, 0, 0,
		3, 1, 's', 0, 0, 0, 0, 1, 'C', 0, 0, 0, 0, 0, 0, 0,
		8, 1, 'g', 0, 1, 'u', 0, 0,
		1, 2, 3, 4}
	n, err := frameLen(be)
	if err != nil || n != len(be) {
		t.Fatalf("frameLen %d %v want %d", n, err, len(be))
	}
	var m Message
	if err := parse(be, &m); err != nil {
		t.Fatal(err)
	}
	if m.Path != "/a" || m.Interface != "a.b" || m.Member != "C" || m.Body().Uint32() != 0x01020304 {
		t.Fatalf("%+v", m)
	}
}

func TestEncoderRejects(t *testing.T) {
	cases := map[string]func(e *Encoder){
		"nul in string":   func(e *Encoder) { e.Str("a\x00b") },
		"bad utf-8":       func(e *Encoder) { e.Str("\xff") },
		"bad path":        func(e *Encoder) { e.ObjectPath("a/b") },
		"missing value":   func(e *Encoder) {},
		"wrong type":      func(e *Encoder) { e.Byte(1) },
		"extra value":     func(e *Encoder) { e.Str("a"); e.Str("b") },
		"bad variant sig": func(e *Encoder) { e.Variant("ss") },
		"wrong element": func(e *Encoder) {
			a := e.BeginArray('s')
			e.Uint32(77)
			e.EndArray(a)
		},
		"bad nested variant": func(e *Encoder) {
			a := e.BeginArray('v')
			e.Byte(1)
			e.Str("z")
			e.EndArray(a)
		},
	}
	sigs := map[string]string{"wrong element": "as", "bad nested variant": "av"}
	for name, f := range cases {
		sig := sigs[name]
		if sig == "" {
			sig = "s"
		}
		var e Encoder
		e.begin(TypeSignal, 1, 0, "", "/a", "a.b", "C", "", sig)
		f(&e)
		if _, err := e.finish(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
}

func TestParseRejects(t *testing.T) {
	var e Encoder
	good := append([]byte(nil), build(&e)...)
	mutate := map[string]func(b []byte) []byte{
		"bad endian":   func(b []byte) []byte { b[0] = 'x'; return b },
		"bad version":  func(b []byte) []byte { b[3] = 2; return b },
		"zero serial":  func(b []byte) []byte { b[8], b[9], b[10], b[11] = 0, 0, 0, 0; return b },
		"short body":   func(b []byte) []byte { return b[:len(b)-1] },
		"huge body":    func(b []byte) []byte { b[7] = 0xff; return b },
		"no member":    func(b []byte) []byte { return dropField(b, fieldMember) },
		"bad padding":  func(b []byte) []byte { return dirtyPadding(b) },
		"no signature": func(b []byte) []byte { return dropField(b, fieldSignature) },
	}
	for name, f := range mutate {
		b := f(append([]byte(nil), good...))
		var m Message
		n, err := frameLen(b)
		if err == nil && n == len(b) {
			err = parse(b, &m)
		} else if err == nil {
			err = ErrMalformed
		}
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// dropField renames the header field code to an unknown one, so parse
// ignores it.
func dropField(b []byte, code byte) []byte {
	for i := 16; i < len(b)-1; i += 8 {
		if b[i] == code && b[i+1] == 1 {
			b[i] = 200
			return b
		}
	}
	return b
}

// dirtyPadding sets the last padding byte before the body to a non-zero
// value.
func dirtyPadding(b []byte) []byte {
	fields := int(b[12]) | int(b[13])<<8
	if end := headerLen + fields; end%8 != 0 {
		b[pad(end, 8)-1] = 1
	}
	return b
}

func TestReaderStickyErrors(t *testing.T) {
	r := Reader{buf: []byte{1, 0, 0}}
	if r.Uint32() != 0 || r.Err() == nil {
		t.Fatal("short read should fail")
	}
	if r.Byte() != 0 || r.Str() != "" {
		t.Fatal("reads after an error should return zero values")
	}
	// A string without its NUL.
	r = Reader{buf: []byte{1, 0, 0, 0, 'a', 'b'}}
	if r.Str() != "" || r.Err() == nil {
		t.Fatal("string without NUL should fail")
	}
	// An array longer than the buffer.
	r = Reader{buf: []byte{0xff, 0, 0, 0}}
	end := r.Array('y')
	if r.More(end) || r.Err() == nil {
		t.Fatal("oversized array should fail")
	}
}

func TestEncodeParseNoAllocs(t *testing.T) {
	var e Encoder
	var m Message
	build(&e) // grow the buffer once
	allocs := testing.AllocsPerRun(100, func() {
		b := build(&e)
		if err := parse(b, &m); err != nil {
			t.Fatal(err)
		}
		r := m.Body()
		r.Skip(m.Signature)
		if !r.Done() {
			t.Fatal(r.Err())
		}
	})
	if allocs != 0 {
		t.Fatalf("%v allocations per message, want 0", allocs)
	}
}

func FuzzParse(f *testing.F) {
	var e Encoder
	f.Add(append([]byte(nil), build(&e)...))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) < headerLen {
			return
		}
		n, err := frameLen(b)
		if err != nil || n > len(b) {
			return
		}
		var m Message
		if parse(b[:n], &m) != nil {
			return
		}
		r := m.Body()
		r.Skip(m.Signature)
	})
}

// FuzzCheck checks that every body the encoder accepts parses back with
// its signature.
func FuzzCheck(f *testing.F) {
	f.Add("a{sv}", []byte{8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 'a', 0, 1, 'u', 0, 0, 0, 0, 7, 0, 0, 0})
	f.Add("as", []byte{4, 0, 0, 0, 77, 0, 0, 0})
	f.Fuzz(func(t *testing.T, sig string, body []byte) {
		if !validSignature(sig) {
			return
		}
		var e Encoder
		e.begin(TypeSignal, 1, 0, "", "/a", "a.b", "C", "", sig)
		e.buf = append(e.buf, body...)
		b, err := e.finish()
		if err != nil {
			return
		}
		var m Message
		if err := parse(b, &m); err != nil {
			t.Fatalf("accepted body does not parse: %v", err)
		}
		r := m.Body()
		r.check(m.Signature)
		if !r.Done() {
			t.Fatalf("accepted body does not match its signature: %v", r.Err())
		}
	})
}

func FuzzSkip(f *testing.F) {
	f.Add("a{sv}", []byte{8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 'a', 0, 1, 'u', 0, 0, 0, 0, 7, 0, 0, 0})
	f.Add("(v)", []byte{1, 'v', 0})
	f.Fuzz(func(t *testing.T, sig string, b []byte) {
		if !validSignature(sig) {
			return
		}
		r := Reader{buf: b}
		r.Skip(sig)
	})
}

func BenchmarkEncodeParse(b *testing.B) {
	var e Encoder
	var m Message
	b.ReportAllocs()
	for b.Loop() {
		buf := build(&e)
		if err := parse(buf, &m); err != nil {
			b.Fatal(err)
		}
	}
}
