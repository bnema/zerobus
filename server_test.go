package zerobus

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// socketPair returns a Conn whose socket is the client end of a Unix socket,
// without the bus handshake, and the other end to write raw frames to it.
func socketPair(t *testing.T) (*Conn, net.Conn) {
	t.Helper()
	l, err := net.Listen("unix", filepath.Join(t.TempDir(), "s"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	uc, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: l.Addr().String(), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = uc.Close(); _ = peer.Close() })
	return &Conn{c: uc, rbuf: make([]byte, defaultBufSz)}, peer
}

func TestNewSignalToSetsDestination(t *testing.T) {
	var c Conn
	c.NewSignalTo(":1.42", "/a", "a.b", "Hit", "s").Str("x")
	b, err := c.enc.finish()
	if err != nil {
		t.Fatal(err)
	}
	var m Message
	if err := parse(b, &m); err != nil {
		t.Fatal(err)
	}
	if m.Type != TypeSignal || m.Destination != ":1.42" || m.Path != "/a" || m.Interface != "a.b" || m.Member != "Hit" {
		t.Fatalf("message %+v", m)
	}
	if got := m.Body().Str(); got != "x" {
		t.Fatalf("body %q", got)
	}

	c.NewSignal("/a", "a.b", "Hit", "")
	b, err = c.enc.finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := parse(b, &m); err != nil || m.Destination != "" {
		t.Fatalf("broadcast signal has destination %q (%v)", m.Destination, err)
	}

	c.NewSignalTo("not a name", "/a", "a.b", "Hit", "")
	if _, err := c.Send(); err == nil {
		t.Fatal("invalid destination accepted")
	}

	c.NewSignalTo("", "/a", "a.b", "Hit", "")
	if _, err := c.Send(); err == nil {
		t.Fatal("directed signal without destination accepted")
	}
}

func TestDirectedSignalReachesOnlyDestination(t *testing.T) {
	addr := privateBus(t)
	server := dial(t, addr)
	target := dial(t, addr)
	other := dial(t, addr)
	const rule = "type='signal',interface='org.example.Directed'"
	for _, c := range []*Conn{target, other} {
		if err := c.AddMatch(rule); err != nil {
			t.Fatal(err)
		}
	}

	server.NewSignalTo(target.UniqueName(), "/a", "org.example.Directed", "Hit", "u").Uint32(7)
	if _, err := server.Send(); err != nil {
		t.Fatal(err)
	}

	// The bus routes in order: a signal for other would come before the
	// reply to its round trip.
	var leaked bool
	other.Handle = func(m *Message) { leaked = leaked || m.Member == "Hit" }
	server.NewCall(busName, busPath, busName, "GetId", "")
	if _, err := server.Call(); err != nil {
		t.Fatal(err)
	}
	other.NewCall(busName, busPath, busName, "GetId", "")
	if _, err := other.Call(); err != nil {
		t.Fatal(err)
	}
	if leaked {
		t.Fatal("directed signal reached another peer")
	}

	for {
		m, err := target.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if m.Member != "Hit" {
			continue
		}
		if m.Type != TypeSignal || m.Sender != server.UniqueName() || m.Destination != target.UniqueName() || m.Body().Uint32() != 7 {
			t.Fatalf("message %+v", m)
		}
		return
	}
}

func TestAsyncMethodCallMatching(t *testing.T) {
	addr := privateBus(t)
	c := dial(t, addr)
	other := dial(t, addr)

	// Two requests are in flight at once; the loop only uses Send and
	// ReadMessage and tells the replies apart by their serial.
	want := map[uint32]string{}
	for _, name := range []string{c.UniqueName(), other.UniqueName(), "org.example.Nobody"} {
		e, serial := c.NewMethodCall(busName, busPath, busName, "GetNameOwner", "s")
		e.Str(name)
		sent, err := c.Send()
		if err != nil {
			t.Fatal(err)
		}
		if sent != serial {
			t.Fatalf("NewMethodCall serial %d, Send serial %d", serial, sent)
		}
		if _, dup := want[serial]; dup {
			t.Fatalf("serial %d reused", serial)
		}
		want[serial] = name
	}

	for len(want) > 0 {
		m, err := c.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if m.Type != TypeMethodReturn && m.Type != TypeError {
			continue // NameAcquired
		}
		name, ok := want[m.ReplySerial]
		if !ok {
			t.Fatalf("unexpected reply to serial %d", m.ReplySerial)
		}
		delete(want, m.ReplySerial)
		switch name {
		case "org.example.Nobody":
			if m.Type != TypeError {
				t.Fatalf("reply for %q is %d, want an error", name, m.Type)
			}
		default:
			if m.Type != TypeMethodReturn || m.Body().Str() != name {
				t.Fatalf("reply for %q is %d", name, m.Type)
			}
		}
	}
}

func TestBufferedAndPollLoop(t *testing.T) {
	c, peer := socketPair(t)
	if c.Buffered() {
		t.Fatal("empty connection is buffered")
	}
	two := append(signal("One"), signal("Two")...)
	three := signal("Three")
	// Two whole frames and the start of a third arrive in one write.
	if _, err := peer.Write(append(two, three[:10]...)); err != nil {
		t.Fatal(err)
	}
	waitReadable(t, c)
	for _, want := range []string{"One", "Two"} {
		m, err := c.ReadMessage()
		if err != nil || m.Member != want {
			t.Fatalf("got %v, %v; want %q", m, err, want)
		}
		if got, wantBuf := c.Buffered(), want == "One"; got != wantBuf {
			t.Fatalf("Buffered after %q = %v, want %v", want, got, wantBuf)
		}
	}
	// The head of a third frame is not a message.
	if c.Buffered() {
		t.Fatal("a partial frame counts as buffered")
	}
	if _, err := peer.Write(three[10:]); err != nil {
		t.Fatal(err)
	}
	waitReadable(t, c)
	m, err := c.ReadMessage()
	if err != nil || m.Member != "Three" {
		t.Fatalf("got %v, %v", m, err)
	}
	if c.Buffered() {
		t.Fatal("drained connection is buffered")
	}
}

func TestBufferedReportsStickyError(t *testing.T) {
	c, peer := socketPair(t)
	bad := signal("Bad")
	bad[3] = 9 // protocol version
	if _, err := peer.Write(append(bad, signal("Next")...)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadMessage(); err == nil {
		t.Fatal("malformed frame accepted")
	}
	if !c.Buffered() {
		t.Fatal("a sticky error must not make a poll loop wait")
	}
}

// waitReadable blocks until the socket of c is readable, as a poll loop does.
func waitReadable(t *testing.T, c *Conn) {
	t.Helper()
	rc, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fd int
	if err := rc.Control(func(f uintptr) { fd = int(f) }); err != nil {
		t.Fatal(err)
	}
	if err := waitFd(fd, 5*time.Second); errors.Is(err, errNoWaitFd) {
		t.Skip(err)
	} else if err != nil {
		t.Fatal(err)
	}
}

func TestSignalAllocations(t *testing.T) {
	c := dial(t, privateBus(t))
	send := func() {
		e := c.NewSignalTo(":1.1", "/a", "org.example.Alloc", "Hit", "us")
		e.Uint32(1)
		e.Str("x")
		if _, err := c.Send(); err != nil {
			t.Fatal(err)
		}
	}
	send() // grow the buffers once
	if allocs := testing.AllocsPerRun(200, send); allocs != 0 {
		t.Fatalf("%v allocations per directed signal, want 0", allocs)
	}
}

func TestMethodCallAllocations(t *testing.T) {
	c := dial(t, privateBus(t))
	var serial uint32
	call := func() {
		e, s := c.NewMethodCall(busName, busPath, busName, "GetConnectionUnixProcessID", "s")
		e.Str(c.UniqueName())
		sent, err := c.Send()
		if err != nil || sent != s {
			t.Fatalf("Send = %d, %v; want %d", sent, err, s)
		}
		serial = s
	}
	call()
	if allocs := testing.AllocsPerRun(200, call); allocs != 0 {
		t.Fatalf("%v allocations per method call, want 0", allocs)
	}
	// The replies must be read for the test to leave the bus quiet; they
	// also prove the calls were well formed.
	if err := c.c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got := 0
	for got < 202 {
		m, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("after %d of 202 replies: %v", got, err)
		}
		if m.Type == TypeMethodReturn && m.ReplySerial <= serial {
			got++
		}
	}
}

func TestReadMessageAllocations(t *testing.T) {
	addr := privateBus(t)
	t.Run("method call", func(t *testing.T) {
		client, server := dial(t, addr), dial(t, addr)
		ping := func() {
			e := client.NewCall(server.UniqueName(), "/a", "org.example.Alloc", "Ping", "s")
			e.SetFlags(FlagNoReplyExpected)
			e.Str("hello")
			if _, err := client.Send(); err != nil {
				t.Fatal(err)
			}
			for {
				m, err := server.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				if m.Type == TypeMethodCall && m.Member == "Ping" {
					if m.Body().Str() != "hello" {
						t.Fatal("bad body")
					}
					return
				}
			}
		}
		ping() // grow the buffers once
		if allocs := testing.AllocsPerRun(200, ping); allocs != 0 {
			t.Fatalf("%v allocations per method call read, want 0", allocs)
		}
	})
	t.Run("method return", func(t *testing.T) {
		c := dial(t, addr)
		ask := func() {
			e, serial := c.NewMethodCall(busName, busPath, busName, "GetNameOwner", "s")
			e.Str(busName)
			if _, err := c.Send(); err != nil {
				t.Fatal(err)
			}
			for {
				m, err := c.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				if m.Type == TypeMethodReturn && m.ReplySerial == serial {
					if m.Body().Str() == "" {
						t.Fatal("empty owner")
					}
					return
				}
			}
		}
		ask()
		if allocs := testing.AllocsPerRun(200, ask); allocs != 0 {
			t.Fatalf("%v allocations per method return read, want 0", allocs)
		}
	})
}
