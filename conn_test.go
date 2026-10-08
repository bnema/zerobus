package zerobus

import (
	"bufio"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// privateBus starts a dbus-daemon for the test and returns its address. The
// test is skipped when dbus-daemon is not installed.
func privateBus(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("dbus-daemon")
	if err != nil {
		t.Skip("dbus-daemon not installed")
	}
	cmd := exec.Command(path, "--session", "--nofork", "--print-address")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	line := make(chan string, 1)
	go func() {
		s, _ := bufio.NewReader(out).ReadString('\n')
		line <- strings.TrimSpace(s)
	}()
	select {
	case addr := <-line:
		if addr == "" {
			t.Fatal("dbus-daemon printed no address")
		}
		return addr
	case <-time.After(5 * time.Second):
		t.Fatal("dbus-daemon did not start")
	}
	return ""
}

func dial(t *testing.T, addr string) *Conn {
	t.Helper()
	c, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestHelloAndListNames(t *testing.T) {
	c := dial(t, privateBus(t))
	if !strings.HasPrefix(c.UniqueName(), ":") {
		t.Fatalf("unique name %q", c.UniqueName())
	}
	c.NewCall(busName, busPath, busName, "ListNames", "")
	m, err := c.Call()
	if err != nil {
		t.Fatal(err)
	}
	r := m.Body()
	found := false
	end := r.Array('s')
	for r.More(end) {
		if r.Str() == c.UniqueName() {
			found = true
		}
	}
	if !r.Done() || !found {
		t.Fatalf("own name not listed: %v", r.Err())
	}
}

func TestErrorReply(t *testing.T) {
	c := dial(t, privateBus(t))
	c.NewCall("org.example.Missing", "/", "org.example.I", "M", "").SetFlags(FlagNoAutoStart)
	_, err := c.Call()
	var de *Error
	// ServiceUnknown or NameHasNoOwner, depending on the bus.
	if !errors.As(err, &de) || !strings.HasPrefix(de.Name, "org.freedesktop.DBus.Error.") || de.Message == "" {
		t.Fatalf("err = %v", err)
	}
	// The connection stays usable after an error reply.
	if _, err := c.RequestName("org.example.Zerobus", NameFlagDoNotQueue); err != nil {
		t.Fatal(err)
	}
}

func TestSignalAndMethodBetweenPeers(t *testing.T) {
	addr := privateBus(t)
	server := dial(t, addr)
	client := dial(t, addr)

	if code, err := server.RequestName("org.example.Echo", NameFlagDoNotQueue); err != nil || code != NamePrimaryOwner {
		t.Fatalf("RequestName = %d, %v", code, err)
	}
	if err := client.AddMatch("type='signal',interface='org.example.Echo'"); err != nil {
		t.Fatal(err)
	}

	// The server answers one Echo call, then emits a signal.
	done := make(chan error, 1)
	go func() {
		for {
			m, err := server.ReadMessage()
			if err != nil {
				done <- err
				return
			}
			if m.Type != TypeMethodCall || m.Member != "Echo" {
				continue // NameAcquired and similar
			}
			s := m.Body().Str()
			server.NewReply(m, "s").Str(strings.ToUpper(s))
			if _, err := server.Send(); err != nil {
				done <- err
				return
			}
			server.NewSignal("/org/example/Echo", "org.example.Echo", "Echoed", "s").Str(s)
			_, err = server.Send()
			done <- err
			return
		}
	}()

	client.NewCall("org.example.Echo", "/org/example/Echo", "org.example.Echo", "Echo", "s").Str("hi")
	m, err := client.Call()
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Body().Str(); got != "HI" {
		t.Fatalf("reply %q", got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for {
		m, err := client.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if m.Type == TypeSignal && m.Member == "Echoed" {
			if s := m.Body().Str(); s != "hi" || m.Sender != server.UniqueName() {
				t.Fatalf("signal %q from %q", s, m.Sender)
			}
			return
		}
	}
}

func TestCloseUnblocksRead(t *testing.T) {
	c := dial(t, privateBus(t))
	errc := make(chan error, 1)
	go func() {
		for {
			if _, err := c.ReadMessage(); err != nil {
				errc <- err
				return
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadMessage did not return after Close")
	}
}

func TestCallNoAllocs(t *testing.T) {
	c := dial(t, privateBus(t))
	call := func() {
		c.NewCall(busName, busPath, busName, "GetNameOwner", "s").Str(busName)
		m, err := c.Call()
		if err != nil {
			t.Fatal(err)
		}
		if m.Body().Str() == "" {
			t.Fatal("empty owner")
		}
	}
	call() // grow the buffers once
	if allocs := testing.AllocsPerRun(200, call); allocs != 0 {
		t.Fatalf("%v allocations per call, want 0", allocs)
	}
}

func TestLargeMessageGrowsBuffer(t *testing.T) {
	addr := privateBus(t)
	a := dial(t, addr)
	b := dial(t, addr)
	if err := b.AddMatch("type='signal',interface='org.example.Big'"); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 3*defaultBufSz)
	for i := range payload {
		payload[i] = byte(i)
	}
	a.NewSignal("/a", "org.example.Big", "Blob", "ay").ByteArray(payload)
	if _, err := a.Send(); err != nil {
		t.Fatal(err)
	}
	for {
		m, err := b.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if m.Member == "Blob" {
			got := m.Body().ByteArray()
			if len(got) != len(payload) || got[len(got)-1] != payload[len(payload)-1] {
				t.Fatalf("payload of %d bytes", len(got))
			}
			return
		}
	}
}

func TestDialErrors(t *testing.T) {
	for _, addr := range []string{"", "tcp:host=localhost,port=1", "unix:guid=abc", "unix:path=/nonexistent/zerobus"} {
		if c, err := Dial(addr); err == nil {
			_ = c.Close()
			t.Errorf("Dial(%q) succeeded", addr)
		}
	}
}

func TestUnescape(t *testing.T) {
	got, err := unescape("/tmp/a%20b%2c")
	if err != nil || got != "/tmp/a b," {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := unescape("%zz"); err == nil {
		t.Fatal("bad escape accepted")
	}
}
