package zerobus

import (
	"bufio"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
)

// fakeBus is a minimal bus on a Unix socket that answers the handshake and
// Hello, then writes the given frames to the client one byte at a time, so
// every message is split across reads.
func fakeBus(t *testing.T, authReply string, frames ...[]byte) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "bus")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		r := bufio.NewReader(c)
		if _, err := r.ReadString('\n'); err != nil { // \0AUTH EXTERNAL ...
			return
		}
		var hello Encoder
		hello.begin(TypeMethodReturn, 1, 1, "", "", "", "", "", "s")
		hello.Str(":1.7")
		b, _ := hello.finish()
		// The OK line and the Hello reply arrive in the same write: the
		// client must keep the bytes after the line.
		if _, err := c.Write(append([]byte(authReply), b...)); err != nil {
			return
		}
		if _, err := r.ReadString('\n'); err != nil { // BEGIN
			return
		}
		for _, f := range frames {
			for i := range f {
				if _, err := c.Write(f[i : i+1]); err != nil {
					return
				}
			}
		}
		_, _ = r.ReadByte() // wait for the client to close
	}()
	return "unix:path=" + sock
}

func signal(member string) []byte {
	var e Encoder
	e.begin(TypeSignal, 2, 0, "", "/a", "a.b", member, "", "s")
	e.Str(member)
	b, _ := e.finish()
	return append([]byte(nil), b...)
}

func TestFakeBusSplitFrames(t *testing.T) {
	addr := fakeBus(t, "OK 0123456789abcdef\r\n", signal("One"), signal("Two"))
	c := dial(t, addr)
	if c.UniqueName() != ":1.7" {
		t.Fatalf("unique name %q", c.UniqueName())
	}
	for _, want := range []string{"One", "Two"} {
		m, err := c.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if m.Member != want || m.Body().Str() != want {
			t.Fatalf("got %q, want %q", m.Member, want)
		}
	}
}

func TestFakeBusRejected(t *testing.T) {
	addr := fakeBus(t, "REJECTED EXTERNAL\r\n")
	if c, err := Dial(addr); err == nil {
		_ = c.Close()
		t.Fatal("rejected auth accepted")
	}
}

func TestFakeBusMalformedIsSticky(t *testing.T) {
	bad := signal("Bad")
	bad[3] = 9 // protocol version
	c := dial(t, fakeBus(t, "OK 0123456789abcdef\r\n", bad))
	_, err := c.ReadMessage()
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed", err)
	}
	if _, err := c.ReadMessage(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("second read: %v, want the same error", err)
	}
}

func TestSessionBusFromRuntimeDir(t *testing.T) {
	addr := fakeBus(t, "OK 0123456789abcdef\r\n")
	t.Setenv("DBUS_SESSION_BUS_ADDRESS", "")
	t.Setenv("XDG_RUNTIME_DIR", filepath.Dir(strings.TrimPrefix(addr, "unix:path=")))
	c, err := SessionBus()
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
}
