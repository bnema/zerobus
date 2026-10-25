//go:build linux

package zerobus

import (
	"bufio"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func fdPair(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a, err := net.DialUnix("unix", nil, l.Addr().(*net.UnixAddr))
	if err != nil {
		t.Fatal(err)
	}
	b, err := l.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	x := &Conn{c: a, rbuf: make([]byte, defaultBufSz), unixFDs: true}
	y := &Conn{c: b, rbuf: make([]byte, defaultBufSz), unixFDs: true}
	t.Cleanup(func() { _ = x.Close(); _ = y.Close() })
	return x, y
}

func TestFDReplyAndSend(t *testing.T) {
	a, b := fdPair(t)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	a.NewReply(&Message{Serial: 1}, "h").UnixFD(int(r.Fd()))
	if _, err := a.Send(); err != nil {
		t.Fatal(err)
	}
	m, err := b.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m.NumFDs() != 1 {
		t.Fatal(m.NumFDs())
	}
	f, err := m.TakeFD(m.Body().UnixFD())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := m.TakeFD(0); err == nil {
		t.Fatal("second take succeeded")
	}
	if _, err := m.TakeFD(1); err == nil {
		t.Fatal("out of range take succeeded")
	}
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_GETFD, 0)
	if errno != 0 || flags&syscall.FD_CLOEXEC == 0 {
		t.Fatal("missing close-on-exec")
	}
	if _, err := w.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var buf [1]byte
	if _, err := f.Read(buf[:]); err != nil || buf[0] != 'x' {
		t.Fatalf("read %q: %v", buf, err)
	}
}

func TestFDRelease(t *testing.T) {
	for _, closeConn := range []bool{false, true} {
		t.Run(map[bool]string{false: "next-read", true: "close"}[closeConn], func(t *testing.T) {
			a, b := fdPair(t)
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			defer w.Close()
			before, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Fatal(err)
			}
			a.NewReply(&Message{Serial: 1}, "h").UnixFD(int(r.Fd()))
			if _, err := a.Send(); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ReadMessage(); err != nil {
				t.Fatal(err)
			}
			after, _ := os.ReadDir("/proc/self/fd")
			if len(after) != len(before)+1 {
				t.Fatalf("fd count %d -> %d", len(before), len(after))
			}
			if closeConn {
				_ = b.Close()
			} else {
				a.NewSignal("/a", "a.b", "Next", "")
				if _, err := a.Send(); err != nil {
					t.Fatal(err)
				}
				if _, err := b.ReadMessage(); err != nil {
					t.Fatal(err)
				}
			}
			after, _ = os.ReadDir("/proc/self/fd")
			want := len(before)
			if closeConn {
				want--
			}
			if len(after) != want {
				t.Fatalf("remaining fds %d want %d", len(after), want)
			}
		})
	}
}

func TestFDShortQueueSticky(t *testing.T) {
	a, b := fdPair(t)
	a.NewReply(&Message{Serial: 1}, "h").UnixFD(0)
	frame, err := a.enc.finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.c.Write(frame); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := b.ReadMessage(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%v", err)
		}
	}
}

func TestFDNotNegotiated(t *testing.T) {
	a, _ := fdPair(t)
	a.unixFDs = false
	a.NewReply(&Message{Serial: 1}, "h").UnixFD(0)
	if _, err := a.Send(); err == nil {
		t.Fatal("send succeeded")
	}
}

func TestFDNegotiation(t *testing.T) {
	c := dial(t, privateBus(t))
	if !c.UnixFDs() {
		t.Fatal("bus did not agree")
	}
	c = dial(t, fakeBus(t, "OK 0123456789abcdef\r\n"))
	if c.UnixFDs() {
		t.Fatal("ERROR enabled fds")
	}
}

func TestFDCTruncSticky(t *testing.T) {
	a, b := fdPair(t)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	before := openFDCount(t)
	fds := make([]int, 253)
	for i := range fds {
		fds[i] = int(r.Fd())
	}
	if _, _, err := a.c.WriteMsgUnix(signal("Truncated"), syscall.UnixRights(fds...), nil); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := b.ReadMessage(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%v", err)
		}
	}
	if after := openFDCount(t); after != before {
		t.Fatalf("CTRUNC leaked fds: %d -> %d", before, after)
	}
}

func TestFDCloseDuringRead(t *testing.T) {
	a, b := fdPair(t)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	a.NewReply(&Message{Serial: 1}, "h").UnixFD(int(r.Fd()))
	if _, err := a.Send(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := b.ReadMessage(); done <- err }()
	_ = b.Close()
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatalf("%v", err)
	}
}

func TestFDReadAheadFIFO(t *testing.T) {
	a, b := fdPair(t)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	// A plain message preceding the rights-bearing message must not consume it.
	if _, err := a.c.Write(signal("Before")); err != nil {
		t.Fatal(err)
	}
	a.NewReply(&Message{Serial: 1}, "h").UnixFD(int(r.Fd()))
	if _, err := a.Send(); err != nil {
		t.Fatal(err)
	}
	m, err := b.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m.NumFDs() != 0 {
		t.Fatal("preceding message consumed fd")
	}
	m, err = b.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	f, err := m.TakeFD(0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
}

func openFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestFDUnnegotiatedRightsNoHeader(t *testing.T) {
	a, b := fdPair(t)
	b.unixFDs = false
	// Run the real handshake, with the peer explicitly rejecting negotiation.
	done := make(chan error, 1)
	go func() {
		r := bufio.NewReader(a.c)
		for _, reply := range []string{"OK 0123456789abcdef\r\n", "ERROR unsupported\r\n"} {
			if _, err := r.ReadString('\n'); err != nil {
				done <- err
				return
			}
			if _, err := a.c.Write([]byte(reply)); err != nil {
				done <- err
				return
			}
		}
		_, err := r.ReadString('\n')
		done <- err
	}()
	if err := b.auth(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b.UnixFDs() {
		t.Fatal("negotiated despite ERROR")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	before := openFDCount(t)
	if _, _, err := a.c.WriteMsgUnix(signal("NoHeader"), syscall.UnixRights(int(r.Fd())), nil); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := b.ReadMessage(); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%v", err)
		}
	}
	if after := openFDCount(t); after != before {
		t.Fatalf("fd count %d -> %d", before, after)
	}
}

func TestFDQueuedDistinct(t *testing.T) {
	a, b := fdPair(t)
	var reads [2]*os.File
	for i := range reads {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		reads[i] = r
		defer r.Close()
		if _, err := w.Write([]byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
		_ = w.Close()
		a.NewReply(&Message{Serial: 1}, "h").UnixFD(int(r.Fd()))
		if _, err := a.Send(); err != nil {
			t.Fatal(err)
		}
	}
	// Fill without parsing so both rights-bearing messages are queued first.
	if err := b.fill(16); err != nil {
		t.Fatal(err)
	}
	first, err := frameLen(b.rbuf[b.start:b.end])
	if err != nil {
		t.Fatal(err)
	}
	if err := b.fill(first + 16); err != nil {
		t.Fatal(err)
	}
	b.fdMu.Lock()
	queued := len(b.fdQueue)
	b.fdMu.Unlock()
	if queued != 2 {
		t.Fatalf("queued %d fds", queued)
	}
	for i := range reads {
		m, err := b.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		f, err := m.TakeFD(m.Body().UnixFD())
		if err != nil {
			t.Fatal(err)
		}
		var data [1]byte
		_, err = f.Read(data[:])
		_ = f.Close()
		if err != nil || data[0] != byte('a'+i) {
			t.Fatalf("message %d: %q, %v", i, data, err)
		}
	}
}
