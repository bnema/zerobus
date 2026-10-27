//go:build linux

package zerobus

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

func pairDispatcher(t *testing.T) (*Dispatcher, *Conn) {
	t.Helper()
	a, b := fdPair(t)
	d := NewDispatcher(a)
	t.Cleanup(func() { _ = d.Close() })
	return d, b
}
func pairCall(t *testing.T, d *Dispatcher, b *Conn) (*Pending, *Message) {
	t.Helper()
	d.NewCall("", "/", "org.example.Test", "Call", "")
	p, err := d.Go()
	if err != nil {
		t.Fatal(err)
	}
	m, err := b.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	return p, m
}
func pairBarrier(t *testing.T, d *Dispatcher, b *Conn) {
	t.Helper()
	p, m := pairCall(t, d, b)
	b.NewReply(m, "")
	if _, err := b.Send(); err != nil {
		t.Fatal(err)
	}
	waitDone(t, p.Done())
	reply, err := p.Result()
	if err != nil {
		t.Fatal(err)
	}
	reply.Close()
}
func pipeFD(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	return r, w
}
func fdCount(t *testing.T) int {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(fds)
}
func TestDispatcherFDReply(t *testing.T) {
	for _, take := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "take"}[take], func(t *testing.T) {
			d, b := pairDispatcher(t)
			r, w := pipeFD(t)
			baseline := fdCount(t)
			p, m := pairCall(t, d, b)
			b.NewReply(m, "h").UnixFD(int(r.Fd()))
			if _, err := b.Send(); err != nil {
				t.Fatal(err)
			}
			waitDone(t, p.Done())
			msg, err := p.Result()
			if err != nil {
				t.Fatal(err)
			}
			if msg.NumFDs() != 1 {
				t.Fatal(msg.NumFDs())
			}
			if take {
				f, err := msg.TakeFD(msg.Body().UnixFD())
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if _, err := msg.TakeFD(0); !errors.Is(err, ErrInvalid) {
					t.Fatal(err)
				}
				flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_GETFD, 0)
				if errno != 0 || flags&syscall.FD_CLOEXEC == 0 {
					t.Fatal("missing CLOEXEC")
				}
				msg.Close()
				if _, err := w.Write([]byte("x")); err != nil {
					t.Fatal(err)
				}
				var data [1]byte
				if _, err := f.Read(data[:]); err != nil || data[0] != 'x' {
					t.Fatal(err)
				}
				_ = f.Close()
			} else {
				msg.Close()
				msg.Close()
			}
			pairBarrier(t, d, b)
			if got := fdCount(t); got != baseline {
				t.Fatalf("fds %d want %d", got, baseline)
			}
		})
	}
}
func TestDispatcherFDCancel(t *testing.T) {
	for _, ordering := range []string{"reply_wins", "cancel_wins", "race"} {
		t.Run(ordering, func(t *testing.T) {
			d, b := pairDispatcher(t)
			r, _ := pipeFD(t)
			baseline := fdCount(t)
			for range 50 {
				p, m := pairCall(t, d, b)
				var wg sync.WaitGroup
				if ordering == "cancel_wins" {
					p.Cancel()
				} else if ordering == "race" {
					wg.Go(p.Cancel)
				}
				b.NewReply(m, "h").UnixFD(int(r.Fd()))
				if _, err := b.Send(); err != nil {
					t.Fatal(err)
				}
				if ordering == "reply_wins" {
					waitDone(t, p.Done())
				}
				wg.Wait()
				p.Cancel()
				msg, err := p.Result()
				if msg != nil || !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				pairBarrier(t, d, b)
			}
			if got := fdCount(t); got != baseline {
				t.Fatalf("fds %d want %d", got, baseline)
			}
		})
	}
}
func TestDispatcherFDSignal(t *testing.T) {
	d, b := pairDispatcher(t)
	r, _ := pipeFD(t)
	baseline := fdCount(t)
	// Install directly: the socketpair has no bus to answer AddMatch.
	s := &Subscription{d: d, ch: make(chan *Msg, 1)}
	s.C = s.ch
	d.smu.Lock()
	d.subs = append(d.subs, s)
	d.smu.Unlock()
	b.NewSignal("/", "org.example.Test", "Changed", "h").UnixFD(int(r.Fd()))
	if _, err := b.Send(); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-s.C:
		if m.NumFDs() != 0 {
			t.Fatal("signal owns fd")
		}
		m.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
	pairBarrier(t, d, b)
	if got := fdCount(t); got != baseline {
		t.Fatalf("fds %d want %d", got, baseline)
	}
}
func TestDispatcherCloseQueuedFD(t *testing.T) {
	a, b := fdPair(t)
	r, _ := pipeFD(t)
	fd, err := syscall.Dup(int(r.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	a.fdQueue = append(a.fdQueue, fd)
	d := NewDispatcher(a)
	_ = d.Close()
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != syscall.EBADF {
		t.Fatalf("queued fd remains open: %v", err)
	}
	_ = b.Close()
}
func TestDispatcherCallContext(t *testing.T) {
	d, b := pairDispatcher(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		d.NewCall("", "/", "org.example.Test", "Slow", "")
		m, err := d.Call(ctx)
		m.Close()
		result <- err
	}()
	request, err := b.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	b.NewReply(request, "")
	if _, err := b.Send(); err != nil {
		t.Fatal(err)
	}
	pairBarrier(t, d, b)
	d.pmu.Lock()
	defer d.pmu.Unlock()
	if len(d.pending) != 0 {
		t.Fatal("pending calls")
	}
}
func TestDispatcherCloseFailsPending(t *testing.T) {
	baseline := runtime.NumGoroutine()
	d, b := pairDispatcher(t)
	var calls []*Pending
	for range 3 {
		p, _ := pairCall(t, d, b)
		calls = append(calls, p)
	}
	_ = d.Close()
	for _, p := range calls {
		waitDone(t, p.Done())
		m, err := p.Result()
		if m != nil || !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	}
	// Close joins the sole dispatcher goroutine; Done is stronger than a
	// process-global goroutine count in a test suite with runtime goroutines.
	waitDone(t, d.Done())
	if runtime.NumGoroutine() > baseline+1 {
		t.Fatal("reader goroutine leaked")
	}
}
func TestDispatcherMalformedFailsWaiters(t *testing.T) {
	d, b := pairDispatcher(t)
	p, _ := pairCall(t, d, b)
	s := &Subscription{d: d, ch: make(chan *Msg, 1)}
	s.C = s.ch
	d.smu.Lock()
	d.subs = append(d.subs, s)
	d.smu.Unlock()
	bad := make([]byte, headerLen)
	bad[0] = 'x'
	if _, err := b.c.Write(bad); err != nil {
		t.Fatal(err)
	}
	waitDone(t, d.Done())
	waitDone(t, p.Done())
	m, err := p.Result()
	if m != nil || !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	if _, ok := <-s.C; ok {
		t.Fatal("subscription open")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
