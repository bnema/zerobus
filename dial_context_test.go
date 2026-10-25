package zerobus

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDialContextStalled(t *testing.T) {
	for _, stage := range []string{"auth", "negotiate", "hello"} {
		for _, deadline := range []bool{false, true} {
			t.Run(stage+map[bool]string{false: "/cancel", true: "/deadline"}[deadline], func(t *testing.T) {
				if stage == "negotiate" && !fdSupported {
					t.Skip("no descriptor negotiation")
				}
				// Warm the runtime poller before counting descriptors.
				sock := filepath.Join(t.TempDir(), "bus")
				l, err := net.Listen("unix", sock)
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				before, countErr := os.ReadDir("/proc/self/fd")
				ready := make(chan struct{})
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					c, err := l.Accept()
					if err != nil {
						close(ready)
						return
					}
					defer c.Close()
					_ = c.SetDeadline(time.Now().Add(3 * time.Second))
					r := bufio.NewReader(c)
					if stage != "auth" {
						_, _ = r.ReadString('\n')
						_, _ = c.Write([]byte("OK 0123456789abcdef\r\n"))
						if stage == "hello" {
							if fdSupported {
								_, _ = r.ReadString('\n')
								_, _ = c.Write([]byte("ERROR unsupported\r\n"))
							}
							_, _ = r.ReadString('\n') // BEGIN
							var header [16]byte
							_, _ = io.ReadFull(r, header[:]) // Hello has been sent
						}
					}
					close(ready)
					_, _ = io.Copy(io.Discard, r)
				}()
				ctx, cancel := context.WithCancel(context.Background())
				want := context.Canceled
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
					want = context.DeadlineExceeded
				}
				defer cancel()
				result := make(chan error, 1)
				go func() {
					c, err := DialContext(ctx, "unix:path="+sock)
					if c != nil {
						c.Close()
					}
					result <- err
				}()
				select {
				case <-ready:
				case <-time.After(2 * time.Second):
					t.Fatal("peer not ready")
				}
				if !deadline {
					cancel()
				}
				select {
				case err := <-result:
					if !errors.Is(err, want) {
						t.Fatalf("got %v, want %v", err, want)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("DialContext did not unblock")
				}
				<-finished
				if countErr == nil {
					after, err := os.ReadDir("/proc/self/fd")
					if err != nil {
						t.Fatal(err)
					}
					if len(after) != len(before) {
						t.Fatalf("descriptor count: before %d, after %d", len(before), len(after))
					}
				}
			})
		}
	}
}

func TestDialContextSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c, err := DialContext(ctx, fakeBus(t, "OK 0123456789abcdef\r\n", signal("After")))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.UniqueName() != ":1.7" {
		t.Fatal(c.UniqueName())
	}
	<-ctx.Done() // Neither cancellation nor the original deadline affects the Conn.
	m, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m.Member != "After" {
		t.Fatal(m.Member)
	}
}

func TestBusContextEnvironment(t *testing.T) {
	for _, bus := range []struct {
		env  string
		dial func(context.Context) (*Conn, error)
	}{
		{"DBUS_SESSION_BUS_ADDRESS", SessionBusContext},
		{"DBUS_SYSTEM_BUS_ADDRESS", SystemBusContext},
	} {
		t.Run(bus.env, func(t *testing.T) {
			t.Setenv(bus.env, fakeBus(t, "OK 0123456789abcdef\r\n"))
			c, err := bus.dial(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			c.Close()
		})
	}
}
