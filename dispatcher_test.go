package zerobus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func dispatcher(t *testing.T, addr string) *Dispatcher {
	t.Helper()
	d, err := DialDispatcher(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}
func owner(d *Dispatcher) (*Msg, error) {
	d.NewCall(busName, busPath, busName, "GetNameOwner", "s").Str(busName)
	return d.Call(context.Background())
}
func barrier(t *testing.T, d *Dispatcher) {
	t.Helper()
	m, err := owner(d)
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
}
func waitDone(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestDispatcherConcurrentCalls(t *testing.T) {
	d := dispatcher(t, privateBus(t))
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 100 {
				m, err := owner(d)
				if err != nil {
					t.Error(err)
					return
				}
				r := m.Body()
				if r.Str() != busName || !r.Done() {
					t.Error("wrong owner")
				}
				m.Close()
			}
		})
	}
	wg.Wait()
	_ = d.Close()
	waitDone(t, d.Done())
	if !errors.Is(d.Err(), ErrClosed) {
		t.Fatal(d.Err())
	}
}
func TestDispatcherCallAllocs(t *testing.T) {
	d := dispatcher(t, privateBus(t))
	barrier(t, d)
	n := testing.AllocsPerRun(100, func() {
		m, err := owner(d)
		if err != nil {
			panic(err)
		}
		m.Close()
	})
	t.Logf("Call allocations: %g", n)
	if n != 4 {
		t.Fatalf("Call allocations = %g, want 4", n)
	}
}
func TestDispatcherErrorReply(t *testing.T) {
	d := dispatcher(t, privateBus(t))
	for _, async := range []bool{false, true} {
		d.NewCall(busName, busPath, busName, "MissingMethod", "")
		var m *Msg
		var err error
		if async {
			var p *Pending
			p, err = d.Go()
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, p.Done())
			m, err = p.Result()
		} else {
			m, err = d.Call(context.Background())
		}
		var e *Error
		if !errors.As(err, &e) || e.Name != "org.freedesktop.DBus.Error.UnknownMethod" {
			t.Fatal(err)
		}
		m.Close()
	}
}
func TestDispatcherGoCancelRace(t *testing.T) {
	d := dispatcher(t, privateBus(t))
	for range 100 {
		d.NewCall(busName, busPath, busName, "GetNameOwner", "s").Str(busName)
		p, err := d.Go()
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Go(p.Cancel)
		waitDone(t, p.Done())
		wg.Wait()
		p.Cancel()
		m, err := p.Result()
		if m != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("%v %v", m, err)
		}
	}
	barrier(t, d)
	d.pmu.Lock()
	defer d.pmu.Unlock()
	if len(d.pending) != 0 {
		t.Fatal("pending calls")
	}
}
func TestDispatcherNoReplyExpectedRejected(t *testing.T) {
	d := dispatcher(t, privateBus(t))
	d.NewCall(busName, busPath, busName, "Hello", "").SetFlags(FlagNoReplyExpected)
	if _, err := d.Go(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	d.NewSignal("/", "org.example.Test", "Changed", "")
	if _, err := d.Call(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	barrier(t, d)
}
func TestDispatcherUnknownMethod(t *testing.T) {
	addr := privateBus(t)
	d := dispatcher(t, addr)
	c := dial(t, addr)
	c.NewCall(d.UniqueName(), "/", "org.example.Test", "Missing", "")
	_, err := c.Call()
	var e *Error
	if !errors.As(err, &e) || e.Name != "org.freedesktop.DBus.Error.UnknownMethod" {
		t.Fatal(err)
	}
}
func TestDispatcherSignals(t *testing.T) {
	addr := privateBus(t)
	a, b := dispatcher(t, addr), dispatcher(t, addr)
	ctx := context.Background()
	match := Match{Sender: a.UniqueName(), Path: "/", Interface: "org.example.Test"}
	s, err := b.Subscribe(ctx, match, 8)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := b.Subscribe(ctx, match, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close(ctx)
	for i := uint32(0); i < 3; i++ {
		a.NewSignal("/", match.Interface, "Changed", "u").Uint32(i)
		if _, err := a.Send(); err != nil {
			t.Fatal(err)
		}
	}
	for _, sub := range []*Subscription{s, s2} {
		for i := uint32(0); i < 3; i++ {
			select {
			case m := <-sub.C:
				if m.Body().Uint32() != i {
					t.Fatal("signal order")
				}
				m.Close()
			case <-time.After(5 * time.Second):
				t.Fatal("signal timeout")
			}
		}
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	a.NewSignal("/", match.Interface, "Changed", "")
	if _, err := a.Send(); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-s2.C:
		m.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("duplicate rule removed")
	}
	if _, ok := <-s.C; ok {
		t.Fatal("stream not closed")
	}
}
func TestDispatcherSubscriptionDrop(t *testing.T) {
	addr := privateBus(t)
	a, b := dispatcher(t, addr), dispatcher(t, addr)
	ctx := context.Background()
	s, err := b.Subscribe(ctx, Match{Sender: a.UniqueName(), Interface: "org.example.Test"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	for i := uint32(0); i < 10; i++ {
		a.NewSignal("/", "org.example.Test", "Changed", "u").Uint32(i)
		if _, err := a.Send(); err != nil {
			t.Fatal(err)
		}
	}
	// A bus roundtrip on the emitter orders its signals before a marker sent to b.
	barrier(t, a)
	a.NewCall(b.UniqueName(), "/", "org.example.Test", "Marker", "")
	m, _ := a.Call(ctx)
	m.Close()
	if s.Dropped() != 9 {
		t.Fatalf("dropped %d", s.Dropped())
	}
	if m := <-s.C; m.Body().Uint32() != 0 {
		t.Fatal("not drop-newest")
	}
}
func TestDispatcherSendSignal(t *testing.T) {
	d := dispatcher(t, privateBus(t))
	d.NewSignal("/", "org.example.Test", "Changed", "")
	if _, err := d.Send(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherMalformed(t *testing.T) {
	bad := make([]byte, headerLen)
	bad[0] = 'x'
	d := dispatcher(t, fakeBus(t, "OK guid\r\n", bad))
	waitDone(t, d.Done())
	if !errors.Is(d.Err(), ErrMalformed) {
		t.Fatal(d.Err())
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherSenderIsolation(t *testing.T) {
	addr := privateBus(t)
	a, b, c := dispatcher(t, addr), dispatcher(t, addr), dispatcher(t, addr)
	ctx := context.Background()
	broad, err := b.Subscribe(ctx, Match{Interface: "org.example.Test"}, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer broad.Close(ctx)
	narrow, err := b.Subscribe(ctx, Match{Sender: a.UniqueName(), Interface: "org.example.Test"}, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer narrow.Close(ctx)
	if s, err := b.Subscribe(ctx, Match{Sender: busName}, 1); s != nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("well-known sender accepted: %v", err)
	}
	for _, emitter := range []*Dispatcher{a, c} {
		emitter.NewSignal("/", "org.example.Test", "Changed", "")
		if _, err := emitter.Send(); err != nil {
			t.Fatal(err)
		}
		emitter.NewCall(b.UniqueName(), "/", "org.example.Test", "Barrier", "")
		m, _ := emitter.Call(ctx)
		m.Close()
	}
	for _, want := range []string{a.UniqueName(), c.UniqueName()} {
		select {
		case m := <-broad.C:
			if m.Sender != want {
				t.Fatal(m.Sender)
			}
			m.Close()
		case <-time.After(5 * time.Second):
			t.Fatal("missing broad signal")
		}
	}
	select {
	case m := <-narrow.C:
		if m.Sender != a.UniqueName() {
			t.Fatal(m.Sender)
		}
		m.Close()
	default:
		t.Fatal("missing sender signal")
	}
	select {
	case m := <-narrow.C:
		m.Close()
		t.Fatal("wrong sender delivered")
	default:
	}
}

func TestDispatcherSubscriptionCloseRace(t *testing.T) {
	addr := privateBus(t)
	a := dispatcher(t, addr)
	for range 20 {
		b := dispatcher(t, addr)
		s, err := b.Subscribe(context.Background(), Match{Interface: "org.example.Test"}, 2)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = s.Close(ctx)
		})
		wg.Go(func() { _ = b.Close() })
		wg.Go(func() {
			for range 10 {
				a.NewSignal("/", "org.example.Test", "Changed", "")
				if _, err := a.Send(); err != nil {
					t.Error(err)
					return
				}
			}
		})
		wg.Wait()
		for m := range s.C {
			m.Close()
		}
		waitDone(t, b.Done())
	}
}

// cancelOnWait cancels when Call starts waiting, after Go has sent AddMatch.
// It makes the otherwise timing-dependent cancellation window deterministic.
type cancelOnWait struct {
	context.Context
	cancel context.CancelFunc
}

func (c cancelOnWait) Done() <-chan struct{} { c.cancel(); return c.Context.Done() }

func TestDispatcherCanceledSubscribeBusRule(t *testing.T) {
	d := dispatcher(t, privateBus(t))
	match := Match{Interface: "org.example.Canceled"}
	for _, afterSend := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		var callCtx context.Context = ctx
		if afterSend {
			callCtx = cancelOnWait{Context: ctx, cancel: cancel}
		} else {
			cancel()
		}
		s, err := d.Subscribe(callCtx, match, 1)
		cancel()
		if s != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe = %v, %v", s, err)
		}
		// Bus rules are refcounted. If cleanup missed an installed rule, this
		// second removal would succeed rather than return MatchRuleNotFound.
		err = d.matchCall(context.Background(), "RemoveMatch", match.rule())
		var e *Error
		if !errors.As(err, &e) || e.Name != "org.freedesktop.DBus.Error.MatchRuleNotFound" {
			t.Fatalf("rule remains installed: %v", err)
		}
	}
}
