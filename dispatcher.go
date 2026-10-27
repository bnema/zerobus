package zerobus

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Msg is a detached message. Its strings and decoded body remain valid across
// reads. Close it when done to release untaken descriptors. Msg is not safe
// for concurrent use. Signals never contain descriptors.
type Msg struct {
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
	Signature   string

	buf, body []byte
	big       bool
	fds       []int
	r         Reader
}

// Body restarts the body reader.
func (m *Msg) Body() *Reader { m.r = Reader{buf: m.body, big: m.big}; return &m.r }

// NumFDs returns the number of received descriptor slots, including taken slots.
func (m *Msg) NumFDs() int { return len(m.fds) }

// TakeFD transfers a descriptor to the caller, who must close it.
func (m *Msg) TakeFD(i uint32) (*os.File, error) {
	if i >= uint32(len(m.fds)) || m.fds[i] < 0 {
		return nil, ErrInvalid
	}
	fd := m.fds[i]
	m.fds[i] = -1
	return os.NewFile(uintptr(fd), "zerobus-received"), nil
}

// Close releases untaken descriptors. It is idempotent and nil-safe.
func (m *Msg) Close() {
	if m != nil {
		closeFDs(m.fds)
		for i := range m.fds {
			m.fds[i] = -1
		}
	}
}

func detach(src *Message) *Msg {
	m := &Msg{Type: src.Type, Flags: src.Flags, Serial: src.Serial, ReplySerial: src.ReplySerial, big: src.big}
	n := len(src.Path) + len(src.Interface) + len(src.Member) + len(src.ErrorName) + len(src.Destination) + len(src.Sender) + len(src.Signature)
	off := pad(n, 8)
	m.buf = make([]byte, off+len(src.body))
	pos := 0
	for _, pair := range []struct {
		src string
		dst *string
	}{{src.Path, &m.Path}, {src.Interface, &m.Interface}, {src.Member, &m.Member}, {src.ErrorName, &m.ErrorName}, {src.Destination, &m.Destination}, {src.Sender, &m.Sender}, {src.Signature, &m.Signature}} {
		if len(pair.src) > 0 {
			copy(m.buf[pos:], pair.src)
			*pair.dst = unsafe.String(&m.buf[pos], len(pair.src))
			pos += len(pair.src)
		}
	}
	m.body = m.buf[off:]
	copy(m.body, src.body)
	if src.Type != TypeSignal && src.conn != nil {
		c := src.conn
		c.fdMu.Lock()
		if len(c.pending) > 0 {
			m.fds = append([]int(nil), c.pending...)
			for i := range c.pending {
				c.pending[i] = -1
			}
		}
		c.fdMu.Unlock()
	}
	return m
}

// Dispatcher owns a Conn and provides concurrent calls and bounded signal
// streams using one reader goroutine. Do not use the Conn after handing it off.
// A NewCall/NewSignal and its following Call/Go/Send form one critical section:
// they must run on the same goroutine, with no intervening Dispatcher methods.
type Dispatcher struct {
	c          *Conn
	wmu        sync.Mutex
	pmu        sync.Mutex
	pending    map[uint32]*Pending
	closed     bool
	err        error
	done       chan struct{}
	smu        sync.RWMutex
	subs       []*Subscription
	subsClosed bool
}

// NewDispatcher takes ownership of c and starts its reader. c.Handle must be nil.
func NewDispatcher(c *Conn) *Dispatcher {
	if c.Handle != nil {
		panic("zerobus: Dispatcher requires nil Handle")
	}
	d := &Dispatcher{c: c, pending: make(map[uint32]*Pending), done: make(chan struct{})}
	go d.read()
	return d
}

// DialDispatcher connects and starts a dispatcher.
func DialDispatcher(ctx context.Context, address string) (*Dispatcher, error) {
	c, e := DialContext(ctx, address)
	if e != nil {
		return nil, e
	}
	return NewDispatcher(c), nil
}

// SessionBusDispatcher connects to the session bus and starts a dispatcher.
func SessionBusDispatcher(ctx context.Context) (*Dispatcher, error) {
	c, e := SessionBusContext(ctx)
	if e != nil {
		return nil, e
	}
	return NewDispatcher(c), nil
}

// SystemBusDispatcher connects to the system bus and starts a dispatcher.
func SystemBusDispatcher(ctx context.Context) (*Dispatcher, error) {
	c, e := SystemBusContext(ctx)
	if e != nil {
		return nil, e
	}
	return NewDispatcher(c), nil
}

// UniqueName returns the bus-assigned name.
func (d *Dispatcher) UniqueName() string { return d.c.name }

// UnixFDs reports negotiated descriptor support.
func (d *Dispatcher) UnixFDs() bool { return d.c.unixFDs }

// Done closes when the reader exits.
func (d *Dispatcher) Done() <-chan struct{} { return d.done }

// Err returns the terminal error after Done closes.
func (d *Dispatcher) Err() error { d.pmu.Lock(); defer d.pmu.Unlock(); return d.err }

// Close closes the connection and waits for the reader. It is idempotent.
func (d *Dispatcher) Close() error { _ = d.c.Close(); <-d.done; return nil }

// NewCall starts a call. Exactly one Call, Go or Send must follow.
func (d *Dispatcher) NewCall(dest, path, iface, member, sig string) *Encoder {
	d.wmu.Lock()
	return d.c.NewCall(dest, path, iface, member, sig)
}

// NewSignal starts a signal. Send must follow.
func (d *Dispatcher) NewSignal(path, iface, member, sig string) *Encoder {
	d.wmu.Lock()
	return d.c.NewSignal(path, iface, member, sig)
}

func (d *Dispatcher) send() (uint32, error) {
	b, err := d.c.enc.finish()
	d.c.enc.body = 0
	if err != nil {
		return 0, err
	}
	if len(d.c.enc.fds) > 0 && !d.c.unixFDs {
		return 0, ErrInvalid
	}
	if err = d.c.writeMessage(b, d.c.enc.fds); err != nil {
		_ = d.c.Close()
		return 0, closedErr(err)
	}
	return d.c.serial, nil
}

// Send sends the built message without tracking a reply.
func (d *Dispatcher) Send() (uint32, error) { defer d.wmu.Unlock(); return d.send() }

// Pending is an asynchronous call. Always retrieve its result and close the
// returned Msg: abandoning a delivered result can leak descriptors. Pending
// is not safe for concurrent use, except Cancel racing completion.
type Pending struct {
	d      *Dispatcher
	serial uint32
	done   chan struct{}
	msg    *Msg
	err    error
}

// Serial returns the outgoing serial.
func (p *Pending) Serial() uint32 { return p.serial }

// Done closes on reply, cancellation or connection failure.
func (p *Pending) Done() <-chan struct{} { return p.done }

// Result returns the result after Done closes.
func (p *Pending) Result() (*Msg, error) { p.d.pmu.Lock(); defer p.d.pmu.Unlock(); return p.msg, p.err }

// Cancel cancels the call, closing even a reply that raced cancellation.
func (p *Pending) Cancel() {
	d := p.d
	d.pmu.Lock()
	defer d.pmu.Unlock()
	if d.pending[p.serial] == p {
		delete(d.pending, p.serial)
		close(p.done)
	}
	p.msg.Close()
	p.msg = nil
	p.err = context.Canceled
}

// Go sends a method call and returns its pending result.
func (d *Dispatcher) Go() (*Pending, error) {
	defer d.wmu.Unlock()
	e := &d.c.enc
	if e.body == 0 || Type(e.buf[1]) != TypeMethodCall || Flags(e.buf[2])&FlagNoReplyExpected != 0 {
		e.body = 0
		return nil, ErrInvalid
	}
	d.pmu.Lock()
	if d.closed {
		d.pmu.Unlock()
		e.body = 0
		return nil, ErrClosed
	}
	p := &Pending{d: d, serial: d.c.serial, done: make(chan struct{})}
	d.pending[p.serial] = p
	d.pmu.Unlock()
	_, err := d.send()
	if err != nil {
		d.pmu.Lock()
		if d.pending[p.serial] == p {
			delete(d.pending, p.serial)
			p.err = err
			close(p.done)
		}
		d.pmu.Unlock()
		return nil, err
	}
	return p, nil
}

// Call sends and waits. Cancellation always returns ctx.Err(), never a reply.
func (d *Dispatcher) Call(ctx context.Context) (*Msg, error) {
	p, err := d.Go()
	if err != nil {
		return nil, err
	}
	select {
	case <-p.done:
	case <-ctx.Done():
	}
	if err = ctx.Err(); err != nil {
		p.Cancel()
		return nil, err
	}
	return p.Result()
}

func (d *Dispatcher) read() {
	for {
		m, err := d.c.ReadMessage()
		if err != nil {
			d.teardown(err)
			return
		}
		switch m.Type {
		case TypeMethodReturn, TypeError:
			d.pmu.Lock()
			p := d.pending[m.ReplySerial]
			if p != nil {
				delete(d.pending, m.ReplySerial)
				p.msg = detach(m)
				if m.Type == TypeError {
					p.err = replyError(m)
				}
				close(p.done)
			}
			d.pmu.Unlock()
		case TypeSignal:
			d.smu.RLock()
			for _, s := range d.subs {
				if s.match.matches(m) {
					// Only this reader sends, and smu prevents channel closure.
					// Check capacity before detaching so drops allocate nothing.
					if len(s.ch) < cap(s.ch) {
						s.ch <- detach(m)
					} else {
						s.dropped.Add(1)
					}
				}
			}
			d.smu.RUnlock()
		case TypeMethodCall:
			if m.Flags&FlagNoReplyExpected == 0 {
				d.wmu.Lock()
				e := d.c.NewError(m, "org.freedesktop.DBus.Error.UnknownMethod", "s")
				const suffix = " is not implemented"
				e.Uint32(uint32(len(m.Interface) + 1 + len(m.Member) + len(suffix)))
				e.buf = append(e.buf, m.Interface...)
				e.buf = append(e.buf, '.')
				e.buf = append(e.buf, m.Member...)
				e.buf = append(e.buf, suffix...)
				e.buf = append(e.buf, 0)
				_, _ = d.send()
				d.wmu.Unlock()
			}
		}
	}
}
func (d *Dispatcher) teardown(err error) {
	d.pmu.Lock()
	d.closed = true
	d.err = err
	if err != ErrClosed {
		d.err = closedErr(err)
	}
	for serial, p := range d.pending {
		p.err = d.err
		close(p.done)
		delete(d.pending, serial)
	}
	d.pmu.Unlock()
	d.smu.Lock()
	d.subsClosed = true
	for _, s := range d.subs {
		s.closed = true
		close(s.ch)
	}
	d.subs = nil
	d.smu.Unlock()
	_ = d.c.Close()
	close(d.done)
}

// Match selects signals. Empty fields match anything. Well-known senders
// are filtered by the bus; only unique sender names are filtered locally.
type Match struct{ Sender, Path, Interface, Member string }

func (m Match) matches(s *Message) bool {
	return (m.Sender == "" || !strings.HasPrefix(m.Sender, ":") || m.Sender == s.Sender) && (m.Path == "" || m.Path == s.Path) && (m.Interface == "" || m.Interface == s.Interface) && (m.Member == "" || m.Member == s.Member)
}
func (m Match) rule() string {
	var b strings.Builder
	b.Grow(len("type='signal'") + len(m.Sender) + len(m.Path) + len(m.Interface) + len(m.Member) + 64 + 3*(strings.Count(m.Sender, "'")+strings.Count(m.Path, "'")+strings.Count(m.Interface, "'")+strings.Count(m.Member, "'")))
	b.WriteString("type='signal'")
	for _, v := range []struct{ k, v string }{{"sender", m.Sender}, {"path", m.Path}, {"interface", m.Interface}, {"member", m.Member}} {
		if v.v != "" {
			b.WriteByte(',')
			b.WriteString(v.k)
			b.WriteString("='")
			for _, r := range v.v {
				if r == '\'' {
					b.WriteString("'\\''")
				} else {
					b.WriteRune(r)
				}
			}
			b.WriteByte('\'')
		}
	}
	return b.String()
}

// Subscription is a bounded drop-newest signal stream. Close unregisters it.
type Subscription struct {
	C       <-chan *Msg
	ch      chan *Msg
	d       *Dispatcher
	match   Match
	rule    string
	closed  bool
	dropped atomic.Uint64
}

// Dropped returns the number of signals dropped because the buffer was full.
func (s *Subscription) Dropped() uint64 { return s.dropped.Load() }
func (d *Dispatcher) matchCall(ctx context.Context, member, rule string) error {
	d.NewCall(busName, busPath, busName, member, "s").Str(rule)
	m, err := d.Call(ctx)
	m.Close()
	return err
}

// Subscribe installs a bus match and a local stream with capacity buf (at least 1).
func (d *Dispatcher) Subscribe(ctx context.Context, m Match, buf int) (*Subscription, error) {
	if buf < 1 {
		return nil, ErrInvalid
	}
	rule := m.rule()
	if err := d.matchCall(ctx, "AddMatch", rule); err != nil {
		return nil, err
	}
	s := &Subscription{d: d, match: m, rule: rule, ch: make(chan *Msg, buf)}
	s.C = s.ch
	d.smu.Lock()
	defer d.smu.Unlock()
	if d.subsClosed {
		return nil, ErrClosed
	}
	d.subs = append(d.subs, s)
	return s, nil
}

// Close stops delivery immediately, then best-effort removes the bus match.
// After dispatcher teardown it returns nil without a bus call.
func (s *Subscription) Close(ctx context.Context) error {
	d := s.d
	d.smu.Lock()
	if s.closed {
		d.smu.Unlock()
		return nil
	}
	s.closed = true
	for i, v := range d.subs {
		if v == s {
			d.subs = append(d.subs[:i], d.subs[i+1:]...)
			break
		}
	}
	close(s.ch)
	d.smu.Unlock()
	return d.matchCall(ctx, "RemoveMatch", s.rule)
}
