package zerobus

import "testing"

func TestDispatcherDetach(t *testing.T) {
	for _, big := range []bool{false, true} {
		src := &Message{Type: TypeMethodReturn, Flags: FlagNoAutoStart, Serial: 17, ReplySerial: 12, Path: "/test", Interface: "org.example.Test", Member: "Changed", ErrorName: "org.example.Error", Destination: ":1.2", Sender: ":1.3", Signature: "s", body: []byte{1, 0, 0, 0, 'x', 0}, big: big}
		if big {
			src.body = []byte{0, 0, 0, 1, 'x', 0}
		}
		m := detach(src)
		if m.Type != src.Type || m.Flags != src.Flags || m.Serial != 17 || m.ReplySerial != 12 || m.Path != src.Path || m.Interface != src.Interface || m.Member != src.Member || m.ErrorName != src.ErrorName || m.Destination != src.Destination || m.Sender != src.Sender || m.Signature != src.Signature {
			t.Fatal("lost headers")
		}
		clear(src.body)
		r := m.Body()
		if r.Str() != "x" || !r.Done() {
			t.Fatal("detached body invalid")
		}
		if m.Body().Str() != "x" {
			t.Fatal("reader did not restart")
		}
		m.Close()
		if m.Sender != ":1.3" || m.Body().Str() != "x" {
			t.Fatal("Close invalidated storage")
		}
	}
	var m *Msg
	m.Close()
}
func TestDispatcherMatch(t *testing.T) {
	msg := &Message{Sender: ":1.2", Path: "/test", Interface: "org.example.Test", Member: "Changed"}
	for _, m := range []Match{{}, {Sender: ":1.2"}, {Path: "/test", Interface: "org.example.Test", Member: "Changed"}} {
		if !m.matches(msg) {
			t.Fatalf("did not match %+v", m)
		}
	}
	for _, m := range []Match{{Sender: "org.example.Service"}, {Sender: ":1.3"}, {Path: "/other"}, {Interface: "org.example.Other"}, {Member: "Other"}} {
		if m.matches(msg) {
			t.Fatalf("matched %+v", m)
		}
	}
	m := Match{Sender: ":1.2", Path: "/test", Interface: "org.example.Test", Member: "Changed"}
	if rule := m.rule(); rule != "type='signal',sender=':1.2',path='/test',interface='org.example.Test',member='Changed'" {
		t.Fatal(rule)
	}
	if n := testing.AllocsPerRun(100, func() { _ = m.matches(msg) }); n != 0 {
		t.Fatal(n)
	}
	if n := testing.AllocsPerRun(100, func() { _ = m.rule() }); n != 1 {
		t.Fatalf("rule allocations %g", n)
	}
}
