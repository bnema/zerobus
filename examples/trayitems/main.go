// Command trayitems lists the StatusNotifierItems registered on the session
// bus, then prints their changes as they happen.
package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/bnema/zerobus"
)

const (
	watcher     = "org.kde.StatusNotifierWatcher"
	watcherPath = "/StatusNotifierWatcher"
	props       = "org.freedesktop.DBus.Properties"
	item        = "org.kde.StatusNotifierItem"
)

func main() {
	c, err := zerobus.SessionBus()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	e := c.NewCall(watcher, watcherPath, props, "Get", "ss")
	e.Str(watcher)
	e.Str("RegisteredStatusNotifierItems")
	m, err := c.Call()
	if err != nil {
		log.Fatal(err)
	}
	// The reply is a variant holding an array of strings. Copy the names:
	// the next call reuses the buffer they point into.
	r := m.Body()
	var items []string
	if r.Variant() == "as" {
		end := r.Array('s')
		for r.More(end) {
			items = append(items, strings.Clone(r.Str()))
		}
	}
	if r.Err() != nil {
		log.Fatal(r.Err())
	}

	for _, it := range items {
		// An item is "bus-name/object/path", or only a bus name.
		dest, path, ok := strings.Cut(it, "/")
		if ok {
			path = "/" + path
		} else {
			path = "/StatusNotifierItem"
		}
		fmt.Printf("%s\n  id=%q icon=%q status=%q\n", it, get(c, dest, path, "Id"), get(c, dest, path, "IconName"), get(c, dest, path, "Status"))
	}

	if err := c.AddMatch("type='signal',interface='" + item + "'"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("waiting for changes (Ctrl+C to stop)")
	for {
		m, err := c.ReadMessage()
		if err != nil {
			log.Fatal(err)
		}
		if m.Type == zerobus.TypeSignal {
			fmt.Printf("%s %s %s\n", m.Sender, m.Path, m.Member)
		}
	}
}

// get reads a string property, or returns "" when it has none.
func get(c *zerobus.Conn, dest, path, name string) string {
	e := c.NewCall(dest, path, props, "Get", "ss")
	e.Str(item)
	e.Str(name)
	m, err := c.Call()
	if err != nil {
		return ""
	}
	r := m.Body()
	if r.Variant() != "s" {
		return ""
	}
	return strings.Clone(r.Str())
}
