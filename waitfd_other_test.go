//go:build !linux

package zerobus

import (
	"errors"
	"time"
)

// errNoWaitFd means the platform has no waitFd: syscall.FdSet differs
// between the BSDs and Darwin, and the poll tests only run on Linux.
var errNoWaitFd = errors.New("waitFd: not supported on this platform")

func waitFd(int, time.Duration) error { return errNoWaitFd }
