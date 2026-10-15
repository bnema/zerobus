package zerobus

import (
	"errors"
	"fmt"
	"syscall"
	"time"
	"unsafe"
)

// errNoWaitFd is returned where waitFd is not supported; never on Linux.
var errNoWaitFd = errors.New("waitFd: not supported on this platform")

// waitFd blocks until fd is readable or the timeout passes, as a poll loop
// does.
func waitFd(fd int, timeout time.Duration) error {
	var set syscall.FdSet
	bits := int(unsafe.Sizeof(set.Bits[0])) * 8
	set.Bits[fd/bits] |= 1 << (uint(fd) % uint(bits))
	tv := syscall.NsecToTimeval(timeout.Nanoseconds())
	n, err := syscall.Select(fd+1, &set, nil, nil, &tv)
	if err != nil || n != 1 {
		return fmt.Errorf("select = %d, %v", n, err)
	}
	return nil
}
