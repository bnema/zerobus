//go:build !linux

package zerobus

import (
	"fmt"
	"syscall"
	"time"
	"unsafe"
)

// waitFd blocks until fd is readable or the timeout passes, as a poll loop
// does. syscall.Select returns only an error outside Linux.
func waitFd(fd int, timeout time.Duration) error {
	var set syscall.FdSet
	bits := int(unsafe.Sizeof(set.Bits[0])) * 8
	set.Bits[fd/bits] |= 1 << (uint(fd) % uint(bits))
	tv := syscall.NsecToTimeval(timeout.Nanoseconds())
	if err := syscall.Select(fd+1, &set, nil, nil, &tv); err != nil {
		return fmt.Errorf("select: %v", err)
	}
	if set.Bits[fd/bits]&(1<<(uint(fd)%uint(bits))) == 0 {
		return fmt.Errorf("select: fd %d not readable after %v", fd, timeout)
	}
	return nil
}
