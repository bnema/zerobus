//go:build linux && !386

package zerobus

import (
	"syscall"
	"unsafe"
)

func recvmsgSyscall(fd uintptr, msg unsafe.Pointer) (uintptr, syscall.Errno) {
	n, _, errno := syscall.Syscall(syscall.SYS_RECVMSG, fd, uintptr(msg), syscall.MSG_CMSG_CLOEXEC)
	return n, errno
}
