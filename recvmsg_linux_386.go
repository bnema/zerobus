//go:build linux && 386

package zerobus

import (
	"syscall"
	"unsafe"
)

func recvmsgSyscall(fd uintptr, msg unsafe.Pointer) (uintptr, syscall.Errno) {
	// Linux/i386 multiplexes socket operations through socketcall;
	// operation 17 is recvmsg. Keep the argument vector on the stack.
	args := [3]uintptr{fd, uintptr(msg), syscall.MSG_CMSG_CLOEXEC}
	n, _, errno := syscall.Syscall(syscall.SYS_SOCKETCALL, 17, uintptr(unsafe.Pointer(&args[0])), 0)
	return n, errno
}
