//go:build linux

package zerobus

import (
	"io"
	"syscall"
	"unsafe"
)

const fdSupported = true

func closeFDs(fds []int) {
	for _, fd := range fds {
		if fd >= 0 {
			_ = syscall.Close(fd)
		}
	}
}

func (c *Conn) readMessageBytes(b []byte) (int, error) {
	if c.raw == nil {
		raw, err := c.c.SyscallConn()
		if err != nil {
			return 0, err
		}
		c.raw = raw
		c.recvFn = c.recvmsg
	}
	c.recvBuf = b
	err := c.raw.Read(c.recvFn)
	c.recvBuf = nil
	if err != nil {
		return 0, err
	}
	if c.recvErr != nil {
		return 0, c.recvErr
	}
	n, oobn, flags := c.recvN, c.recvOOB, c.recvFlags
	c.fdMu.Lock()
	defer c.fdMu.Unlock()
	// Parse even a truncated control buffer, so every delivered fd is owned.
	msgs, parseErr := syscall.ParseSocketControlMessage(c.oob[:oobn])
	for _, msg := range msgs {
		if msg.Header.Level != syscall.SOL_SOCKET || msg.Header.Type != syscall.SCM_RIGHTS {
			continue
		}
		fds, e := syscall.ParseUnixRights(&msg)
		if e != nil {
			parseErr = e
			continue
		}
		if !c.unixFDs && len(fds) > 0 {
			parseErr = ErrMalformed
		}
		if c.fdClosed || !c.unixFDs {
			closeFDs(fds)
		} else {
			c.fdQueue = append(c.fdQueue, fds...)
		}
	}
	if flags&syscall.MSG_CTRUNC != 0 || parseErr != nil {
		closeFDs(c.fdQueue)
		c.fdQueue = c.fdQueue[:0]
		return n, ErrMalformed
	}
	if c.fdClosed {
		return n, ErrClosed
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (c *Conn) writeMessage(b []byte, fds []int) error {
	if len(fds) > 0 {
		n, _, err := c.c.WriteMsgUnix(b, syscall.UnixRights(fds...), nil)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	for len(b) > 0 {
		n, err := c.c.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func (c *Conn) recvmsg(fd uintptr) bool {
	var iov syscall.Iovec
	iov.Base = &c.recvBuf[0]
	iov.SetLen(len(c.recvBuf))
	var msg syscall.Msghdr
	msg.Iov = &iov
	msg.Iovlen = 1
	msg.Control = &c.oob[0]
	msg.SetControllen(len(c.oob))
	n, errno := recvmsgSyscall(fd, unsafe.Pointer(&msg))
	c.recvErr = nil
	if errno != 0 {
		c.recvErr = errno
	}
	c.recvN, c.recvOOB, c.recvFlags = int(n), int(msg.Controllen), int(msg.Flags)
	return c.recvErr != syscall.EAGAIN && c.recvErr != syscall.EWOULDBLOCK && c.recvErr != syscall.EINTR
}
