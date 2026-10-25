//go:build !linux

package zerobus

import "syscall"

const fdSupported = false

func closeFDs(fds []int) {
	for _, fd := range fds {
		if fd >= 0 {
			_ = syscall.Close(fd)
		}
	}
}
func (c *Conn) readMessageBytes(b []byte) (int, error) { return c.c.Read(b) }
func (c *Conn) writeMessage(b []byte, fds []int) error {
	if len(fds) != 0 {
		return ErrInvalid
	}
	_, err := c.c.Write(b)
	return err
}
