//go:build unix

package memory

import "syscall"

// mmapAnon maps size bytes of zeroed anonymous memory. MAP_NORESERVE (where
// supported) keeps large lazily-touched segments from counting against
// strict overcommit accounting until they are written.
func mmapAnon(size int) ([]byte, error) {
	return syscall.Mmap(-1, 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE|mapNoReserve)
}
