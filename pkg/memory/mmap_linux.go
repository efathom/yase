//go:build linux

package memory

import "syscall"

const mapNoReserve = syscall.MAP_NORESERVE
