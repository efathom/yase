//go:build unix && !linux

package memory

// Darwin and the BSDs commit anonymous pages lazily already.
const mapNoReserve = 0
