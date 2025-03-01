package pad

import "sync/atomic"

// AtomicInt64 is an atomic 64-bit int that is padded
// to prevent false sharing.
type AtomicInt64 struct {
	atomic.Int64
	_ [56]byte
}

// Int64 is a int64 padded to prevent false sharing.
type Int64 struct {
	Val int64
	_   [56]byte
}
