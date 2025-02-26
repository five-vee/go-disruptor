package disruptor

import "sync/atomic"

type paddedAtomicInt64 struct {
	atomic.Int64
	_ [56]byte // cache line padding (64 bytes total)
}
