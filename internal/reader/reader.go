package reader

import (
	"time"

	"github.com/five-vee/disruptor/internal/barrier"
	"github.com/five-vee/disruptor/internal/closer"
	"github.com/five-vee/disruptor/internal/pad"
)

// Reader represents a single Reader of the ring buffer.
type Reader[T any] struct {
	buffer          []T
	mask            int64
	f               func(*T)
	upstreamBarrier barrier.Barrier
	closedBarrier   barrier.ClosedBarrier

	_      [64]byte
	cursor pad.AtomicInt64
	closer closer.Closer
}

// NewReader returns a new Reader and its corresponding cursor.
func NewReader[T any](upstreamBarrier barrier.Barrier, f func(*T), closedBarrier barrier.ClosedBarrier, buffer []T) (r *Reader[T], cursor *pad.AtomicInt64, state *closer.Closer) {
	r = &Reader[T]{
		buffer:          buffer,
		mask:            int64(len(buffer) - 1),
		f:               f,
		upstreamBarrier: upstreamBarrier,
		closedBarrier:   closedBarrier,
	}
	return r, &r.cursor, &r.closer
}

// LoopRead continuously reads messages.
// Blocks until the ring buffer is closed and empty.
func (r *Reader[T]) LoopRead() {
	defer r.closer.Close()
	current := r.cursor.Load()
	for {
		upstream := r.upstreamBarrier.Load()
		if current == upstream {
			if r.closedBarrier.IsClosed() {
				return
			}
			time.Sleep(50 * time.Microsecond)
			continue
		}
		for seq := current + 1; seq <= upstream; seq++ {
			r.f(&r.buffer[seq&r.mask])
		}
		r.cursor.Store(upstream)
		current = upstream
	}
}
