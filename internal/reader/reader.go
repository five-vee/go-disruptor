package reader

import (
	"github.com/five-vee/go-disruptor/internal/barrier"
	"github.com/five-vee/go-disruptor/internal/closer"
	"github.com/five-vee/go-disruptor/internal/pad"
)

// SingleReader represents a SingleReader of the ring buffer.
type SingleReader[T any] struct {
	buffer          []T
	mask            int64
	f               func(*T)
	readerYield     func()
	upstreamBarrier barrier.Barrier
	closedBarrier   barrier.ClosedBarrier

	_ [64]byte // padding

	cursor pad.AtomicInt64
	closer closer.Closer
}

// NewSingleReader returns a new SingleReader, its cursor, and its closer.
func NewSingleReader[T any](upstreamBarrier barrier.Barrier, f func(*T), closedBarrier barrier.ClosedBarrier, buffer []T, readerYield func()) (r *SingleReader[T], cursor *pad.AtomicInt64, closer *closer.Closer) {
	r = &SingleReader[T]{
		buffer:          buffer,
		mask:            int64(len(buffer) - 1),
		f:               f,
		readerYield:     readerYield,
		upstreamBarrier: upstreamBarrier,
		closedBarrier:   closedBarrier,
	}
	return r, &r.cursor, &r.closer
}

// LoopRead continuously reads messages.
// Blocks until the ring buffer is closed and empty.
func (r *SingleReader[T]) LoopRead() {
	defer r.closer.Close()
	current := r.cursor.Load()

	for {
		if upstream := r.upstreamBarrier.Load(); current < upstream {
			for seq := current + 1; seq <= upstream; seq++ {
				r.f(&r.buffer[seq&r.mask])
			}
			r.cursor.Store(upstream)
			current = upstream
		} else if upstream := r.upstreamBarrier.Load(); current < upstream {
			// try again
			for seq := current + 1; seq <= upstream; seq++ {
				r.f(&r.buffer[seq&r.mask])
			}
			r.cursor.Store(upstream)
			current = upstream
		} else if r.closedBarrier.IsClosed() {
			return
		} else {
			r.readerYield()
		}
	}
}

// BatchReader represents a batch reader of the ring buffer.
type BatchReader[T any] struct {
	buffer          []T
	mask            int64
	f               func(ptrs [2]*T, lens [2]int)
	readerYield     func()
	upstreamBarrier barrier.Barrier
	closedBarrier   barrier.ClosedBarrier

	_      [64]byte
	cursor pad.AtomicInt64
	closer closer.Closer
}

// NewBatchReader returns a new batch reader, its cursor, and its closer.
func NewBatchReader[T any](upstreamBarrier barrier.Barrier, f func(ptrs [2]*T, lens [2]int), closedBarrier barrier.ClosedBarrier, buffer []T, readerYield func()) (r *BatchReader[T], cursor *pad.AtomicInt64, closer *closer.Closer) {
	r = &BatchReader[T]{
		buffer:          buffer,
		mask:            int64(len(buffer) - 1),
		f:               f,
		readerYield:     readerYield,
		upstreamBarrier: upstreamBarrier,
		closedBarrier:   closedBarrier,
	}
	return r, &r.cursor, &r.closer
}

// LoopRead continuously reads messages.
// Blocks until the ring buffer is closed and empty.
func (r *BatchReader[T]) LoopRead() {
	defer r.closer.Close()
	current := r.cursor.Load()

	for {
		if upstream := r.upstreamBarrier.Load(); current < upstream {
			i, j := (current+1)&r.mask, upstream&r.mask
			len1, len2 := unwrap(int64(len(r.buffer)), i, j)
			r.f([2]*T{&r.buffer[i], &r.buffer[0]}, [2]int{len1, len2})
			r.cursor.Store(upstream)
			current = upstream
		} else if upstream := r.upstreamBarrier.Load(); current < upstream {
			// try again
			i, j := (current+1)&r.mask, upstream&r.mask
			len1, len2 := unwrap(int64(len(r.buffer)), i, j)
			r.f([2]*T{&r.buffer[i], &r.buffer[0]}, [2]int{len1, len2})
			r.cursor.Store(upstream)
			current = upstream
		} else if r.closedBarrier.IsClosed() {
			return
		} else {
			r.readerYield()
		}
	}
}

// unwrap returns the range of data from `i` to `j`,
// where it is possible that `j` wraps around the buffer.
//
// Both `i` and `j` are inclusive indexes into the buffer.
//
// The range is returned as two slices, where the 1st slice
// is up to the end of the buffer, and the 2nd slice is
// from the beginning of the buffer.
//
// If j does not wrap around, then the 2nd slice is empty.
func unwrap(capacity, i, j int64) (int, int) {
	// Compute wrap: 1 if j < i, 0 if i <= j.
	wrap := ((j - i) >> 63) & 1
	// Create a mask that is 0 if wrap==0, and -1 (all ones) if wrap==1.
	mask := -wrap

	// When there is no wrap (mask==0), we want firstLen = j - i + 1.
	// When wrapping (mask==-1), we want firstLen = capacity - i.
	// We can compute this branchlessly:
	firstLen := ((j - i + 1) & (^mask)) | ((capacity - i) & mask)

	// Similarly, the second length is 0 when there's no wrap, or (j + 1) when wrapping.
	secondLen := (j + 1) & mask

	return int(firstLen), int(secondLen)
}
