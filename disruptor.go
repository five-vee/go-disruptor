// Package disruptor provides an implementation of the LMAX Disruptor.
//
// If for some reason you have Go code that needs to process messages at
// sub-microsecond latency, where shaving every nanosecond counts, then
// consider the disruptor pattern.
package disruptor

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/five-vee/go-disruptor/internal/barrier"
	"github.com/five-vee/go-disruptor/internal/closer"
	"github.com/five-vee/go-disruptor/internal/pad"
	"github.com/five-vee/go-disruptor/internal/reader"
)

var (
	// ErrCapacity is the error corresponding to wrong capacity.
	ErrCapacity = fmt.Errorf("capacity must be a power of two")

	// ErrMissingReaderGroup is the error corresponding to missing
	// reader group(s).
	ErrMissingReaderGroup = fmt.Errorf("missing reader group(s)")

	// ErrEmptyReaderGroup is the error corresponding to an empty
	// reader group.
	ErrEmptyReaderGroup = fmt.Errorf("reader group is empty")
)

// Builder builds a disruptor.
type Builder[T any] struct {
	capacity     int64
	readerGroups [][]ReaderFunc
}

// NewBuilder returns a builder of a disruptor.
func NewBuilder[T any](capacity int64) *Builder[T] {
	return &Builder[T]{capacity: capacity}
}

// WithReaderGroup represents a group of readers.
// If this is the first time WithReaderGroup is called,
// the reader group is the descendant of the Writer.
// Otherwise, the reader group is a descendant of the
// reader group of the previously passed in WithReaderGroup().
func (b *Builder[T]) WithReaderGroup(group ...ReaderFunc) *Builder[T] {
	b.readerGroups = append(b.readerGroups, group)
	return b
}

// ReaderFunc represents a reader function.
type ReaderFunc interface {
	implementReaderFunc()
}

type singleReaderFunc[T any] struct {
	F func(*T)
}

func (singleReaderFunc[T]) implementReaderFunc() {}

// SingleReaderFunc returns a ReaderFunc that reads one at a time.
func SingleReaderFunc[T any](f func(*T)) ReaderFunc {
	return singleReaderFunc[T]{f}
}

type batchReaderFunc[T any] struct {
	F func(ptrs [2]*T, lens [2]int)
}

func (batchReaderFunc[T]) implementReaderFunc() {}

// BatchReaderFunc returns a ReaderFunc that reads in batches.
// f is essentially a function that accepts a sub-slice of the
// internal ring buffer and is called twice:
//
// 1. Once with a sub-slice to the end of the ring buffer,
// 2. Once with a sub-slice from the beginning of the ring buffer.
//
// It is possible the 2nd sub-slice is empty if n doesn't wrap around the
// ring buffer, i.e. length == 0.
//
// Use BatchReaderFunc over SingleReaderFunc only if the complexity is needed
// and if the overhead of sub-slicing is much smaller than the time saved by
// batching, e.g. when working with SIMD code to read large numbers of items
// from the disruptor.
func BatchReaderFunc[T any](f func(ptrs [2]*T, lens [2]int)) ReaderFunc {
	return batchReaderFunc[T]{f}
}

// Build builds the disruptor.
func (b *Builder[T]) Build() (*Disruptor[T], error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	d := &Disruptor[T]{
		capacity: b.capacity,
		mask:     b.capacity - 1,
		buffer:   make([]T, b.capacity),
	}
	d.readers, d.readBarrier = b.wireReaders(&d.writeCursor, &d.closer, d.buffer)
	return d, nil
}

func (b *Builder[T]) validate() error {
	if b.capacity <= 0 || b.capacity&(b.capacity-1) != 0 {
		return ErrCapacity
	}
	if len(b.readerGroups) == 0 {
		return ErrMissingReaderGroup
	}
	for _, readerGroup := range b.readerGroups {
		if len(readerGroup) == 0 {
			return ErrEmptyReaderGroup
		}
	}
	return nil
}

// wireReaders wires up the reader dependency graph.
func (b *Builder[T]) wireReaders(writeCursor *pad.AtomicInt64, writeCloser *closer.Closer, buffer []T) ([]readLooper, barrier.Barrier) {
	var readers []readLooper
	var upstreamBarrier barrier.Barrier = writeCursor
	var upstreamClosedBarrier barrier.ClosedBarrier = writeCloser
	for _, readerGroup := range b.readerGroups {
		var barrierGroup barrier.MinimumBarrier
		var closedBarrierGroup barrier.CompositeClosedBarrier
		for _, f := range readerGroup {
			var r readLooper
			var cursor *pad.AtomicInt64
			var closer *closer.Closer
			switch x := f.(type) {
			case singleReaderFunc[T]:
				r, cursor, closer = reader.NewSingleReader(upstreamBarrier, x.F, upstreamClosedBarrier, buffer)
			case batchReaderFunc[T]:
				r, cursor, closer = reader.NewBatchReader(upstreamBarrier, x.F, upstreamClosedBarrier, buffer)
			}
			readers = append(readers, r)
			barrierGroup = append(barrierGroup, cursor)
			closedBarrierGroup = append(closedBarrierGroup, closer)
		}
		upstreamBarrier = barrierGroup
		upstreamClosedBarrier = closedBarrierGroup
		// Optimize: don't need the compositeBarrier type if size 1.
		if len(barrierGroup) == 1 {
			upstreamBarrier = barrierGroup[0]
			upstreamClosedBarrier = closedBarrierGroup[0]
		}
	}
	return readers, upstreamBarrier
}

type readLooper interface {
	LoopRead()
}

// Disruptor supports a single writer and multiple readers.
type Disruptor[T any] struct {
	capacity      int64
	mask          int64
	buffer        []T
	readers       []readLooper
	readBarrier   barrier.Barrier
	slowestReader pad.Int64 // cached version of readBarrier
	closer        closer.Closer
	writeCursor   pad.AtomicInt64
	currentWriter pad.Int64 // cached version of writeCursor
}

// Write adds an item to the disruptor.
// f writes in-place into the ring buffer.
func (d *Disruptor[T]) Write(f func(item *T)) {
	if d.closer.IsClosed() {
		panic("Write() called after Close() was called.")
	}
	nextWriter := d.currentWriter.Val + 1
	d.reserve(nextWriter)
	f(&d.buffer[nextWriter&d.mask])
	d.commit(nextWriter)
}

func (d *Disruptor[T]) reserve(nextWriter int64) {
	for ; nextWriter >= d.slowestReader.Val+d.capacity; d.slowestReader.Val = d.readBarrier.Load() {
		runtime.Gosched()
	}
}

func (d *Disruptor[T]) commit(nextWriter int64) {
	d.writeCursor.Store(nextWriter)
	d.currentWriter.Val = nextWriter
}

// WriteBatch adds n items to the disruptor.
// f is essentially a function that accepts a sub-slice of the
// internal ring buffer and is called twice:
//
// 1. Once with a sub-slice to the end of the ring buffer,
// 2. Once with a sub-slice from the beginning of the ring buffer.
//
// It is possible the 2nd sub-slice is empty if n doesn't wrap around the
// ring buffer, i.e. length == 0.
//
// Use WriteBatch over Write only if the complexity is needed and if the
// overhead of sub-slicing is much smaller than the time saved by batching,
// e.g. when working with SIMD code to write large numbers of items into
// the disruptor.
func (d *Disruptor[T]) WriteBatch(n int64, f func(ptrs [2]*T, lens [2]int)) {
	if n > d.capacity {
		panic("WriteBatch() attempted to write more items than capacity allows")
	}
	if d.closer.IsClosed() {
		panic("WriteBatch() called after Close() was called.")
	}
	nextWriter := d.currentWriter.Val + n
	d.reserve(nextWriter)

	i, j := (d.currentWriter.Val+1)&d.mask, nextWriter&d.mask
	len1, len2 := unwrap(d.capacity, i, j)
	f([2]*T{&d.buffer[i], &d.buffer[0]}, [2]int{len1, len2})

	d.commit(nextWriter)
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

// LoopRead continuously reads messages
// and passes them to a provided reader(s).
// Blocks until the ring buffer is closed and empty.
func (d *Disruptor[T]) LoopRead() {
	var wg sync.WaitGroup
	for _, r := range d.readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.LoopRead()
		}()
	}
	wg.Wait()
}

// Close stops the disruptor.
func (d *Disruptor[T]) Close() {
	d.closer.Close()
}
