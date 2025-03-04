package disruptor

import (
	"fmt"
	"runtime"
	"time"

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
	writerYield  func(spins int)
	readerYield  func()
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

// WithWriterYield overrides how Write/WriteBatch yields
// when the buffer is full. yield receives the number of times
// yield has been called so far in a Write/WriteBatch call.
func (b *Builder[T]) WithWriterYield(yield func(spins int)) *Builder[T] {
	b.writerYield = yield
	return b
}

// WithReaderYield overrides how ReadLoop yields when the buffer is empty.
func (b *Builder[T]) WithReaderYield(yield func()) *Builder[T] {
	b.readerYield = yield
	return b
}

// Build builds the disruptor.
func (b *Builder[T]) Build() (*Disruptor[T], error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	writerYield := func(spins int) {
		const spinMask = (1 << 14) - 1
		if spins&spinMask == 0 {
			runtime.Gosched()
		}
	}
	if b.writerYield != nil {
		writerYield = b.writerYield
	}
	readerYield := func() {
		time.Sleep(50 * time.Microsecond)
	}
	if b.readerYield != nil {
		readerYield = b.readerYield
	}
	d := &Disruptor[T]{
		capacity:    b.capacity,
		mask:        b.capacity - 1,
		buffer:      make([]T, b.capacity),
		writerYield: writerYield,
	}
	d.readers, d.readBarrier = b.wireReaders(&d.writeCursor, &d.closer, d.buffer, readerYield)
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
func (b *Builder[T]) wireReaders(writeCursor *pad.AtomicInt64, writeCloser *closer.Closer, buffer []T, readerYield func()) ([]readLooper, barrier.Barrier) {
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
				r, cursor, closer = reader.NewSingleReader(upstreamBarrier, x.F, upstreamClosedBarrier, buffer, readerYield)
			case batchReaderFunc[T]:
				r, cursor, closer = reader.NewBatchReader(upstreamBarrier, x.F, upstreamClosedBarrier, buffer, readerYield)
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
// 1. First sub-slice is to the end of the ring buffer,
// 2. Secondsub-slice is from the beginning of the ring buffer.
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
