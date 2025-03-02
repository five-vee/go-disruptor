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

	"github.com/five-vee/disruptor/internal/barrier"
	"github.com/five-vee/disruptor/internal/closer"
	"github.com/five-vee/disruptor/internal/pad"
	"github.com/five-vee/disruptor/internal/reader"
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
	readerGroups [][]func(*T)
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
func (b *Builder[T]) WithReaderGroup(group ...func(*T)) *Builder[T] {
	b.readerGroups = append(b.readerGroups, group)
	return b
}

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
func (b *Builder[T]) wireReaders(writeCursor *pad.AtomicInt64, writeCloser *closer.Closer, buffer []T) ([]*reader.Reader[T], barrier.Barrier) {
	var readers []*reader.Reader[T]
	var upstreamBarrier barrier.Barrier = writeCursor
	var upstreamClosedBarrier barrier.ClosedBarrier = writeCloser
	for _, readerGroup := range b.readerGroups {
		var barrierGroup barrier.MinimumBarrier
		var closedBarrierGroup barrier.CompositeClosedBarrier
		for _, f := range readerGroup {
			r, cursor, state := reader.NewReader(upstreamBarrier, f, upstreamClosedBarrier, buffer)
			readers = append(readers, r)
			barrierGroup = append(barrierGroup, cursor)
			closedBarrierGroup = append(closedBarrierGroup, state)
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

// Disruptor supports a single writer and multiple readers.
type Disruptor[T any] struct {
	capacity      int64
	mask          int64
	buffer        []T
	readers       []*reader.Reader[T]
	readBarrier   barrier.Barrier
	slowestReader pad.Int64 // cached version of readBarrier
	closer        closer.Closer
	writeCursor   pad.AtomicInt64
	currentWriter pad.Int64 // cached version of writeCursor
}

// Write adds an item to the disruptor.
func (d *Disruptor[T]) Write(f func(item *T)) {
	if d.closer.IsClosed() {
		panic("Write() called after Close() was called.")
	}
	nextWriter := d.currentWriter.Val + 1
	for ; nextWriter >= d.slowestReader.Val+d.capacity; d.slowestReader.Val = d.readBarrier.Load() {
		runtime.Gosched()
	}
	f(&d.buffer[nextWriter&d.mask])
	d.writeCursor.Store(nextWriter)
	d.currentWriter.Val = nextWriter
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
