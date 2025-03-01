//go:build amd64 || arm64

package disruptor

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

const (
	openBuffer   = 0
	closedBuffer = 1
)

var (
	// ErrCapacity is the error corresponding to wrong capacity.
	ErrCapacity = fmt.Errorf("capacity must be a power of two")
)

// Disruptor supports only a single writer and single reader.
type Disruptor[T any] struct {
	capacity int64
	mask     int64
	read     func(T)
	buffer   []T

	_ [64]byte

	writeCursor cursor
	readCursor  cursor
	state       paddedAtomicInt64
}

// NewDisruptor returns a new disruptor.
func NewDisruptor[T any](capacity int64, read func(T)) (*Disruptor[T], error) {
	if capacity <= 0 || capacity&(capacity-1) != 0 {
		return nil, ErrCapacity
	}
	d := &Disruptor[T]{
		capacity:    capacity,
		mask:        capacity - 1,
		read:        read,
		buffer:      make([]T, capacity),
		writeCursor: &paddedAtomicInt64{},
		readCursor:  &paddedAtomicInt64{},
	}
	d.state.Store(openBuffer)
	return d, nil
}

// Write adds an item to the disruptor.
func (d *Disruptor[T]) Write(item T) {
	if d.state.Load() == closedBuffer {
		panic("Write() called after Close() was called.")
	}
	currentWriter := d.writeCursor.Load()
	nextWriter := currentWriter + 1
	for {
		currentReader := d.readCursor.Load()
		if nextWriter < currentReader+d.capacity {
			break
		}
		runtime.Gosched()
	}
	d.buffer[nextWriter&d.mask] = item
	d.writeCursor.Store(nextWriter)
}

// LoopRead continuously reads messages
// and passes them to a provided reader.
// Blocks until Close() is called.
func (d *Disruptor[T]) LoopRead() {
	currentReader := d.readCursor.Load()
	for {
		currentWriter := d.writeCursor.Load()
		if currentReader == currentWriter {
			if d.state.Load() == closedBuffer {
				return
			}
			runtime.Gosched()
			continue
		}
		for seq := currentReader + 1; seq <= currentWriter; seq++ {
			d.read(d.buffer[seq&d.mask])
		}
		d.readCursor.Store(currentWriter)
		currentReader = currentWriter
	}
}

// Close stops the disruptor.
func (d *Disruptor[T]) Close() {
	d.state.Store(closedBuffer)
}

type paddedAtomicInt64 struct {
	atomic.Int64
	_ [56]byte
}

type cursor interface {
	Load() int64
	Store(int64)
}
