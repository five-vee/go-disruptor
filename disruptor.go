//go:build amd64 || arm64

// Package disruptor is an implementation of the LMAX disruptor.
package disruptor

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

// RingBufferBuilder builds a RingBuffer.
type RingBufferBuilder[T any] struct {
	size  int64
	yield func()
}

// NewRingBufferBuilder returns a builder of RingBuffer.
func NewRingBufferBuilder[T any]() *RingBufferBuilder[T] {
	return &RingBufferBuilder[T]{}
}

// WithSize sets the ring buffer size.
// size must be a power of two.
func (b *RingBufferBuilder[T]) WithSize(size int64) *RingBufferBuilder[T] {
	b.size = size
	return b
}

// WithYield customizes how the producer/consumer yields
// when blocked.
// The default yield is runtime.Gosched.
func (b *RingBufferBuilder[T]) WithYield(yield func()) *RingBufferBuilder[T] {
	b.yield = yield
	return b
}

// Build builds the RingBuffer.
// Returns an error if the buffer is invalid.
func (b *RingBufferBuilder[T]) Build() (*RingBuffer[T], error) {
	if b.size <= 0 || (b.size&(b.size-1)) != 0 {
		return nil, fmt.Errorf("ring buffer size must be positive power of two, got %d instead", b.size)
	}
	yield := b.yield
	if yield == nil {
		yield = runtime.Gosched
	}
	return &RingBuffer[T]{
		size:   b.size,
		mask:   b.size - 1,
		yield:  yield,
		buffer: make([]T, b.size),
	}, nil
}

type paddedAtomicInt64 struct {
	atomic.Int64
	_ [56]byte // cache line padding (64 bytes total)
}

// RingBuffer implements a single-producer, single-consumer lock-free ring
// buffer.
// Size must be a power of two for efficient modulo operations using
// bitmasking.
type RingBuffer[T any] struct {
	size        int64
	mask        int64 // size - 1 for quick modulo operations.
	yield       func()
	buffer      []T
	_           [64]byte // cache line padding
	producerSeq paddedAtomicInt64
	consumerSeq paddedAtomicInt64
}

// Produce adds an item to the buffer.
// Blocks until the buffer is no longer full.
func (rb *RingBuffer[T]) Produce(data T) {
	for {
		// Prepare to claim a sequence slot.
		currentProducerSeq := rb.producerSeq.Load()
		nextSeq := currentProducerSeq + 1

		// Check buffer capacity.
		currentConsumerSeq := rb.consumerSeq.Load()

		// Attempt to claim the slot.
		if nextSeq > currentConsumerSeq+rb.size {
			rb.yield()
			continue
		}

		// Write data.
		index := nextSeq & rb.mask
		rb.buffer[index] = data

		// Ensure visibility.
		rb.producerSeq.Store(nextSeq)
		return
	}
}

// Consume retrieves the next item from the buffer.
// Blocks until the buffer has available data.
func (rb *RingBuffer[T]) Consume() T {
	for {
		// Check if there's available data.
		currentConsumerSeq := rb.consumerSeq.Load()
		currentProducerSeq := rb.producerSeq.Load()
		if currentConsumerSeq >= currentProducerSeq {
			rb.yield()
			continue
		}

		// Obtain data.
		nextSeq := currentConsumerSeq + 1
		index := nextSeq & rb.mask
		data := rb.buffer[index]

		// Signal that the data has been consumed.
		rb.consumerSeq.Store(nextSeq)

		return data
	}
}
