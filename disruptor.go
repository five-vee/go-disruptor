//go:build amd64 || arm64

// Package disruptor is an implementation of the LMAX disruptor.
package disruptor

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

const cacheLinePadSize = 64

// RingBuffer implements a single-producer, single-consumer lock-free ring
// buffer.
// Size must be a power of two for efficient modulo operations using
// bitmasking.
type RingBuffer[T any] struct {
	size        int64
	mask        int64 // size - 1 for quick modulo operations.
	buffer      []T
	_           [cacheLinePadSize]byte
	producerSeq int64 // Atomic: tracks the next write position.
	_           [cacheLinePadSize]byte
	consumerSeq int64 // Atomic: tracks the last read position.
	_           [cacheLinePadSize]byte
}

// NewRingBuffer creates a new ring buffer with the given size.
func NewRingBuffer[T any](size int64) (*RingBuffer[T], error) {
	if size <= 0 || (size&(size-1)) != 0 {
		return nil, fmt.Errorf("ring buffer size must be positive power of two, got %d instead", size)
	}
	return &RingBuffer[T]{
		size:   size,
		mask:   size - 1,
		buffer: make([]T, size),
	}, nil
}

// Publish adds an item to the buffer. Blocks until space is available.
func (rb *RingBuffer[T]) Publish(data T) {
	for {
		currentProducerSeq := atomic.LoadInt64(&rb.producerSeq)
		nextSeq := currentProducerSeq + 1
		currentConsumerSeq := atomic.LoadInt64(&rb.consumerSeq)

		// Check if buffer is full (producer can't wrap around consumer).
		if nextSeq > currentConsumerSeq+rb.size-1 {
			// Use CAS would be required for multiple producers here.
			runtime.Gosched() // Yield instead of busy-wait.
			continue
		}

		// Write data before updating producer sequence to ensure visibility.
		index := nextSeq & rb.mask
		rb.buffer[index] = data

		// Atomic store ensures consumer sees sequence update after data write.
		atomic.StoreInt64(&rb.producerSeq, nextSeq)
		return
	}
}

// Consume retrieves the next item from the buffer.
// Blocks until item is available.
func (rb *RingBuffer[T]) Consume() T {
	for {
		currentConsumerSeq := atomic.LoadInt64(&rb.consumerSeq)
		currentProducerSeq := atomic.LoadInt64(&rb.producerSeq)

		if currentConsumerSeq >= currentProducerSeq {
			runtime.Gosched() // Yield instead of busy-wait.
			continue
		}

		nextSeq := currentConsumerSeq + 1
		index := nextSeq & rb.mask
		data := rb.buffer[index]

		// Atomic store ensures producer sees consumer progress.
		atomic.StoreInt64(&rb.consumerSeq, nextSeq)
		return data
	}
}
