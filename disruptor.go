//go:build amd64 || arm64

// Package disruptor is an implementation of the LMAX disruptor.
package disruptor

import (
	"fmt"
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

// TryPublish trys to add an item to the buffer. Returns false if blocked.
func (rb *RingBuffer[T]) TryPublish(data T) bool {
	currentProducerSeq := atomic.LoadInt64(&rb.producerSeq)
	nextSeq := currentProducerSeq + 1
	currentConsumerSeq := atomic.LoadInt64(&rb.consumerSeq)

	// Check if buffer is full (producer can't wrap around consumer).
	if nextSeq > currentConsumerSeq+rb.size {
		return false
	}

	// Write data before updating producer sequence to ensure visibility.
	index := nextSeq & rb.mask
	rb.buffer[index] = data

	// Atomic store ensures consumer sees sequence update after data write.
	atomic.StoreInt64(&rb.producerSeq, nextSeq)
	return true
}

// TryConsume tries to retrieve the next item from the buffer.
// bool=false if blocked.
func (rb *RingBuffer[T]) TryConsume() (T, bool) {
	currentConsumerSeq := atomic.LoadInt64(&rb.consumerSeq)
	currentProducerSeq := atomic.LoadInt64(&rb.producerSeq)

	if currentConsumerSeq >= currentProducerSeq {
		var zero T
		return zero, false
	}

	nextSeq := currentConsumerSeq + 1
	index := nextSeq & rb.mask
	data := rb.buffer[index]

	// Atomic store ensures producer sees consumer progress.
	atomic.StoreInt64(&rb.consumerSeq, nextSeq)
	return data, true
}
