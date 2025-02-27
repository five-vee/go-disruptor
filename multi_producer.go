package disruptor

import (
	"fmt"
	"runtime"
)

// MultiProducerBuilder builds a MultiProducer.
type MultiProducerBuilder[T any] struct {
	size  int64
	yield func()
}

// NewMultiProducerBuilder returns a builder of MultiProducer.
func NewMultiProducerBuilder[T any]() *MultiProducerBuilder[T] {
	return &MultiProducerBuilder[T]{}
}

// WithSize sets the ring buffer size.
// size must be a power of two.
func (b *MultiProducerBuilder[T]) WithSize(size int64) *MultiProducerBuilder[T] {
	b.size = size
	return b
}

// WithYield customizes how the producer/consumer yields
// when blocked.
// The default yield is runtime.Gosched.
func (b *MultiProducerBuilder[T]) WithYield(yield func()) *MultiProducerBuilder[T] {
	b.yield = yield
	return b
}

// Build builds the MultiProducer.
// Returns an error if the buffer is invalid.
func (b *MultiProducerBuilder[T]) Build() (*MultiProducer[T], error) {
	if b.size <= 0 || (b.size&(b.size-1)) != 0 {
		return nil, fmt.Errorf("ring buffer size must be positive power of two, got %d instead", b.size)
	}
	yield := b.yield
	if yield == nil {
		yield = runtime.Gosched
	}
	return &MultiProducer[T]{
		size:   b.size,
		mask:   b.size - 1,
		yield:  yield,
		buffer: make([]T, b.size),
	}, nil
}

// MultiProducer implements a multi-producer, single-consumer lock-free ring
// buffer.
// Size must be a power of two for efficient modulo operations using
// bitmasking.
type MultiProducer[T any] struct {
	size      int64
	mask      int64 // size - 1 for quick modulo operations.
	yield     func()
	_         [64]byte // cache line padding
	producer  paddedAtomicInt64
	published paddedAtomicInt64
	consumer  paddedAtomicInt64
	buffer    []T
}

// Produce adds an item to the buffer.
// Blocks until the buffer is no longer full.
func (mp *MultiProducer[T]) Produce(data T) {
	for {
		// Prepare to claim a sequence slot.
		currentProducer := mp.producer.Load()
		nextProducer := currentProducer + 1

		// Check buffer capacity.
		currentConsumer := mp.consumer.Load()
		if nextProducer > currentConsumer+mp.size {
			mp.yield()
			continue
		}

		// Attempt to claim the slot.
		if !mp.producer.CompareAndSwap(currentProducer, nextProducer) {
			mp.yield()
			continue
		}

		// Write data.
		index := nextProducer & mp.mask
		mp.buffer[index] = data

		// Ensure visibility.
		mp.updatePublished(nextProducer)
		return
	}
}

func (mp *MultiProducer[T]) updatePublished(nextPublished int64) {
	for {
		currentPublished := mp.published.Load()
		if nextPublished != currentPublished+1 {
			// Wait for previous sequence(s) to be published.
			mp.yield()
			continue
		}
		if mp.published.CompareAndSwap(currentPublished, nextPublished) {
			return // Successfully updated published sequence.
		}
	}
}

// Consume retrieves the next item from the buffer.
// Blocks until the buffer has available data.
func (mp *MultiProducer[T]) Consume() T {
	for {
		// Check if there's available data.
		currentConsumer := mp.consumer.Load()
		currentPublished := mp.published.Load()
		if currentConsumer >= currentPublished {
			mp.yield()
			continue
		}

		// Obtain data.
		nextSeq := currentConsumer + 1
		index := nextSeq & mp.mask
		data := mp.buffer[index]

		// Signal that the data has been consumed.
		mp.consumer.Store(nextSeq)

		return data
	}
}
