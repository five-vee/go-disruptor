package disruptor

import (
	"fmt"
	"runtime"
)

// SingleProducerBuilder builds a SingleProducer.
type SingleProducerBuilder[T any] struct {
	size  int64
	yield func()
}

// NewSingleProducerBuilder returns a builder of SingleProducer.
func NewSingleProducerBuilder[T any]() *SingleProducerBuilder[T] {
	return &SingleProducerBuilder[T]{}
}

// WithSize sets the ring buffer size.
// size must be a power of two.
func (b *SingleProducerBuilder[T]) WithSize(size int64) *SingleProducerBuilder[T] {
	b.size = size
	return b
}

// WithYield customizes how the producer/consumer yields
// when blocked.
// The default yield is runtime.Gosched.
func (b *SingleProducerBuilder[T]) WithYield(yield func()) *SingleProducerBuilder[T] {
	b.yield = yield
	return b
}

// Build builds the SingleProducer.
// Returns an error if the buffer is invalid.
func (b *SingleProducerBuilder[T]) Build() (*SingleProducer[T], error) {
	if b.size <= 0 || (b.size&(b.size-1)) != 0 {
		return nil, fmt.Errorf("ring buffer size must be positive power of two, got %d instead", b.size)
	}
	yield := b.yield
	if yield == nil {
		yield = runtime.Gosched
	}
	return &SingleProducer[T]{
		size:   b.size,
		mask:   b.size - 1,
		yield:  yield,
		buffer: make([]T, b.size),
	}, nil
}

// SingleProducer implements a single-producer, single-consumer lock-free ring
// buffer.
// Size must be a power of two for efficient modulo operations using
// bitmasking.
type SingleProducer[T any] struct {
	size     int64
	mask     int64 // size - 1 for quick modulo operations.
	yield    func()
	buffer   []T
	_        [64]byte // cache line padding
	producer paddedAtomicInt64
	consumer paddedAtomicInt64
}

// Produce adds an item to the buffer.
// Blocks until the buffer is no longer full.
func (sp *SingleProducer[T]) Produce(data T) {
	for {
		// Claim a sequence slot.
		currentProducer := sp.producer.Load()
		nextProducer := currentProducer + 1

		// Check buffer capacity.
		currentConsumer := sp.consumer.Load()
		if nextProducer > currentConsumer+sp.size {
			sp.yield()
			continue
		}

		// Write data.
		index := nextProducer & sp.mask
		sp.buffer[index] = data

		// Ensure visibility.
		sp.producer.Store(nextProducer)
		return
	}
}

// Consume retrieves the next item from the buffer.
// Blocks until the buffer has available data.
func (sp *SingleProducer[T]) Consume() T {
	for {
		// Check if there's available data.
		currentConsumer := sp.consumer.Load()
		currentProducer := sp.producer.Load()
		if currentConsumer >= currentProducer {
			sp.yield()
			continue
		}

		// Obtain data.
		nextConsumer := currentConsumer + 1
		index := nextConsumer & sp.mask
		data := sp.buffer[index]

		// Signal that the data has been consumed.
		sp.consumer.Store(nextConsumer)

		return data
	}
}
