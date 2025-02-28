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
	ErrCapacity           = fmt.Errorf("capacity must be a power of two")
	ErrNoSingularConsumer = fmt.Errorf("must pass in exactly one consumer")
)

// Disruptor supports only a single producer and single consumer.
type Disruptor[T any] struct {
	capacity         int64
	mask             int64
	yieldProducer    func()
	yieldConsumer    func()
	consumer         func(T)
	buffer           []T
	_                [64]byte
	producerSequence paddedAtomicInt64
	consumerSequence paddedAtomicInt64
	state            paddedAtomicInt64
}

// NewDisruptor returns a new disruptor.
func NewDisruptor[T any](capacity int64, consumer func(T)) (*Disruptor[T], error) {
	if capacity <= 0 || capacity&(capacity-1) != 0 {
		return nil, ErrCapacity
	}
	d := &Disruptor[T]{
		capacity:      capacity,
		mask:          capacity - 1,
		yieldProducer: runtime.Gosched,
		yieldConsumer: runtime.Gosched,
		consumer:      consumer,
		buffer:        make([]T, capacity),
	}
	d.state.Store(openBuffer)
	return d, nil
}

// Produce adds an item to the disruptor.
func (d *Disruptor[T]) Produce(item T) {
	if d.state.Load() == closedBuffer {
		panic("Produce() called after Close() was called.")
	}
	currentProducerSequence := d.producerSequence.Load()
	nextProducerSequence := currentProducerSequence + 1
	for {
		currentConsumerSequence := d.consumerSequence.Load()
		if nextProducerSequence < currentConsumerSequence+d.capacity {
			break
		}
		d.yieldProducer()
	}
	d.buffer[nextProducerSequence&d.mask] = item
	d.producerSequence.Store(nextProducerSequence)
}

// LoopConsume continuously consumes messages
// and passes them to a provided consumer.
// Blocks until Close() is called.
func (d *Disruptor[T]) LoopConsume() {
	currentConsumerSequence := d.consumerSequence.Load()
	for {
		currentProducerSequence := d.producerSequence.Load()
		if currentConsumerSequence == currentProducerSequence {
			if d.state.Load() == closedBuffer {
				return
			}
			d.yieldConsumer()
			continue
		}
		for seq := currentConsumerSequence + 1; seq <= currentProducerSequence; seq++ {
			d.consumer(d.buffer[seq&d.mask])
		}
		d.consumerSequence.Store(currentProducerSequence)
		currentConsumerSequence = currentProducerSequence
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
