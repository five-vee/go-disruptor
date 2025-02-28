package main

import (
	"fmt"
	"log"
	"runtime"
	"sync"
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
	yieldProducer    func(int)
	yieldConsumer    func(int)
	consumer         func(T)
	buffer           []T
	_                [64]byte
	producerSequence paddedAtomicInt64
	consumerSequence paddedAtomicInt64
	state            paddedAtomicInt64
}

// NewDisruptor returns a new disruptor.
func NewDisruptor[T any](capacity int64, opts ...Option[T]) (*Disruptor[T], error) {
	if capacity <= 0 || capacity&(capacity-1) != 0 {
		return nil, ErrCapacity
	}
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	d := &Disruptor[T]{
		capacity:      capacity,
		mask:          capacity - 1,
		yieldProducer: func(int) { runtime.Gosched() },
		yieldConsumer: func(int) { runtime.Gosched() },
		buffer:        make([]T, capacity),
	}
	for _, opt := range opts {
		opt.f(d)
	}
	d.state.Store(openBuffer)
	return d, nil
}

// Produce adds an item to the disruptor.
func (d *Disruptor[T]) Produce(item T) {
	currentProducerSequence := d.producerSequence.Load()
	nextProducerSequence := currentProducerSequence + 1
	count := 0
	for {
		currentConsumerSequence := d.consumerSequence.Load()
		if nextProducerSequence < currentConsumerSequence+d.capacity {
			break
		}
		count++
		d.yieldProducer(count)
	}
	d.buffer[nextProducerSequence&d.mask] = item
	d.producerSequence.Store(nextProducerSequence)
}

// Consume continuously consumes messages
// and passes them to a provided consumer.
// Blocks until Close() is called.
func (d *Disruptor[T]) Consume() {
	currentConsumerSequence := d.consumerSequence.Load()
	count := 0
	for {
		if d.state.Load() == closedBuffer {
			return
		}
		currentProducerSequence := d.producerSequence.Load()
		if currentConsumerSequence == currentProducerSequence {
			count++
			d.yieldConsumer(count)
			continue
		}
		for nextConsumerSequence := currentConsumerSequence + 1; nextConsumerSequence <= currentProducerSequence; nextConsumerSequence++ {
			d.consumer(d.buffer[nextConsumerSequence&d.mask])
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

func validateOptions[T any](opts []Option[T]) error {
	consumerCount := 0
	for _, opt := range opts {
		switch opt.(type) {
		case withConsumer[T]:
			consumerCount++
		}
	}
	if consumerCount != 1 {
		return ErrNoSingularConsumer
	}
	return nil
}

// Option customizes the disruptor.
type Option[T any] interface {
	f(*Disruptor[T])
}

type withConsumer[T any] struct {
	consumer func(T)
}

// WithConsumer sets the consumer of the disruptor.
// It is an error if this is passed more than once to NewDisruptor.
func WithConsumer[T any](consumer func(T)) Option[T] {
	return withConsumer[T]{consumer}
}

func (w withConsumer[T]) f(d *Disruptor[T]) {
	d.consumer = w.consumer
}

func main() {
	const (
		numItems = 1 << 20
		bufSize  = 1 << 12
	)
	var (
		produced int
		consumed int
	)
	type object struct{ _ [12]byte }
	consumer := func(o object) {
		_ = o
		consumed++
	}
	d, err := NewDisruptor(bufSize,
		WithConsumer(consumer))
	if err != nil {
		log.Fatalf("Failed to create a new disruptor: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer d.Close()
		for range numItems {
			d.Produce(object{})
			produced++
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.Consume()
	}()
	wg.Wait()
	fmt.Printf("Produced %d items\n", produced)
	fmt.Printf("Consumed %d items\n", consumed)
}
