package benchmark_test

import (
	"sync"
	"testing"

	fivevee "github.com/five-vee/disruptor"
	smartystreets "github.com/smartystreets-prototypes/go-disruptor"
)

type object struct{ _ [16]byte }

// a consumer function that just accepts an object
// without needing to deal with ring buffer internals.
func consume[T any](item T) {
	_ = item
}

// consumer to be used by the smartystreets disruptor.
type smartystreetsConsumer struct {
	mask       int64
	ringBuffer []object
}

func (c smartystreetsConsumer) Consume(lower, upper int64) {
	for seq := lower; seq <= upper; seq++ {
		consume(c.ringBuffer[seq&c.mask])
	}
}

func BenchmarkSmartystreets_1_20(b *testing.B) {
	ringBuffer := make([]object, 1<<20)
	mask := int64((1 << 20) - 1)
	disruptor := smartystreets.New(
		smartystreets.WithCapacity(1<<20),
		smartystreets.WithConsumerGroup(smartystreetsConsumer{mask, ringBuffer}),
	)
	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range b.N {
			sequence := disruptor.Reserve(1)
			ringBuffer[sequence&mask] = object{}
			disruptor.Commit(sequence, sequence)
		}
		_ = disruptor.Close()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		disruptor.Read()
	}()
	wg.Wait()
}

func BenchmarkDisruptor_1_20(b *testing.B) {
	const bufSize = 1 << 20
	d, _ := fivevee.NewDisruptor[object](bufSize, consume)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer d.Close()
		for b.Loop() {
			d.Produce(object{})
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.LoopConsume()
	}()
	wg.Wait()
}

func BenchmarkChannel_1_20(b *testing.B) {
	c := make(chan object, 1<<20)
	for range 1 << 19 {
		c <- object{}
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range b.N {
			c <- object{}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range (1 << 19) + b.N {
			consume(<-c)
		}
	}()
	wg.Wait()
}
