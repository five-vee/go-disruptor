package benchmark_test

import (
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

func BenchmarkDisruptor_22(b *testing.B) {
	const bufSize = 1 << 22
	d, _ := fivevee.NewBuilder[object](bufSize).
		WithReaderGroup(consume).
		Build()
	b.ResetTimer()
	go func() {
		defer d.Close()
		for b.Loop() {
			d.Write(object{})
		}
	}()
	d.LoopRead()
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

func BenchmarkSmartystreets_22(b *testing.B) {
	const bufSize = 1 << 22
	ringBuffer := make([]object, bufSize)
	mask := int64(bufSize - 1)
	d := smartystreets.New(
		smartystreets.WithCapacity(bufSize),
		smartystreets.WithConsumerGroup(smartystreetsConsumer{mask, ringBuffer}),
	)
	produce := func() object {
		return object{}
	}
	b.ResetTimer()
	go func() {
		for range b.N {
			sequence := d.Reserve(1)
			o := produce()
			ringBuffer[sequence&mask] = o
			d.Commit(sequence, sequence)
		}
		_ = d.Close()
	}()
	d.Read()
}

func BenchmarkChannel_22(b *testing.B) {
	c := make(chan object, 1<<22)
	b.ResetTimer()
	go func() {
		for range b.N {
			c <- object{}
		}
	}()
	for range b.N {
		consume(<-c)
	}
}
