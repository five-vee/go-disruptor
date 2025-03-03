package benchmark_test

import (
	"testing"

	fivevee "github.com/five-vee/go-disruptor"
	smartystreets "github.com/smartystreets-prototypes/go-disruptor"
)

type object struct{ x [128]byte }

// a produce function that just accepts an object
// without needing to deal with ring buffer internals.
func produce(o *object) {
	o.x[0] = '0'
}

// a consume function that just accepts an object
// without needing to deal with ring buffer internals.
func consume(o *object) {
	_ = o.x[0]
}

func BenchmarkDisruptor_22(b *testing.B) {
	const bufSize = 1 << 22
	d, _ := fivevee.NewBuilder[object](bufSize).
		WithReaderGroup(fivevee.SingleReaderFunc(consume)).
		Build()
	b.ResetTimer()
	go func() {
		defer d.Close()
		for b.Loop() {
			d.Write(produce)
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
		consume(&c.ringBuffer[seq&c.mask])
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
	b.ResetTimer()
	go func() {
		for range b.N {
			sequence := d.Reserve(1)
			produce(&ringBuffer[sequence&mask])
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
		defer close(c)
		for b.Loop() {
			c <- object{}
		}
	}()
	for o := range c {
		_ = o
	}
}
