package benchmark_test

import (
	"sync"
	"testing"

	fivevee "github.com/five-vee/disruptor"
	smartystreets "github.com/smartystreets-prototypes/go-disruptor"
)

type testData struct{ _ [16]byte }

func BenchmarkDisruptor_1_20(b *testing.B) {
	sp, err := fivevee.NewSingleProducerBuilder[testData]().
		WithSize(1 << 20).
		Build()
	if err != nil {
		b.Fatalf("NewSingleProducerBuilder.Build() failed: %v", err)
	}
	for range 1 << 19 {
		sp.Produce(testData{})
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range b.N {
			sp.Produce(testData{})
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range (1 << 19) + b.N {
			_ = sp.Consume()
		}
	}()
	wg.Wait()
}

// consumer to be used by the smartystreets disruptor.
type smartystreetsConsumer struct {
	mask       int64
	ringBuffer []testData
}

func (c smartystreetsConsumer) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
		_ = c.ringBuffer[lower&c.mask]
	}
}

func BenchmarkSmartystreets_1_20(b *testing.B) {
	ringBuffer := make([]testData, 1<<20)
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
			ringBuffer[sequence&mask] = testData{}
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

func BenchmarkChannel_1_20(b *testing.B) {
	c := make(chan testData, 1<<20)
	for range 1 << 19 {
		c <- testData{}
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range b.N {
			c <- testData{}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range (1 << 19) + b.N {
			_ = <-c
		}
	}()
	wg.Wait()
}

func BenchmarkDisruptor_4_20(b *testing.B) {
	mp, err := fivevee.NewMultiProducerBuilder[testData]().
		WithSize(1 << 20).
		Build()
	if err != nil {
		b.Fatalf("NewMultiProducerBuilder.Build() failed: %v", err)
	}
	for range 1 << 19 {
		mp.Produce(testData{})
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range b.N {
				mp.Produce(testData{})
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 4 * b.N {
			_ = mp.Consume()
		}
	}()
	wg.Wait()
}

func BenchmarkChannel_4_20(b *testing.B) {
	c := make(chan testData, 1<<20)
	for range 1 << 19 {
		c <- testData{}
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range b.N {
				c <- testData{}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 4 * b.N {
			_ = <-c
		}
	}()
	wg.Wait()
}
