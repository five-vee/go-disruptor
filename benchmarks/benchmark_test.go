package benchmark_test

import (
	"sync"
	"testing"

	"github.com/five-vee/disruptor"
)

type testData struct{ _ [16]byte }

func BenchmarkDisruptor_1_65536(b *testing.B) {
	sp, err := disruptor.NewSingleProducerBuilder[testData]().
		WithSize(1 << 16).
		Build()
	if err != nil {
		b.Fatalf("NewSingleProducerBuilder.Build() failed: %v", err)
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
		for range b.N {
			_ = sp.Consume()
		}
	}()
	wg.Wait()
}

func BenchmarkChannel_1_65536(b *testing.B) {
	c := make(chan testData, 1<<16)
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
		for range b.N {
			_ = <-c
		}
	}()
	wg.Wait()
}

func BenchmarkDisruptor_4_65536(b *testing.B) {
	mp, err := disruptor.NewMultiProducerBuilder[testData]().
		WithSize(1 << 16).
		Build()
	if err != nil {
		b.Fatalf("NewMultiProducerBuilder.Build() failed: %v", err)
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

func BenchmarkChannel_4_65536(b *testing.B) {
	c := make(chan testData, 1<<16)
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
