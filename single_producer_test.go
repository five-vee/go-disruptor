package disruptor

import (
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSingleProducerBuilder_Build(t *testing.T) {
	testCases := []struct {
		name    string
		builder *SingleProducerBuilder[int]
		wantErr bool
	}{
		{
			name:    "valid size and default yield",
			builder: NewSingleProducerBuilder[int]().WithSize(8),
			wantErr: false,
		},
		{
			name: "valid size and custom yield",
			builder: NewSingleProducerBuilder[int]().WithSize(8).WithYield(func() {
				// Custom yield implementation (no-op for testing)
			}),
			wantErr: false,
		},
		{
			name:    "invalid size - not power of two",
			builder: NewSingleProducerBuilder[int]().WithSize(7),
			wantErr: true,
		},
		{
			name:    "invalid size - zero",
			builder: NewSingleProducerBuilder[int]().WithSize(0),
			wantErr: true,
		},
		{
			name:    "invalid size - negative",
			builder: NewSingleProducerBuilder[int]().WithSize(-8),
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sp, err := tc.builder.Build()
			if (err != nil) != tc.wantErr {
				t.Fatalf("RingBufferBuilder.Build() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && sp == nil {
				t.Fatalf("RingBufferBuilder.Build() returned nil SingleProducer, want non-nil")
			}
		})
	}
}

// Functional tests.
func TestSingleProducer_ProduceAndConsume(t *testing.T) {
	type testData struct {
		id int
	}

	t.Run("producer blocked by consumer", func(*testing.T) {
		bufferSize := int64(2)
		signal := make(chan struct{})

		sp, _ := NewSingleProducerBuilder[testData]().
			WithSize(bufferSize).
			WithYield(func() {
				signal <- struct{}{} // Signal that the producer is blocked.
				signal <- struct{}{} // Signal that the producer is unblocked.
			}).
			Build()

		// Produce 2 items to fill the buffer.
		sp.Produce(testData{1})
		sp.Produce(testData{2})

		// Launch a goroutine to produce a 3rd item, which should block.
		var produceDone sync.WaitGroup
		produceDone.Add(1)
		go func() {
			defer produceDone.Done()
			sp.Produce(testData{3})
		}()

		// Wait for the signal that the producer is blocked.
		<-signal

		// Consume 1 item, which should unblock the producer.
		_ = sp.Consume()

		// Signal that the producer is no longer blocked.
		<-signal

		// Wait for the produce goroutine to complete.
		produceDone.Wait()
	})

	t.Run("consumer blocked by produceer", func(*testing.T) {
		bufferSize := int64(1)
		signal := make(chan struct{})

		sp, _ := NewSingleProducerBuilder[testData]().
			WithSize(bufferSize).
			WithYield(func() {
				signal <- struct{}{} // Signal that the consumer is blocked.
				signal <- struct{}{} // Signal that the consumer is unblocked.
			}).
			Build()

		// Launch a goroutine to consume an item, which should block.
		var consumeDone sync.WaitGroup
		consumeDone.Add(1)
		go func() {
			defer consumeDone.Done()
			_ = sp.Consume()
		}()

		// Wait for the signal that the consumer is blocked.
		<-signal

		// Produce 1 item, which should unblock the consumer.
		sp.Produce(testData{id: 1})

		// Signal that the consumer is no longer blocked.
		<-signal

		// Wait for the consume goroutine to complete.
		consumeDone.Wait()
	})

	t.Run("consumer observes producer", func(t *testing.T) {
		bufferSize := int64(2)
		sp, _ := NewSingleProducerBuilder[testData]().WithSize(bufferSize).Build()

		// Produce 2 items.
		var produceDone sync.WaitGroup
		produceDone.Add(1)
		go func() {
			defer produceDone.Done()
			sp.Produce(testData{id: 1})
			sp.Produce(testData{id: 2})
		}()
		produceDone.Wait()

		// Consume 1 item.
		data1 := sp.Consume()
		if data1.id != 1 {
			t.Errorf("Consume() consumed wrong data, got %+v, want %+v", data1, testData{1})
		}

		// Consume another item.
		data2 := sp.Consume()
		if data2.id != 2 {
			t.Errorf("Consume() consumed wrong data, got %+v, want %+v", data2, testData{2})
		}
	})

	t.Run("produce and consume multiple items", func(t *testing.T) {
		bufferSize := int64(4)
		sp, _ := NewSingleProducerBuilder[testData]().WithSize(bufferSize).Build()

		const numItems = 4
		var produceDone sync.WaitGroup
		produceDone.Add(1)
		go func() {
			defer produceDone.Done()
			for i := 1; i <= numItems; i++ {
				sp.Produce(testData{id: i})
			}
		}()

		var consumeDone sync.WaitGroup
		consumeDone.Add(1)
		go func() {
			defer consumeDone.Done()
			for i := 1; i <= numItems; i++ {
				data := sp.Consume()
				if data.id != i {
					t.Errorf("Consume() consumed wrong data, got %+v, want %+v", data, testData{i})
				}
			}
		}()
		produceDone.Wait()
		consumeDone.Wait()
	})
}

// Smoke test to provide coverage of concurrent producer/consumer.
func TestSingleProducer_SmokeTest(t *testing.T) {
	const n = 500_000
	const bufferSize = 1 << 12
	type testData struct{ i int }
	mp, _ := NewSingleProducerBuilder[testData]().WithSize(bufferSize).Build()
	// 1 producer, 1 consumer.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 1; i <= n; i++ {
			mp.Produce(testData{i})
		}
	}()
	const diffLimit = 10 // don't show too many diffs
	var (
		totalDiffs int
		wants      []int
		gots       []int
	)
	go func() {
		defer wg.Done()
		for want := 1; want <= n; want++ {
			got := mp.Consume()
			if got.i == want {
				continue
			}
			totalDiffs++
			if len(gots) < diffLimit {
				gots = append(gots, got.i)
				wants = append(wants, want)
			}
		}
	}()
	wg.Wait()
	diff := cmp.Diff(wants, gots)
	if diff == "" {
		return
	}
	if totalDiffs > diffLimit {
		t.Errorf("Consume() received different data (-want +got, truncated to %d diffs):\n%s",
			diffLimit, diff)
	} else {
		t.Errorf("Consume() received different data (-want +got):\n%s", diff)
	}
}
