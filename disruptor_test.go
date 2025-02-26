package disruptor

import (
	"sync"
	"testing"
)

type testData struct {
	id int
}

func TestRingBufferBuilder_Build(t *testing.T) {
	testCases := []struct {
		name    string
		builder *RingBufferBuilder[testData]
		wantErr bool
	}{
		{
			name:    "valid size and default yield",
			builder: NewRingBufferBuilder[testData]().WithSize(8),
			wantErr: false,
		},
		{
			name: "valid size and custom yield",
			builder: NewRingBufferBuilder[testData]().WithSize(8).WithYield(func() {
				// Custom yield implementation (no-op for testing)
			}),
			wantErr: false,
		},
		{
			name:    "invalid size - not power of two",
			builder: NewRingBufferBuilder[testData]().WithSize(7),
			wantErr: true,
		},
		{
			name:    "invalid size - zero",
			builder: NewRingBufferBuilder[testData]().WithSize(0),
			wantErr: true,
		},
		{
			name:    "invalid size - negative",
			builder: NewRingBufferBuilder[testData]().WithSize(-8),
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rb, err := tc.builder.Build()
			if (err != nil) != tc.wantErr {
				t.Fatalf("RingBufferBuilder.Build() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && rb == nil {
				t.Fatalf("RingBufferBuilder.Build() returned nil RingBuffer, want non-nil")
			}
		})
	}
}

func TestRingBuffer_produceAndConsume(t *testing.T) {
	t.Run("produceer blocked by consumer", func(*testing.T) {
		bufferSize := int64(2)
		signal := make(chan struct{})

		rb, _ := NewRingBufferBuilder[testData]().
			WithSize(bufferSize).
			WithYield(func() {
				signal <- struct{}{} // Signal that the producer is blocked.
				signal <- struct{}{} // Signal that the producer is unblocked.
			}).
			Build()

		// Produce 2 items to fill the buffer.
		rb.Produce(testData{1})
		rb.Produce(testData{2})

		// Launch a goroutine to produce a 3rd item, which should block.
		var produceDone sync.WaitGroup
		produceDone.Add(1)
		go func() {
			defer produceDone.Done()
			rb.Produce(testData{3})
		}()

		// Wait for the signal that the producer is blocked.
		<-signal

		// Consume 1 item, which should unblock the producer.
		_ = rb.Consume()

		// Signal that the producer is no longer blocked.
		<-signal

		// Wait for the produce goroutine to complete.
		produceDone.Wait()
	})

	t.Run("consumer blocked by produceer", func(*testing.T) {
		bufferSize := int64(1)
		signal := make(chan struct{})

		rb, _ := NewRingBufferBuilder[testData]().
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
			_ = rb.Consume()
		}()

		// Wait for the signal that the consumer is blocked.
		<-signal

		// Produce 1 item, which should unblock the consumer.
		rb.Produce(testData{id: 1})

		// Signal that the consumer is no longer blocked.
		<-signal

		// Wait for the consume goroutine to complete.
		consumeDone.Wait()
	})

	t.Run("consume observes produce", func(t *testing.T) {
		bufferSize := int64(2)
		rb, _ := NewRingBufferBuilder[testData]().WithSize(bufferSize).Build()

		// Produce 2 items.
		var produceDone sync.WaitGroup
		produceDone.Add(1)
		go func() {
			defer produceDone.Done()
			rb.Produce(testData{id: 1})
			rb.Produce(testData{id: 2})
		}()
		produceDone.Wait()

		// Consume 1 item.
		data1 := rb.Consume()
		if data1.id != 1 {
			t.Errorf("Consume() consumed wrong data, got %+v, want %+v", data1, testData{1})
		}

		// Consume another item.
		data2 := rb.Consume()
		if data2.id != 2 {
			t.Errorf("Consume() consumed wrong data, got %+v, want %+v", data2, testData{2})
		}
	})

	t.Run("produce and consume multiple items", func(t *testing.T) {
		bufferSize := int64(4)
		rb, _ := NewRingBufferBuilder[testData]().WithSize(bufferSize).Build()

		const numItems = 4
		var produceDone sync.WaitGroup
		produceDone.Add(1)
		go func() {
			defer produceDone.Done()
			for i := 1; i <= numItems; i++ {
				rb.Produce(testData{id: i})
			}
		}()

		var consumeDone sync.WaitGroup
		consumeDone.Add(1)
		go func() {
			defer consumeDone.Done()
			for i := 1; i <= numItems; i++ {
				data := rb.Consume()
				if data.id != i {
					t.Errorf("Consume() consumed wrong data, got %+v, want %+v", data, testData{i})
				}
			}
		}()
		produceDone.Wait()
		consumeDone.Wait()
	})
}
