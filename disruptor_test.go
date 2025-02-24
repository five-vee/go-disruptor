package disruptor

import (
	"fmt"
	"testing"
	"time"
)

type testData struct {
	id    int
	time  time.Time
	value string
}

func TestNewRingBuffer(t *testing.T) {
	testCases := []struct {
		name    string
		size    int64
		wantErr bool
	}{
		{
			name:    "valid size",
			size:    8,
			wantErr: false,
		},
		{
			name:    "invalid size - not power of two",
			size:    7,
			wantErr: true,
		},
		{
			name:    "invalid size - zero",
			size:    0,
			wantErr: true,
		},
		{
			name:    "invalid size - negative",
			size:    -8,
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rb, err := NewRingBuffer[testData](tc.size)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewRingBuffer(%d) error = %v, wantErr %v", tc.size, err, tc.wantErr)
			}
			if !tc.wantErr && rb == nil {
				t.Fatalf("NewRingBuffer(%d) returned nil RingBuffer, want non-nil", tc.size)
			}
		})
	}
}

// End-to-end test for Publish and Consume.
func TestRingBuffer_PublishConsume(t *testing.T) {
	t.Run("consume from empty buffer blocks", func(t *testing.T) {
		size := int64(8)
		rb, err := NewRingBuffer[testData](size)
		if err != nil {
			t.Fatalf("NewRingBuffer(%d) error = %v", size, err)
		}

		// Consumer should block until data is published.
		// Use a channel to signal when consume returns.
		consumed := make(chan testData)
		go func() {
			data := rb.Consume()
			consumed <- data
		}()

		// Wait briefly to ensure consumer is blocked.
		time.Sleep(100 * time.Millisecond)

		// At this point consumer should be blocked.
		// Publish data to unblock consumer.
		publishData := testData{id: 1, time: time.Now(), value: "test data"}
		rb.Publish(publishData)

		// Wait for consume to return.
		select {
		case data := <-consumed:
			if data != publishData {
				t.Errorf("Consume() = %v, want %v", data, publishData)
			}
		case <-time.After(time.Second):
			t.Fatal("Consume() timed out")
		}
	})

	t.Run("publish and consume multiple items", func(t *testing.T) {
		size := int64(8)
		rb, err := NewRingBuffer[testData](size)
		if err != nil {
			t.Fatalf("NewRingBuffer(%d) error = %v", size, err)
		}

		numItems := 5
		publishedData := make([]testData, numItems)
		for i := 0; i < numItems; i++ {
			data := testData{id: i, time: time.Now(), value: fmt.Sprintf("test data %d", i)}
			publishedData[i] = data
			rb.Publish(data)
		}

		consumedData := make([]testData, numItems)
		for i := 0; i < numItems; i++ {
			consumedData[i] = rb.Consume()
		}

		for i := 0; i < numItems; i++ {
			if consumedData[i] != publishedData[i] {
				t.Errorf("Consumed data at index %d = %v, want %v", i, consumedData[i], publishedData[i])
			}
		}
	})

	t.Run("consume from non-empty buffer", func(t *testing.T) {
		size := int64(8)
		rb, err := NewRingBuffer[testData](size)
		if err != nil {
			t.Fatalf("NewRingBuffer(%d) error = %v", size, err)
		}

		publishData := testData{id: 1, time: time.Now(), value: "test data"}
		rb.Publish(publishData)

		data := rb.Consume()
		if data != publishData {
			t.Errorf("Consume() = %v, want %v", data, publishData)
		}
	})
}
