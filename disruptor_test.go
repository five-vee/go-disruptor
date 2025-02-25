package disruptor

import (
	"testing"
)

type testData struct {
	id int
}

func mustPublish(t *testing.T, rb *RingBuffer[testData], id int) {
	t.Helper()
	if ok := rb.TryPublish(testData{id}); !ok {
		t.Fatalf("TryPublish({id: %d}) failed", id)
	}
}

func mustConsume(t *testing.T, rb *RingBuffer[testData]) testData {
	t.Helper()
	data, ok := rb.TryConsume()
	if !ok {
		t.Fatalf("TryConsume() failed")
	}
	return data
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

// End-to-end tests for TryPublish and TryConsume.
func TestRingBuffer_EndToEnd(t *testing.T) {
	t.Run("producer blocked by consumer", func(t *testing.T) {
		bufferSize := int64(2)
		rb, _ := NewRingBuffer[testData](bufferSize)

		// Produce 2 items to fill the buffer.
		mustPublish(t, rb, 1)
		mustPublish(t, rb, 2)

		// Consume 1 item.
		_ = mustConsume(t, rb)

		// Attempt to produce a 3rd item (should succeed).
		if ok := rb.TryPublish(testData{id: 3}); !ok {
			t.Fatal("Producer should not be blocked after consumer consumes")
		}

		// Attempt to produce a 4th item (should fail).
		if ok := rb.TryPublish(testData{id: 4}); ok {
			t.Error("Producer should be blocked when buffer is full")
		}
	})

	t.Run("consumer blocked by producer", func(t *testing.T) {
		bufferSize := int64(2)
		rb, _ := NewRingBuffer[testData](bufferSize)

		// Produce 1 item.
		mustPublish(t, rb, 1)

		// Consume 1 item (should succeed).
		_ = mustConsume(t, rb)

		// Attempt to consume again (should fail).
		if data, ok := rb.TryConsume(); ok {
			t.Errorf("Consumer should be blocked when buffer is empty, got id %d", data.id)
		}
	})

	t.Run("consumer observes producer", func(t *testing.T) {
		bufferSize := int64(2)
		rb, _ := NewRingBuffer[testData](bufferSize)

		// Produce 2 items.
		mustPublish(t, rb, 1)
		mustPublish(t, rb, 2)

		// Consume 1 item.
		data1, ok := rb.TryConsume()
		if !ok {
			t.Fatal("Consumer should be able to consume 1")
		}
		if data1.id != 1 {
			t.Errorf("Consumer consumed wrong data, got %+v, want %+v", data1, testData{1})
		}

		// Consume another item.
		data2, ok := rb.TryConsume()
		if !ok {
			t.Fatal("Consumer should be able to consume 2")
		}
		if data2.id != 2 {
			t.Errorf("Consumer consumed wrong data, got %+v, want %+v", data2, testData{2})
		}
	})
}
