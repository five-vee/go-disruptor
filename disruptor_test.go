package disruptor_test

import (
	"sync"
	"testing"

	"github.com/five-vee/disruptor"
	"github.com/google/go-cmp/cmp"
)

func TestNewDisruptor(t *testing.T) {
	type testCase[T any] struct {
		name             string
		capacity         int64
		wantErr          error
		isValidDisruptor bool
	}
	consume := func(int) {}

	testCases := []testCase[int]{
		{
			name:             "zero capacity",
			capacity:         0,
			wantErr:          disruptor.ErrCapacity,
			isValidDisruptor: false,
		},
		{
			name:             "non power of two capacity",
			capacity:         10,
			wantErr:          disruptor.ErrCapacity,
			isValidDisruptor: false,
		},
		{
			name:             "negative capacity",
			capacity:         -3,
			wantErr:          disruptor.ErrCapacity,
			isValidDisruptor: false,
		},
		{
			name:             "valid capacity",
			capacity:         16,
			wantErr:          nil,
			isValidDisruptor: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := disruptor.NewDisruptor(tc.capacity, consume)

			if err != tc.wantErr {
				t.Errorf("NewDisruptor(%q) error = %v, wantErr %v", tc.name, err, tc.wantErr)
				return
			}

			if (d != nil) != tc.isValidDisruptor {
				t.Errorf("NewDisruptor(%q) disruptor validity = %v, want %v", tc.name, d != nil, tc.isValidDisruptor)
			}
		})
	}
}

func TestDisruptor_SmokeTest(t *testing.T) {
	// Setup.
	const (
		capacity = 1 << 12
		n        = (1 << 15) + 3
	)
	wants := func() map[int]int {
		m := map[int]int{}
		for i := 0; i < n; i++ {
			m[i]++
		}
		return m
	}()
	gots := map[int]int{}
	consumer := func(item int) {
		gots[item]++
	}
	d, _ := disruptor.NewDisruptor(capacity, consumer)

	// Run test.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			d.Produce(i)
		}
		d.Close()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.LoopConsume()
	}()
	wg.Wait()

	// Verify outputs.
	if diff := cmp.Diff(wants, gots); diff != "" {
		t.Errorf("Consume() received different messages than Produce() (-want +got):\n%s", diff)
	}
}
