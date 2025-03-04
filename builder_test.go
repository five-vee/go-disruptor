package disruptor_test

import (
	"errors"
	"testing"

	"github.com/five-vee/go-disruptor"
)

func TestBuilder(t *testing.T) {
	type test struct {
		name         string
		capacity     int64
		readerGroups [][]disruptor.ReaderFunc
		wantErr      error
	}
	tests := []test{
		{
			name:         "zero capacity",
			capacity:     0,
			readerGroups: [][]disruptor.ReaderFunc{{disruptor.SingleReaderFunc(func(*int) {})}},
			wantErr:      disruptor.ErrCapacity,
		},
		{
			name:         "negative capacity",
			capacity:     -2,
			readerGroups: [][]disruptor.ReaderFunc{{disruptor.SingleReaderFunc(func(*int) {})}},
			wantErr:      disruptor.ErrCapacity,
		},
		{
			name:         "non power of two capacity",
			capacity:     3,
			readerGroups: [][]disruptor.ReaderFunc{{disruptor.SingleReaderFunc(func(*int) {})}},
			wantErr:      disruptor.ErrCapacity,
		},
		{
			name:         "missing reader group",
			capacity:     4,
			readerGroups: nil,
			wantErr:      disruptor.ErrMissingReaderGroup,
		},
		{
			name:     "empty reader group",
			capacity: 4,
			readerGroups: [][]disruptor.ReaderFunc{
				{disruptor.SingleReaderFunc(func(*int) {})},
				{},
			},
			wantErr: disruptor.ErrEmptyReaderGroup,
		},
		{
			name:     "valid",
			capacity: 4,
			readerGroups: [][]disruptor.ReaderFunc{
				{
					disruptor.SingleReaderFunc(func(*int) {}),
					disruptor.BatchReaderFunc(func(ptrs [2]*int, lens [2]int) {}),
				},
				{
					disruptor.SingleReaderFunc(func(*int) {}),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := disruptor.NewBuilder[int](test.capacity)
			for _, group := range test.readerGroups {
				b = b.WithReaderGroup(group...)
			}
			d, err := b.Build()
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Build(%q) got err = %v, want = %v", test.name, err, test.wantErr)
			}
			if err != nil {
				return
			}
			if d == nil {
				t.Fatalf("Build(%q) got invalid nil disruptor", test.name)
			}
		})
	}
}
