package disruptor_test

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/five-vee/go-disruptor"
	"github.com/google/go-cmp/cmp"
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

func TestDisruptor_SingleReader_SmokeTest(t *testing.T) {
	// Setup.
	const (
		capacity = 1 << 2
		n        = (1 << 3) + 3
	)
	wants := func() map[int]int {
		m := map[int]int{}
		for i := 0; i < n; i++ {
			m[i]++
		}
		return m
	}()
	gots := map[int]int{}
	read := disruptor.SingleReaderFunc(func(item *int) {
		gots[*item]++
	})
	d, _ := disruptor.NewBuilder[int](capacity).
		WithReaderGroup(read).
		Build()

	// Run test.
	go func() {
		for i := 0; i < n; i++ {
			d.Write(func(item *int) { *item = i })
		}
		d.Close()
	}()
	d.LoopRead()

	// Verify outputs.
	if diff := cmp.Diff(wants, gots); diff != "" {
		t.Errorf("Read() received different messages from Write() (-want +got):\n%s", diff)
	}
}

func TestDisruptor_MultiReader_SmokeTest(t *testing.T) {
	// Setup.
	const (
		capacity = 1 << 2
		n        = (1 << 3) + 3
	)
	wants := func() map[int]int {
		m := map[int]int{}
		for i := 0; i < n; i++ {
			m[i]++
		}
		return m
	}()
	gots1 := map[int]int{}
	read1 := disruptor.SingleReaderFunc(func(item *int) {
		gots1[*item]++
	})
	gots2 := map[int]int{}
	read2 := disruptor.SingleReaderFunc(func(item *int) {
		gots2[*item]++
	})
	gots3 := map[int]int{}
	read3 := disruptor.SingleReaderFunc(func(item *int) {
		gots3[*item]++
	})
	d, _ := disruptor.NewBuilder[int](capacity).
		WithReaderGroup(read1, read2).
		WithReaderGroup(read3).
		Build()

	// Run test.
	go func() {
		for i := 0; i < n; i++ {
			d.Write(func(item *int) { *item = i })
		}
		d.Close()
	}()
	d.LoopRead()

	// Verify outputs.
	if diff := cmp.Diff(wants, gots1); diff != "" {
		t.Errorf("ReadLoop() reader 1 received different messages from Write() (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wants, gots2); diff != "" {
		t.Errorf("ReadLoop() reader 2 received different messages from Write() (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wants, gots3); diff != "" {
		t.Errorf("ReadLoop() reader 3 received different messages from Write() (-want +got):\n%s", diff)
	}
}

func TestDisruptor_Batching_SmokeTest(t *testing.T) {
	// Setup.
	const (
		capacity  = 1 << 2
		n         = (1 << 3) + 1
		batchSize = 3
	)
	wants := func() map[int]int {
		m := map[int]int{}
		for i := 0; i < n; i += batchSize {
			m[i]++
			m[i+1]++
			m[i+2]++
		}
		return m
	}()
	gots := map[int]int{}
	read := disruptor.BatchReaderFunc(func(ptrs [2]*int, lens [2]int) {
		for _, item := range unsafe.Slice(ptrs[0], lens[0]) {
			gots[item]++
		}
		for _, item := range unsafe.Slice(ptrs[1], lens[1]) {
			gots[item]++
		}
	})
	d, _ := disruptor.NewBuilder[int](capacity).
		WithReaderGroup(read).
		Build()

	// Run test.
	go func() {
		for i := 0; i < n; i += batchSize {
			batch := [batchSize]int{i, i + 1, i + 2}
			d.WriteBatch(batchSize, func(ptrs [2]*int, lens [2]int) {
				i := copy(unsafe.Slice(ptrs[0], lens[0]), batch[:])
				copy(unsafe.Slice(ptrs[1], lens[1]), batch[i:])
			})
		}
		d.Close()
	}()
	d.LoopRead()

	// Verify outputs.
	if diff := cmp.Diff(wants, gots); diff != "" {
		t.Errorf("Read() received different messages from Write() (-want +got):\n%s", diff)
	}
}
