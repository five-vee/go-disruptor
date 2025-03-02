package disruptor_test

import (
	"errors"
	"testing"

	"github.com/five-vee/disruptor"
	"github.com/google/go-cmp/cmp"
)

func TestBuilder(t *testing.T) {
	type test struct {
		name         string
		capacity     int64
		readerGroups [][]func(*int)
		wantErr      error
	}
	tests := []test{
		{
			name:         "zero capacity",
			capacity:     0,
			readerGroups: [][]func(*int){{func(*int) {}}},
			wantErr:      disruptor.ErrCapacity,
		},
		{
			name:         "negative capacity",
			capacity:     -2,
			readerGroups: [][]func(*int){{func(*int) {}}},
			wantErr:      disruptor.ErrCapacity,
		},
		{
			name:         "non power of two capacity",
			capacity:     3,
			readerGroups: [][]func(*int){{func(*int) {}}},
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
			readerGroups: [][]func(*int){
				{func(*int) {}},
				{},
			},
			wantErr: disruptor.ErrEmptyReaderGroup,
		},
		{
			name:     "valid",
			capacity: 4,
			readerGroups: [][]func(*int){
				{func(*int) {}, func(*int) {}},
				{func(*int) {}},
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
	read := func(item *int) {
		gots[*item]++
	}
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
	read1 := func(item *int) {
		gots1[*item]++
	}
	gots2 := map[int]int{}
	read2 := func(item *int) {
		gots2[*item]++
	}
	gots3 := map[int]int{}
	read3 := func(item *int) {
		gots3[*item]++
	}
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
