package disruptor

import (
	"runtime"
	"sync"

	"github.com/five-vee/go-disruptor/internal/barrier"
	"github.com/five-vee/go-disruptor/internal/closer"
	"github.com/five-vee/go-disruptor/internal/pad"
)

// Disruptor supports a single writer and multiple readers.
type Disruptor[T any] struct {
	capacity    int64
	mask        int64
	buffer      []T
	readers     []readLooper
	readBarrier barrier.Barrier
	closed      bool // cached version of closer

	_ [64]byte // padding

	slowestReader pad.Int64 // cached version of readBarrier
	writeCursor   pad.AtomicInt64
	currentWriter pad.Int64 // cached version of writeCursor
	closer        closer.Closer
}

// Write adds an item to the disruptor.
// f writes in-place into the ring buffer.
func (d *Disruptor[T]) Write(f func(item *T)) {
	if d.closed {
		panic("Write() called after Close() was called.")
	}
	nextWriter := d.currentWriter.Val + 1
	d.reserve(nextWriter)
	f(&d.buffer[nextWriter&d.mask])
	d.commit(nextWriter)
}

func (d *Disruptor[T]) reserve(nextWriter int64) {
	const spinMask = (1 << 14) - 1
	for spin := 0; nextWriter >= d.slowestReader.Val+d.capacity; d.slowestReader.Val = d.readBarrier.Load() {
		if spin&spinMask == 0 {
			runtime.Gosched()
		}
		spin++
	}
}

func (d *Disruptor[T]) commit(nextWriter int64) {
	d.writeCursor.Store(nextWriter)
	d.currentWriter.Val = nextWriter
}

// WriteBatch adds n items to the disruptor.
// f is essentially a function that accepts a two sub-slices of the
// internal ring buffer:
//
// 1. First sub-slice is to the end of the ring buffer,
// 2. Secondsub-slice is from the beginning of the ring buffer.
//
// It is possible the 2nd sub-slice is empty if n doesn't wrap around the
// ring buffer, i.e. length == 0.
//
// Use WriteBatch over Write only if the complexity is needed and if the
// overhead of sub-slicing is much smaller than the time saved by batching,
// e.g. when working with SIMD code to write large numbers of items into
// the disruptor.
func (d *Disruptor[T]) WriteBatch(n int64, f func(ptrs [2]*T, lens [2]int)) {
	if d.closed {
		panic("WriteBatch() called after Close() was called.")
	}
	if n > d.capacity {
		panic("WriteBatch() attempted to write more items than capacity allows")
	}
	nextWriter := d.currentWriter.Val + n
	d.reserve(nextWriter)

	i, j := (d.currentWriter.Val+1)&d.mask, nextWriter&d.mask
	len1, len2 := unwrap(d.capacity, i, j)
	f([2]*T{&d.buffer[i], &d.buffer[0]}, [2]int{len1, len2})

	d.commit(nextWriter)
}

// LoopRead continuously reads messages
// and passes them to a provided reader(s).
// Blocks until the ring buffer is closed and empty.
func (d *Disruptor[T]) LoopRead() {
	var wg sync.WaitGroup
	for _, r := range d.readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.LoopRead()
		}()
	}
	wg.Wait()
}

// Close stops the disruptor.
func (d *Disruptor[T]) Close() {
	d.closer.Close()
	d.closed = true
}

// unwrap returns the range of data from `i` to `j`,
// where it is possible that `j` wraps around the buffer.
//
// Both `i` and `j` are inclusive indexes into the buffer.
//
// The range is returned as two slices, where the 1st slice
// is up to the end of the buffer, and the 2nd slice is
// from the beginning of the buffer.
//
// If j does not wrap around, then the 2nd slice is empty.
func unwrap(capacity, i, j int64) (int, int) {
	// Compute wrap: 1 if j < i, 0 if i <= j.
	wrap := ((j - i) >> 63) & 1
	// Create a mask that is 0 if wrap==0, and -1 (all ones) if wrap==1.
	mask := -wrap

	// When there is no wrap (mask==0), we want firstLen = j - i + 1.
	// When wrapping (mask==-1), we want firstLen = capacity - i.
	// We can compute this branchlessly:
	firstLen := ((j - i + 1) & (^mask)) | ((capacity - i) & mask)

	// Similarly, the second length is 0 when there's no wrap, or (j + 1) when wrapping.
	secondLen := (j + 1) & mask

	return int(firstLen), int(secondLen)
}

type readLooper interface {
	LoopRead()
}
