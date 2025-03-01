package closer

import "sync/atomic"

const (
	openBuffer   = 0
	closedBuffer = 1
)

// Closer represents the open/closed state of the ring buffer.
// Its zero-value represents the open state.
type Closer struct {
	x atomic.Int64
	_ [56]byte
}

// IsClosed returns true if the ring buffer is closed.
func (c *Closer) IsClosed() bool {
	return c.x.Load() == closedBuffer
}

// Close sets the state to closed.
func (c *Closer) Close() {
	c.x.Store(closedBuffer)
}
