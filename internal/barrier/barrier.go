package barrier

// Barrier is a read-only sequence.
type Barrier interface {
	Load() int64
}

// MinimumBarrier loads the minimum from a set of read-only sequences.
type MinimumBarrier []Barrier

func (m MinimumBarrier) Load() int64 {
	// INVARIANT: barriers is non-empty.
	minimum := m[0].Load()
	for i := 1; i < len(m); i++ {
		seq := m[i].Load()
		diff := minimum - seq
		mask := diff >> 63 // arithmetic right shift: 0 if diff>=0, -1 if diff<0
		minimum = seq + (diff & mask)
	}
	return minimum
}

// ClosedBarrier is a closed-status viewer.
type ClosedBarrier interface {
	IsClosed() bool
}

// CompositeClosedBarrier is closed when all barriers in its set are closed.
type CompositeClosedBarrier []ClosedBarrier

func (c CompositeClosedBarrier) IsClosed() bool {
	for _, b := range c {
		if !b.IsClosed() {
			return false
		}
	}
	return true
}
