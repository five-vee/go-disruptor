// Package disruptor provides an implementation of the LMAX Disruptor.
//
// If for some reason you have Go code that needs to process messages at
// sub-microsecond latency, where shaving every nanosecond counts, then
// consider the disruptor pattern.
package disruptor
