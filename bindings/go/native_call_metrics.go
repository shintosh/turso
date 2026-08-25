package turso

import (
	"sync"
	"sync/atomic"
	"time"
)

// NativeCallMetrics is a bounded process-wide snapshot of C ABI activity.
// It contains no SQL text, database paths, or handle identities.
type NativeCallMetrics struct {
	Calls          uint64
	ExecutionTime  time.Duration
	ObjectLocks    uint64
	ObjectWaitTime time.Duration
}

var nativeCallCount atomic.Uint64
var nativeCallExecutionNanos atomic.Uint64
var nativeObjectLockCount atomic.Uint64
var nativeObjectWaitNanos atomic.Uint64

type nativeObjectMutex struct {
	mu sync.Mutex
}

func (m *nativeObjectMutex) Lock() {
	started := time.Now()
	m.mu.Lock()
	nativeObjectLockCount.Add(1)
	nativeObjectWaitNanos.Add(uint64(time.Since(started)))
}

func (m *nativeObjectMutex) Unlock() {
	m.mu.Unlock()
}

func recordNativeCallExecution(elapsed time.Duration) {
	nativeCallCount.Add(1)
	nativeCallExecutionNanos.Add(uint64(elapsed))
}

// ReadNativeCallMetrics returns one consistent-enough monotonic telemetry
// snapshot. Callers compute interval deltas from two snapshots.
func ReadNativeCallMetrics() NativeCallMetrics {
	return NativeCallMetrics{
		Calls:          nativeCallCount.Load(),
		ExecutionTime:  time.Duration(nativeCallExecutionNanos.Load()),
		ObjectLocks:    nativeObjectLockCount.Load(),
		ObjectWaitTime: time.Duration(nativeObjectWaitNanos.Load()),
	}
}
