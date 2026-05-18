package atomicstruct

import (
	"fmt"
	"sync"
)

// AtomicStruct wraps a value of type T behind a sync.RWMutex so it can be
// shared and mutated safely across goroutines.
//
// The zero value (var as AtomicStruct[T]) is usable and wraps the zero value
// of T, but New is the preferred constructor.
type AtomicStruct[T any] struct {
	mu    sync.RWMutex
	value T
}

// New wraps v in an AtomicStruct ready for concurrent use.
func New[T any](v T) *AtomicStruct[T] {
	return &AtomicStruct[T]{value: v}
}

// Get returns a copy of the wrapped value. Safe for concurrent use.
//
// The copy is shallow: any pointers, maps or slices inside T still refer to
// the same underlying data as the wrapped value.
func (as *AtomicStruct[T]) Get() T {
	as.mu.RLock()
	defer as.mu.RUnlock()
	return as.value
}

// Set replaces the wrapped value with v.
func (as *AtomicStruct[T]) Set(v T) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.value = v
}

// Read runs fn with a copy of the wrapped value under a read lock, so multiple
// readers can run concurrently. fn must not retain the value beyond the call.
func (as *AtomicStruct[T]) Read(fn func(v T)) {
	as.mu.RLock()
	defer as.mu.RUnlock()
	fn(as.value)
}

// Update runs fn with a pointer to the wrapped value under a write lock,
// letting fn mutate it in place. fn must not retain the pointer beyond the
// call, and must not call back into this AtomicStruct (deadlock).
func (as *AtomicStruct[T]) Update(fn func(v *T)) {
	as.mu.Lock()
	defer as.mu.Unlock()
	fn(&as.value)
}

// UpdateWithCondition atomically checks cond against the current value and, if
// it returns true, runs update to mutate the value in place. The whole
// check-then-act sequence is performed under a single write lock, so no other
// goroutine can change the value in between.
//
// It reports whether cond matched — and therefore whether update ran.
//
// cond receives a copy of the value; update receives a pointer to it. Neither
// callback may retain what it is given, nor call back into this AtomicStruct.
func (as *AtomicStruct[T]) UpdateWithCondition(cond func(v T) bool, update func(v *T)) bool {
	as.mu.Lock()
	defer as.mu.Unlock()
	if !cond(as.value) {
		return false
	}
	update(&as.value)
	return true
}

// String returns a string representation of the wrapped value. If T implements
// fmt.Stringer its String method is used; otherwise the value is formatted
// with the %v verb. It is read-locked and safe for concurrent use.
//
// When T implements fmt.Stringer, that String method runs while the read lock
// is held, so — like the other callbacks — it must not call back into this
// AtomicStruct.
func (as *AtomicStruct[T]) String() string {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if s, ok := any(as.value).(fmt.Stringer); ok {
		return s.String()
	}
	return fmt.Sprintf("%v", as.value)
}
