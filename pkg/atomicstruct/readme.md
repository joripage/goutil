# atomicstruct

A generic, mutex-guarded wrapper that gives any Go value thread-safe read and
conditional-update semantics. Built on `sync.RWMutex` — concurrent readers,
exclusive writers.

## Install

```sh
go get github.com/joripage/goutil/pkg/atomicstruct
```

Requires Go 1.18+.

## Usage

A runnable example with 4 patterns lives in [`cmd/atomicstruct`](../../cmd/atomicstruct).

Wrap an existing struct and mutate it safely from multiple goroutines:

```go
package main

import (
    "fmt"

    "github.com/joripage/goutil/pkg/atomicstruct"
)

type Order struct {
    Status   string
    LeaveQty int
}

func main() {
    atomicOrder := atomicstruct.New(Order{Status: "Filled", LeaveQty: 100})

    // Atomically: check the condition, and only if it holds, run the update.
    // The whole check-then-act sequence is performed under one write lock.
    updated := atomicOrder.UpdateWithCondition(
        func(o Order) bool { return o.Status == "Filled" },
        func(o *Order)     { o.LeaveQty = 0 },
    )

    fmt.Println(updated)              // true
    fmt.Println(atomicOrder.Get())    // {Filled 0}
}
```

## API

| Method | Lock | Purpose |
| --- | --- | --- |
| `New[T](v T) *AtomicStruct[T]` | — | Wrap a value |
| `Get() T` | read | Copy the value out |
| `Set(v T)` | write | Replace the value |
| `Read(fn func(T))` | read | Read-only access; RLock held during `fn` |
| `Update(fn func(*T))` | write | Mutate the value in place |
| `UpdateWithCondition(cond func(T) bool, update func(*T)) bool` | write | Atomic check-then-act; returns whether `cond` matched |
| `String() string` | read | Delegates to `fmt.Stringer` if `T` implements it, else `%v` |

## Parameterized callbacks

Need to pass extra arguments into a callback? Use a Go closure — no API
change needed. Same pattern works for `Update`, `Read`, and
`UpdateWithCondition`.

### Closure capture — one-shot

```go
leaveQty := 50
atomicOrder.UpdateWithCondition(
    func(o Order) bool { return o.Status == "Filled" },
    func(o *Order)     { o.LeaveQty -= leaveQty }, // captures leaveQty
)
```

Multiple arguments work the same way — just capture more variables from the
enclosing scope.

### Factory function — reusable

When the same parameterized update is used in many places, return the
callback from a factory:

```go
func reduceLeaveQty(by int) func(*Order) {
    return func(o *Order) { o.LeaveQty -= by }
}

isFilled := func(o Order) bool { return o.Status == "Filled" }

atomicOrder.UpdateWithCondition(isFilled, reduceLeaveQty(50))
atomicOrder.UpdateWithCondition(isFilled, reduceLeaveQty(100))
```

This is fully type-safe — the compiler checks every argument. No variadic
`...any`, no casts, no runtime surprises.

## Gotchas

- Callback arguments (`*T` or `T`) are only valid for the duration of the
  call. Do not retain them.
- `Get` and `Read` give a **shallow** copy — pointers, maps and slices inside
  `T` still alias the shared data and are not protected by this wrapper.
- Callbacks must not call back into the same `AtomicStruct`; `sync.RWMutex`
  is not reentrant and doing so deadlocks.
- Never copy an `AtomicStruct` by value — it embeds a `sync.RWMutex`. Pass
  `*AtomicStruct` around. `go vet` flags accidental copies.
- The zero value (`var as AtomicStruct[T]`) is usable, but `New` is
  preferred.

## Safe usage — do's and don'ts

**Golden rule:** the mutex protects the *box*, not what the box *points to*.
You are safe when (1) `T` holds only value-type fields, and (2) no pointer
or copy escapes a callback.

### Do — keep `T` value-only, go through the methods

```go
type Order struct {
    Status   string // value type
    LeaveQty int    // value type
}

as := atomicstruct.New(Order{Status: "New"})
as.Update(func(o *Order) { o.Status = "Filled" }) // write under the lock
snapshot := as.Get()                              // independent copy — safe
```

### Don't — let a pointer escape the callback

```go
var leaked *Order
as.Update(func(o *Order) { leaked = o }) // pointer now aliases as.value
leaked.Status = "X"                      // writes as.value WITHOUT the lock → data race
```

### Don't — mutate a `Get` copy and expect it to persist

```go
o := as.Get()
o.Status = "Filled" // changes a private copy only; as.value is untouched
// → use as.Update(func(o *Order) { o.Status = "Filled" }) instead
```

### Don't — call back into the same `AtomicStruct` (deadlock)

```go
as.Update(func(o *Order) {
    _ = as.Get() // RLock requested while Lock is held → deadlock
})
```

### Reference-type fields (slice/map/pointer) need extra care

```go
type Order struct {
    Status string
    Tags   []string // reference type — the slice is shared
}

snap := as.Get()                              // snap.Tags shares the backing array
as.Update(func(o *Order) { o.Tags[0] = "y" }) // races with any read of snap.Tags

// If T must hold a slice/map/pointer, deep-copy it inside the callback
// and never let the reference escape:
as.Update(func(o *Order) {
    o.Tags = append([]string(nil), o.Tags...)
})
```

**Recommendation:** design `T` with value-type fields only — that way going
through the methods is always safe, with nothing else to think about.

## Testing

```sh
go test -race ./...                                # unit + concurrency tests
go test -run=^$ -bench=. -benchmem ./...           # benchmarks
go test -run=^$ -bench=Parallel -cpu=1,2,4,8 ./... # contention scaling
```

Note: `RWMutex` only pays off when read critical sections are long enough to
amortize the lock overhead. For trivial accessors on a small struct, `Get`
does not scale with more cores — the lock bookkeeping dominates. Expect the
benefit on real workloads where the `Read`/`Update` callbacks do actual work.
