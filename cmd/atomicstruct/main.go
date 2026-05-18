package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/joripage/goutil/pkg/atomicstruct"
)

type Order struct {
	ID       int
	Status   string // "New" | "Filled" | "Cancelled"
	LeaveQty int
}

func main() {
	patternBasic()
	patternConcurrentCounter()
	patternAtomicStateTransition()
	patternConsistentSnapshot()
	patternStateMachine()
}

// --- Pattern 1: basic Get / Set / Update --------------------------------
//
// Wrap a value, mutate it via Update, read a snapshot via Get.
func patternBasic() {
	fmt.Println("Pattern 1: basic Get / Set / Update")

	o := atomicstruct.New(Order{ID: 1, Status: "New", LeaveQty: 100})

	o.Update(func(v *Order) {
		v.Status = "Filled"
		v.LeaveQty = 0
	})

	fmt.Printf("  after Update:  %+v\n", o.Get())

	o.Set(Order{ID: 1, Status: "Cancelled"})
	fmt.Printf("  after Set:     %+v\n\n", o.Get())
}

// --- Pattern 2: concurrent counter --------------------------------------
//
// Many goroutines mutate the same wrapped struct. Update serialises them
// under a write lock, so the final value is deterministic. Run with -race
// to verify there are no data races.
func patternConcurrentCounter() {
	fmt.Println("Pattern 2: concurrent counter (1000 goroutines x 1000 increments)")

	const goroutines = 1000
	const perGoroutine = 1000

	o := atomicstruct.New(Order{})

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				o.Update(func(v *Order) { v.LeaveQty++ })
			}
		}()
	}
	wg.Wait()

	fmt.Printf("  expected: %d, got: %d\n\n", goroutines*perGoroutine, o.Get().LeaveQty)
}

// --- Pattern 3: atomic check-then-act -----------------------------------
//
// The killer feature. Many goroutines race to "fill" an order, but only one
// must succeed (the others must observe Status != "New" and back off).
//
// Doing this with a plain Mutex would require the caller to write the
// lock/check/write/unlock sequence by hand at every call site;
// UpdateWithCondition makes the whole thing one expression.
func patternAtomicStateTransition() {
	fmt.Println("Pattern 3: atomic check-then-act (UpdateWithCondition)")

	const racers = 100

	o := atomicstruct.New(Order{ID: 42, Status: "New", LeaveQty: 100})

	var wins atomic.Int32
	var wg sync.WaitGroup
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			won := o.UpdateWithCondition(
				func(v Order) bool { return v.Status == "New" },
				func(v *Order) {
					v.Status = "Filled"
					v.LeaveQty = 0
				},
			)
			if won {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()

	fmt.Printf("  winners: %d (expected 1)\n", wins.Load())
	fmt.Printf("  final:   %+v\n\n", o.Get())
}

// --- Pattern 4: consistent snapshot via Read ----------------------------
//
// Read holds an RLock while fn runs, so a writer cannot slip in between
// reads of related fields. Compare with calling Get twice: between the two
// Gets a writer could change the value and the two reads would disagree.
func patternConsistentSnapshot() {
	fmt.Println("Pattern 4: consistent multi-field read")

	o := atomicstruct.New(Order{ID: 7, Status: "New", LeaveQty: 100})

	// Writer flipping the order between two valid states.
	stop := make(chan struct{})
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		toggle := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			if toggle {
				o.Update(func(v *Order) { v.Status = "Filled"; v.LeaveQty = 0 })
			} else {
				o.Update(func(v *Order) { v.Status = "New"; v.LeaveQty = 100 })
			}
			toggle = !toggle
		}
	}()

	// Reader: each Read sees Status and LeaveQty from the SAME snapshot,
	// so the invariant Status=="Filled" => LeaveQty==0 always holds.
	const reads = 10000
	violations := 0
	for i := 0; i < reads; i++ {
		o.Read(func(v Order) {
			if v.Status == "Filled" && v.LeaveQty != 0 {
				violations++
			}
		})
	}
	close(stop)
	writerWg.Wait()

	fmt.Printf("  reads: %d, invariant violations: %d (expected 0)\n\n", reads, violations)
}

// --- Pattern 5: state-machine transitions -------------------------------
//
// Encode the allowed transitions as `cond` and the next state as `update`.
// UpdateWithCondition checks and writes under one lock, so an illegal
// transition can never slip through, even under racing writers.
//
// Two goroutines race on the same order, starting at "New":
//
//	G1 -> Filled    allowed from "New" or "Cancel"
//	G2 -> Cancel    allowed only from "New"  (so Filled->Cancel is forbidden)
//
// Whichever goroutine wins, the order must end as "Filled":
//
//	G1 then G2:  New->Filled, G2's cond rejects "Filled"          => Filled
//	G2 then G1:  New->Cancel, G1's cond accepts "Cancel"->Filled  => Filled
//
// Without atomic check-then-act, both goroutines could read "New", G1
// writes "Filled", G2 then overwrites with "Cancel" — an illegal
// Filled->Cancel transition. The invariant below catches it.
func patternStateMachine() {
	fmt.Println("Pattern 5: state-machine transitions")

	const runs = 1000
	violations := 0
	cancelFirst, filledFirst := 0, 0

	for i := 0; i < runs; i++ {
		o := atomicstruct.New(Order{ID: i, Status: "New", LeaveQty: 100})

		var wg sync.WaitGroup
		wg.Add(2)

		// G1: drive toward Filled (allowed from New or Cancel).
		go func() {
			defer wg.Done()
			o.UpdateWithCondition(
				func(v Order) bool { return v.Status == "New" || v.Status == "Cancel" },
				func(v *Order) { v.Status = "Filled"; v.LeaveQty = 0 },
			)
		}()

		// G2: drive toward Cancel (allowed only from New).
		var g2Won bool
		go func() {
			defer wg.Done()
			g2Won = o.UpdateWithCondition(
				func(v Order) bool { return v.Status == "New" },
				func(v *Order) { v.Status = "Cancel" },
			)
		}()

		wg.Wait() // happens-before: safe to read g2Won and o below.

		if o.Get().Status != "Filled" {
			violations++
		}
		if g2Won {
			cancelFirst++
		} else {
			filledFirst++
		}
	}

	fmt.Printf("  runs: %d, invariant violations: %d (expected 0)\n", runs, violations)
	fmt.Printf("  interleavings exercised: cancel-first=%d, filled-first=%d\n",
		cancelFirst, filledFirst)
}
