package atomicstruct_test

import (
	"sync"
	"testing"

	"github.com/joripage/goutil/pkg/atomicstruct"
)

// TestUpdateWithConditionStateMachine proves UpdateWithCondition makes a
// check-then-act state transition atomic.
//
// Two goroutines race on a single Order that starts as "New": G1 drives it
// toward "Filled", G2 toward "Cancel". The transition rules are:
//
//	New    -> Filled  allowed      New    -> Cancel  allowed
//	Cancel -> Filled  allowed      Filled -> Cancel  FORBIDDEN
//
// Because each condition check and its write happen under one lock, only two
// interleavings are possible and BOTH end in "Filled":
//
//	G1 then G2:  New -> Filled, then G2's condition rejects it   => Filled
//	G2 then G1:  New -> Cancel, then Cancel -> Filled            => Filled
//
// If the check and the write were NOT atomic, both goroutines could observe
// "New", G1 could write "Filled", and G2 could then write "Cancel" — an
// illegal Filled -> Cancel transition leaving the order in "Cancel". So a
// final state of "Filled" on every run is the proof of atomicity. Run with
// -race for the strongest signal.
func TestUpdateWithConditionStateMachine(t *testing.T) {
	const runs = 10000

	// "Filled" is reachable from New or Cancel.
	canBecomeFilled := func(o Order) bool {
		return o.Status == "New" || o.Status == "Cancel"
	}
	// "Cancel" is reachable only from New — never from Filled.
	canBecomeCancel := func(o Order) bool {
		return o.Status == "New"
	}

	var sawCancelFirst, sawFilledFirst bool

	for i := 0; i < runs; i++ {
		as := atomicstruct.New(Order{Status: "New"})

		var wg sync.WaitGroup
		wg.Add(2)

		// G1 — drive the order toward "Filled".
		go func() {
			defer wg.Done()
			as.UpdateWithCondition(canBecomeFilled, func(o *Order) {
				o.Status = "Filled"
			})
		}()

		// G2 — drive the order toward "Cancel"; record whether it ran.
		var g2Ran bool
		go func() {
			defer wg.Done()
			g2Ran = as.UpdateWithCondition(canBecomeCancel, func(o *Order) {
				o.Status = "Cancel"
			})
		}()

		wg.Wait() // happens-before: safe to read as / g2Ran below.

		// The invariant: whatever the scheduler did, the order ends "Filled".
		if got := as.Get().Status; got != "Filled" {
			t.Fatalf("run %d: final Status = %q, want %q — an illegal "+
				"transition slipped through; check-then-act was not atomic",
				i, got, "Filled")
		}

		// g2Ran tells us which interleaving the scheduler picked:
		//   g2Ran == true  -> G2 got "New" first (New->Cancel), G1 then Cancel->Filled
		//   g2Ran == false -> G1 set "Filled" first, G2's condition rejected it
		if g2Ran {
			sawCancelFirst = true
		} else {
			sawFilledFirst = true
		}
	}

	// Sanity check: confirm the goroutines genuinely raced both ways, so the
	// invariant above was actually exercised under contention — not trivially
	// satisfied by the scheduler always picking one order.
	if !sawCancelFirst || !sawFilledFirst {
		t.Logf("note: scheduler exercised only one interleaving "+
			"(cancelFirst=%v filledFirst=%v); invariant still held on all %d runs",
			sawCancelFirst, sawFilledFirst, runs)
	}
}
