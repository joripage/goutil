package atomicstruct_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/joripage/goutil/pkg/atomicstruct"
)

// Order mirrors the example struct from overview.md.
type Order struct {
	Status   string
	LeaveQty int
}

func TestNewAndGet(t *testing.T) {
	want := Order{Status: "New", LeaveQty: 100}
	as := atomicstruct.New(want)

	if got := as.Get(); got != want {
		t.Fatalf("Get() = %+v, want %+v", got, want)
	}
}

func TestSet(t *testing.T) {
	as := atomicstruct.New(Order{Status: "New", LeaveQty: 100})

	want := Order{Status: "Cancelled", LeaveQty: 0}
	as.Set(want)

	if got := as.Get(); got != want {
		t.Fatalf("after Set, Get() = %+v, want %+v", got, want)
	}
}

func TestRead(t *testing.T) {
	want := Order{Status: "Filled", LeaveQty: 42}
	as := atomicstruct.New(want)

	var seen Order
	as.Read(func(o Order) { seen = o })

	if seen != want {
		t.Fatalf("Read passed %+v, want %+v", seen, want)
	}
}

func TestUpdate(t *testing.T) {
	as := atomicstruct.New(Order{Status: "New", LeaveQty: 100})

	as.Update(func(o *Order) {
		o.Status = "Filled"
		o.LeaveQty = 0
	})

	want := Order{Status: "Filled", LeaveQty: 0}
	if got := as.Get(); got != want {
		t.Fatalf("after Update, Get() = %+v, want %+v", got, want)
	}
}

func TestUpdateWithCondition(t *testing.T) {
	tests := []struct {
		name      string
		start     Order
		cond      func(Order) bool
		update    func(*Order)
		wantRan   bool
		wantValue Order
	}{
		{
			name:      "condition matches, update runs",
			start:     Order{Status: "Filled", LeaveQty: 100},
			cond:      func(o Order) bool { return o.Status == "Filled" },
			update:    func(o *Order) { o.LeaveQty = 0 },
			wantRan:   true,
			wantValue: Order{Status: "Filled", LeaveQty: 0},
		},
		{
			name:      "condition fails, value untouched",
			start:     Order{Status: "New", LeaveQty: 100},
			cond:      func(o Order) bool { return o.Status == "Filled" },
			update:    func(o *Order) { o.LeaveQty = 0 },
			wantRan:   false,
			wantValue: Order{Status: "New", LeaveQty: 100},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			as := atomicstruct.New(tc.start)

			ran := as.UpdateWithCondition(tc.cond, tc.update)

			if ran != tc.wantRan {
				t.Errorf("UpdateWithCondition returned %v, want %v", ran, tc.wantRan)
			}
			if got := as.Get(); got != tc.wantValue {
				t.Errorf("value = %+v, want %+v", got, tc.wantValue)
			}
		})
	}
}

// TestOverviewExample is the scenario described verbatim in overview.md.
func TestOverviewExample(t *testing.T) {
	atomicOrder := atomicstruct.New(Order{Status: "Filled", LeaveQty: 100})

	ran := atomicOrder.UpdateWithCondition(
		func(o Order) bool { return o.Status == "Filled" },
		func(o *Order) { o.LeaveQty = 0 },
	)

	if !ran {
		t.Fatal("expected condition to match")
	}
	if got := atomicOrder.Get().LeaveQty; got != 0 {
		t.Fatalf("LeaveQty = %d, want 0", got)
	}
}

func TestZeroValueUsable(t *testing.T) {
	var as atomicstruct.AtomicStruct[Order]

	if got := as.Get(); got != (Order{}) {
		t.Fatalf("zero-value Get() = %+v, want zero Order", got)
	}

	as.Update(func(o *Order) { o.Status = "New" })
	if got := as.Get().Status; got != "New" {
		t.Fatalf("after Update, Status = %q, want \"New\"", got)
	}
}

// stringerOrder implements fmt.Stringer.
type stringerOrder struct{ id int }

func (s stringerOrder) String() string { return fmt.Sprintf("order-%d", s.id) }

func TestString(t *testing.T) {
	t.Run("delegates to fmt.Stringer", func(t *testing.T) {
		as := atomicstruct.New(stringerOrder{id: 7})
		if got := as.String(); got != "order-7" {
			t.Fatalf("String() = %q, want %q", got, "order-7")
		}
	})

	t.Run("falls back to %v", func(t *testing.T) {
		as := atomicstruct.New(Order{Status: "New", LeaveQty: 5})
		want := fmt.Sprintf("%v", Order{Status: "New", LeaveQty: 5})
		if got := as.String(); got != want {
			t.Fatalf("String() = %q, want %q", got, want)
		}
	})

	t.Run("works with pointer T", func(t *testing.T) {
		// T == *stringerOrder; the value-receiver String is in its method set.
		as := atomicstruct.New(&stringerOrder{id: 9})
		if got := as.String(); got != "order-9" {
			t.Fatalf("String() = %q, want %q", got, "order-9")
		}
	})
}

// TestGetIsShallowCopy locks in the documented behaviour that Get returns a
// shallow copy: reference fields inside T still alias the wrapped data.
func TestGetIsShallowCopy(t *testing.T) {
	type tagged struct {
		Name string
		Tags []string
	}
	as := atomicstruct.New(tagged{Name: "order", Tags: []string{"a"}})

	snapshot := as.Get()
	// Mutating a slice element through Update is visible via the earlier
	// snapshot, because the copy shares the slice's backing array.
	as.Update(func(v *tagged) { v.Tags[0] = "b" })

	if snapshot.Tags[0] != "b" {
		t.Fatalf("expected shallow copy to alias the slice; Tags[0] = %q, want %q",
			snapshot.Tags[0], "b")
	}
}

// TestConcurrentUpdate stresses the lock: many goroutines each increment the
// wrapped counter many times. Run with -race to catch data races.
func TestConcurrentUpdate(t *testing.T) {
	const goroutines = 50
	const perGoroutine = 1000

	as := atomicstruct.New(Order{})

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				as.Update(func(o *Order) { o.LeaveQty++ })
			}
		}()
	}
	wg.Wait()

	want := goroutines * perGoroutine
	if got := as.Get().LeaveQty; got != want {
		t.Fatalf("LeaveQty = %d, want %d", got, want)
	}
}

// TestConcurrentReadWrite runs readers and writers together so -race can
// verify Read/Get share access correctly with Update.
func TestConcurrentReadWrite(t *testing.T) {
	const workers = 10
	const perWorker = 500

	as := atomicstruct.New(Order{Status: "New"})

	var wg sync.WaitGroup
	wg.Add(workers * 2) // one writer + one reader per worker
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				as.Update(func(o *Order) { o.LeaveQty++ })
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				as.Read(func(o Order) { _ = o.LeaveQty })
				_ = as.Get()
			}
		}()
	}
	wg.Wait()

	want := workers * perWorker
	if got := as.Get().LeaveQty; got != want {
		t.Fatalf("LeaveQty = %d, want %d", got, want)
	}
}
