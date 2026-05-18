package atomicstruct_test

import (
	"testing"

	"github.com/joripage/goutil/pkg/atomicstruct"
)

// Serial benchmarks — per-call cost with no contention.

func BenchmarkGet(b *testing.B) {
	as := atomicstruct.New(Order{Status: "Filled", LeaveQty: 100})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = as.Get()
	}
}

func BenchmarkSet(b *testing.B) {
	as := atomicstruct.New(Order{})
	o := Order{Status: "Filled", LeaveQty: 100}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		as.Set(o)
	}
}

func BenchmarkRead(b *testing.B) {
	as := atomicstruct.New(Order{Status: "Filled", LeaveQty: 100})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		as.Read(func(o Order) { _ = o.LeaveQty })
	}
}

func BenchmarkUpdate(b *testing.B) {
	as := atomicstruct.New(Order{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		as.Update(func(o *Order) { o.LeaveQty++ })
	}
}

func BenchmarkUpdateWithCondition(b *testing.B) {
	cond := func(o Order) bool { return o.Status == "Filled" }
	update := func(o *Order) { o.LeaveQty++ }

	b.Run("match", func(b *testing.B) {
		as := atomicstruct.New(Order{Status: "Filled"})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			as.UpdateWithCondition(cond, update)
		}
	})

	b.Run("no_match", func(b *testing.B) {
		as := atomicstruct.New(Order{Status: "New"})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			as.UpdateWithCondition(cond, update)
		}
	})
}

func BenchmarkString(b *testing.B) {
	b.Run("stringer", func(b *testing.B) {
		as := atomicstruct.New(stringerOrder{id: 7})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = as.String()
		}
	})

	b.Run("fallback", func(b *testing.B) {
		as := atomicstruct.New(Order{Status: "New", LeaveQty: 5})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = as.String()
		}
	})
}

// Parallel benchmarks — these show the point of RWMutex: reads scale, writes
// contend. Run with -cpu=1,2,4,8 to see how each path behaves under load.

func BenchmarkGetParallel(b *testing.B) {
	as := atomicstruct.New(Order{Status: "Filled", LeaveQty: 100})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = as.Get()
		}
	})
}

func BenchmarkUpdateParallel(b *testing.B) {
	as := atomicstruct.New(Order{})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			as.Update(func(o *Order) { o.LeaveQty++ })
		}
	})
}

// BenchmarkMixedParallel mimics a read-heavy workload (~90% reads, ~10%
// writes) — the case RWMutex is chosen for.
func BenchmarkMixedParallel(b *testing.B) {
	as := atomicstruct.New(Order{Status: "Filled"})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i++; i%10 == 0 {
				as.Update(func(o *Order) { o.LeaveQty++ })
			} else {
				_ = as.Get()
			}
		}
	})
}
