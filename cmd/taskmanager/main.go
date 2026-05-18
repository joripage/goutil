package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/joripage/goutil/pkg/taskmanager"
)

var orders = []string{"order1", "order2", "order3"}

func processAllOrders(ctx context.Context) error {
	for _, o := range orders {
		select {
		case <-ctx.Done():
			fmt.Println("    canceled early")
			return ctx.Err()
		default:
			fmt.Println("    processing", o)
			time.Sleep(200 * time.Millisecond)
		}
	}
	fmt.Println("    completed")
	return nil
}

func main() {
	patternStartStop()
	patternSharedParentContext()
	patternTimeout()
	patternReplaceSameID()
	patternPanicRecovery()
	patternGracefulShutdown()
}

// --- Pattern 1: start individual tasks, stop them by ID -----------------
func patternStartStop() {
	fmt.Println("Pattern 1: start / stop individual tasks")

	tm := taskmanager.NewTaskManager()
	_ = tm.StartTask(context.Background(), "task1", processAllOrders)
	_ = tm.StartTask(context.Background(), "task2", processAllOrders)

	time.Sleep(300 * time.Millisecond)
	tm.StopTask("task1")
	tm.StopTask("task2")
	tm.GracefulShutdown(true, time.Second)
	fmt.Println()
}

// --- Pattern 2: cancel many tasks at once via a shared parent context ---
//
// All tasks started with the same ctx are cancelled when that ctx is.
func patternSharedParentContext() {
	fmt.Println("Pattern 2: cancel many tasks via a shared parent context")

	tm := taskmanager.NewTaskManager()
	parentCtx, cancelAll := context.WithCancel(context.Background())
	_ = tm.StartTask(parentCtx, "task3", processAllOrders)
	_ = tm.StartTask(parentCtx, "task4", processAllOrders)

	time.Sleep(300 * time.Millisecond)
	cancelAll() // both tasks observe ctx.Done()
	tm.GracefulShutdown(true, time.Second)
	fmt.Println()
}

// --- Pattern 3: deadline via context.WithTimeout ------------------------
//
// The manager doesn't need a timeout knob — caller picks the lifetime by
// passing a context that already has one.
func patternTimeout() {
	fmt.Println("Pattern 3: auto-cancel via context.WithTimeout")

	tm := taskmanager.NewTaskManager()
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	_ = tm.StartTask(ctx, "deadline_task", processAllOrders)
	tm.GracefulShutdown(true, time.Second)
	fmt.Println()
}

// --- Pattern 4: starting a task with an ID that's already in use --------
//
// StartTask cancels the previous task with that ID and replaces it. Useful
// for "latest wins" pipelines (e.g. recomputing a result when input
// changes; the in-flight stale work is abandoned).
func patternReplaceSameID() {
	fmt.Println("Pattern 4: replace task with the same ID (latest wins)")

	tm := taskmanager.NewTaskManager()

	makeTask := func(label string) func(context.Context) error {
		return func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					fmt.Println("    ", label, "canceled")
					return ctx.Err()
				default:
					fmt.Println("    ", label, "working")
					time.Sleep(150 * time.Millisecond)
				}
			}
		}
	}

	_ = tm.StartTask(context.Background(), "compute", makeTask("old"))
	time.Sleep(200 * time.Millisecond)
	_ = tm.StartTask(context.Background(), "compute", makeTask("new"))
	time.Sleep(300 * time.Millisecond)
	tm.GracefulShutdown(true, time.Second)
	fmt.Println()
}

// --- Pattern 5: panic in a task does not crash the manager --------------
//
// A panic is recovered, the entry is cleaned up, and other tasks keep
// running — important for long-lived services.
func patternPanicRecovery() {
	fmt.Println("Pattern 5: panic recovery")

	tm := taskmanager.NewTaskManager()

	_ = tm.StartTask(context.Background(), "panicker", func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		panic("boom")
	})
	_ = tm.StartTask(context.Background(), "survivor", func(ctx context.Context) error {
		<-ctx.Done()
		fmt.Println("     survivor: still here, ctx done")
		return nil
	})

	time.Sleep(300 * time.Millisecond)
	fmt.Printf("     panicker still tracked? %t  survivor tracked? %t\n",
		tm.HasTask("panicker"), tm.HasTask("survivor"))

	tm.GracefulShutdown(true, time.Second)
	fmt.Println()
}

// --- Pattern 6: graceful shutdown + post-shutdown rejection -------------
//
// GracefulShutdown cancels every running task and (optionally) waits for
// them to finish. After it returns, StartTask is rejected with
// ErrManagerClosed — so callers can't sneak new work in during teardown.
func patternGracefulShutdown() {
	fmt.Println("Pattern 6: graceful shutdown + reject new starts")

	tm := taskmanager.NewTaskManager()
	_ = tm.StartTask(context.Background(), "task5", processAllOrders)
	_ = tm.StartTask(context.Background(), "task6", processAllOrders)

	time.Sleep(300 * time.Millisecond)
	fmt.Println("    shutting down...")
	tm.GracefulShutdown(true, time.Second)

	err := tm.StartTask(context.Background(), "late", processAllOrders)
	fmt.Printf("    StartTask after shutdown: err=%v  is ErrManagerClosed=%t\n",
		err, errors.Is(err, taskmanager.ErrManagerClosed))
}
