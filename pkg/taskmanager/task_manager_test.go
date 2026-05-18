package taskmanager

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartTask_NewTaskAdded(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	started := make(chan struct{})
	err := tm.StartTask(ctx, "task", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error starting task: %v", err)
	}

	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Task did not start in time")
	}

	if !tm.HasTask("task") {
		t.Error("Expected task to be in TaskManager")
	}
}

func TestStartTask_ReplacesOldTask(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	oldCanceled := make(chan struct{})
	_ = tm.StartTask(ctx, "task", func(ctx context.Context) error {
		<-ctx.Done()
		close(oldCanceled)
		return nil
	})

	// start new task with same ID
	newStarted := make(chan struct{})
	_ = tm.StartTask(ctx, "task", func(ctx context.Context) error {
		close(newStarted)
		<-ctx.Done()
		return nil
	})

	select {
	case <-oldCanceled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Old task was not canceled")
	}

	select {
	case <-newStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("New task did not start in time")
	}
}

func TestStartTask_RemovesTaskAfterFinish(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	done := make(chan struct{})
	_ = tm.StartTask(ctx, "task", func(ctx context.Context) error {
		close(done)
		return nil
	})

	<-done
	time.Sleep(10 * time.Millisecond)

	if tm.HasTask("task") {
		t.Error("Expected task to be removed after completion")
	}
}

func TestStartTask_LogCancelAndError(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	canceledDone := make(chan struct{})
	_ = tm.StartTask(ctx, "cancel", func(ctx context.Context) error {
		<-ctx.Done()
		close(canceledDone)
		return context.Canceled
	})
	_ = tm.StartTask(ctx, "cancel", func(ctx context.Context) error {
		return nil
	})
	<-canceledDone

	errorDone := make(chan struct{})
	_ = tm.StartTask(ctx, "error", func(ctx context.Context) error {
		close(errorDone)
		return errors.New("boom")
	})
	<-errorDone
}

func TestStartTask_ContextAlreadyCanceled(t *testing.T) {
	tm := NewTaskManager()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := make(chan struct{})
	err := tm.StartTask(ctx, "should_not_start", func(ctx context.Context) error {
		close(started)
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected context.Canceled error, got %v", err)
	}

	if tm.HasTask("should_not_start") {
		t.Error("Expected no task to be added when context is already canceled")
	}

	select {
	case <-started:
		t.Error("Task function should not have been called")
	default:
		// OK
	}
}

func TestStartTask_InvalidID(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	err := tm.StartTask(ctx, "", func(ctx context.Context) error { return nil })
	if err == nil {
		t.Fatal("Expected error for empty task ID, got nil")
	}
}

func TestStartTask_NilFunction(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	err := tm.StartTask(ctx, "task", nil)
	if err == nil {
		t.Fatal("Expected error for nil task function, got nil")
	}
}

func TestStartTask_TwoTasksRunningIndependently(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	started1 := make(chan struct{})
	started2 := make(chan struct{})
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	_ = tm.StartTask(ctx, "task1", func(ctx context.Context) error {
		close(started1)
		<-ctx.Done()
		close(done1)
		return nil
	})

	_ = tm.StartTask(ctx, "task2", func(ctx context.Context) error {
		close(started2)
		<-ctx.Done()
		close(done2)
		return nil
	})

	select {
	case <-started1:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Task1 did not start in time")
	}
	select {
	case <-started2:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Task2 did not start in time")
	}

	if !tm.HasTask("task1") || !tm.HasTask("task2") {
		t.Error("Expected both task1 and task2 to be running")
	}

	tm.StopTask("task1")

	select {
	case <-done1:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Task1 was not canceled in time")
	}

	select {
	case <-done2:
		t.Error("Task2 should still be running after task1 is canceled")
	default:
		// OK
	}
}

func TestStopTask_ExistingTask(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	stoppedCh := make(chan struct{})
	_ = tm.StartTask(ctx, "task1", func(ctx context.Context) error {
		<-ctx.Done()
		close(stoppedCh)
		return nil
	})

	if !tm.HasTask("task1") {
		t.Fatal("Expected task1 to be running before stopping")
	}

	ok := tm.StopTask("task1")
	if !ok {
		t.Error("Expected StopTask to return true for existing task")
	}

	select {
	case <-stoppedCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Task1 was not canceled after StopTask")
	}

	if tm.HasTask("task1") {
		t.Error("Expected task1 to be removed after StopTask")
	}
}

func TestStopTask_NonExistingTask(t *testing.T) {
	tm := NewTaskManager()

	ok := tm.StopTask("does_not_exist")
	if ok {
		t.Error("Expected StopTask to return false for non-existent task")
	}
}

func TestStopTask_StopTwice(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	_ = tm.StartTask(ctx, "task1", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})

	first := tm.StopTask("task1")
	second := tm.StopTask("task1")

	if !first {
		t.Error("First StopTask should return true")
	}
	if second {
		t.Error("Second StopTask should return false (task already stopped)")
	}
}

func TestStopTask_DoesNotAffectOtherTasks(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	stopped1 := make(chan struct{})
	stopped2 := make(chan struct{})

	_ = tm.StartTask(ctx, "task1", func(ctx context.Context) error {
		<-ctx.Done()
		close(stopped1)
		return nil
	})

	_ = tm.StartTask(ctx, "task2", func(ctx context.Context) error {
		<-ctx.Done()
		close(stopped2)
		return nil
	})

	if !tm.HasTask("task1") || !tm.HasTask("task2") {
		t.Fatal("Expected both task1 and task2 to be running")
	}

	ok := tm.StopTask("task1")
	if !ok {
		t.Error("Expected StopTask to return true for task1")
	}

	select {
	case <-stopped1:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Task1 was not canceled after StopTask")
	}

	select {
	case <-stopped2:
		t.Error("Task2 should not be canceled when stopping task1")
	default:
		// OK
	}

	if tm.HasTask("task1") {
		t.Error("Task1 should be removed from TaskManager after stopping")
	}
	if !tm.HasTask("task2") {
		t.Error("Task2 should still be running in TaskManager")
	}
}

func TestGracefulShutdown_WaitTrue(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	taskDone := make(chan struct{})
	_ = tm.StartTask(ctx, "long_task", func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // simulate cleanup
		close(taskDone)

		return nil
	})

	time.Sleep(20 * time.Millisecond) // ensure task started

	start := time.Now()
	tm.GracefulShutdown(true, 500*time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond {
		t.Error("GracefulShutdown returned before task finished cleanup")
	}

	select {
	case <-taskDone:
	default:
		t.Error("Task was not canceled properly")
	}
}

func TestGracefulShutdown_WaitFalse(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	taskDone := make(chan struct{})
	_ = tm.StartTask(ctx, "long_task", func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // simulate cleanup
		close(taskDone)
		return nil
	})

	time.Sleep(20 * time.Millisecond) // ensure task started

	start := time.Now()
	tm.GracefulShutdown(false, 500*time.Millisecond)
	elapsed := time.Since(start)

	if elapsed >= 50*time.Millisecond {
		t.Error("GracefulShutdown with wait=false should return immediately")
	}

	// Task should still finish eventually
	select {
	case <-taskDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("Task did not finish in expected time")
	}
}

// Verifies the replace-race fix: when a task with the same ID is replaced,
// the old goroutine's cleanup must not delete the replacement task's entry.
func TestStartTask_ReplaceDoesNotEvictNewEntry(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	oldDone := make(chan struct{})
	_ = tm.StartTask(ctx, "race", func(ctx context.Context) error {
		<-ctx.Done()
		// Block briefly so the replacement task is fully registered
		// before this goroutine fires its cleanup defer.
		time.Sleep(20 * time.Millisecond)
		close(oldDone)
		return nil
	})

	newStarted := make(chan struct{})
	_ = tm.StartTask(ctx, "race", func(ctx context.Context) error {
		close(newStarted)
		<-ctx.Done()
		return nil
	})

	<-newStarted
	<-oldDone
	// Give the old goroutine's defer a chance to run.
	time.Sleep(20 * time.Millisecond)

	if !tm.HasTask("race") {
		t.Fatal("Replacement task entry was evicted by the old task's cleanup")
	}
	if !tm.StopTask("race") {
		t.Fatal("StopTask could not find the replacement task")
	}
}

// Ensures a panic inside the user function does not crash the process and
// the task entry is cleaned up afterwards.
func TestStartTask_RecoversFromPanic(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	err := tm.StartTask(ctx, "panicker", func(ctx context.Context) error {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("Unexpected error starting task: %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !tm.HasTask("panicker") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Task entry was not cleaned up after panic")
}

// After GracefulShutdown, StartTask must be rejected.
func TestStartTask_RejectedAfterShutdown(t *testing.T) {
	tm := NewTaskManager()
	tm.GracefulShutdown(false, 0)

	err := tm.StartTask(context.Background(), "late", func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Expected ErrManagerClosed, got %v", err)
	}
	if tm.HasTask("late") {
		t.Error("Task should not be registered after shutdown")
	}
}

// Stress test: concurrent Start/Stop on the same ID must not panic or leak
// entries. Run with -race to catch any remaining data races.
func TestStartStopTask_ConcurrentSameID(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	var wg sync.WaitGroup
	var started atomic.Int64
	const n = 200

	for i := 0; i < n; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			err := tm.StartTask(ctx, "hot", func(ctx context.Context) error {
				started.Add(1)
				<-ctx.Done()
				return nil
			})
			_ = err
		}()
		go func() {
			defer wg.Done()
			tm.StopTask("hot")
		}()
	}
	wg.Wait()

	tm.GracefulShutdown(true, time.Second)
	if tm.TaskCount() != 0 {
		t.Errorf("Expected 0 tasks after shutdown, got %d", tm.TaskCount())
	}
}

func TestGracefulShutdown_Timeout(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	// timeout task
	_ = tm.StartTask(ctx, "stuck_task", func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(200 * time.Millisecond) // simulate very long cleanup
		return nil
	})

	time.Sleep(20 * time.Millisecond) // ensure task started

	start := time.Now()
	tm.GracefulShutdown(true, 50*time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond || elapsed > 100*time.Millisecond {
		t.Errorf("Expected shutdown to timeout around 50ms, got %v", elapsed)
	}

	time.Sleep(200 * time.Millisecond)
}
