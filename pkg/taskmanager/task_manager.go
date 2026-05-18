package taskmanager

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

type taskEntry struct {
	cancel context.CancelFunc
}

type TaskManager struct {
	mu     sync.Mutex
	tasks  map[string]*taskEntry
	wg     sync.WaitGroup
	closed bool
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[string]*taskEntry),
	}
}

func (s *TaskManager) HasTask(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.tasks[id]
	return ok
}

func (s *TaskManager) TaskCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.tasks)
}

func (s *TaskManager) StartTask(ctx context.Context, id string, fn func(ctx context.Context) error) error {
	if id == "" {
		return ErrInvalidTaskID
	}
	if fn == nil {
		return ErrNilTaskFunc
	}
	if ctx.Err() != nil {
		log.Printf("Context already canceled, task %s not started", id)
		return ctx.Err()
	}

	ctxTask, cancel := context.WithCancel(ctx)
	entry := &taskEntry{cancel: cancel}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return ErrManagerClosed
	}
	if old, ok := s.tasks[id]; ok {
		old.cancel()
	}
	s.tasks[id] = entry
	s.wg.Add(1)
	s.mu.Unlock()

	go s.run(ctxTask, id, entry, fn)
	return nil
}

func (s *TaskManager) run(ctx context.Context, id string, entry *taskEntry, fn func(ctx context.Context) error) {
	defer s.wg.Done()
	defer s.cleanup(id, entry)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Task %s panicked: %v", id, r)
		}
	}()

	err := fn(ctx)
	switch {
	case errors.Is(err, context.Canceled):
		log.Printf("Task %s was canceled", id)
	case err != nil:
		log.Printf("Task %s failed: %v", id, err)
	default:
		log.Printf("Task %s completed successfully", id)
	}
}

// cleanup removes the entry only if it is still the one this goroutine owns.
// This prevents a finishing task from deleting the entry of a replacement
// task that was started under the same ID.
func (s *TaskManager) cleanup(id string, entry *taskEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tasks[id] == entry {
		delete(s.tasks, id)
	}
}

func (s *TaskManager) StopTask(id string) bool {
	s.mu.Lock()
	entry, ok := s.tasks[id]
	if ok {
		delete(s.tasks, id)
	}
	s.mu.Unlock()

	if !ok {
		return false
	}
	entry.cancel()
	return true
}

// GracefulShutdown cancels every running task. With wait=true it blocks until
// either all tasks finish or the timeout elapses. After this call, StartTask
// will be rejected with ErrManagerClosed.
func (s *TaskManager) GracefulShutdown(wait bool, timeout time.Duration) {
	s.mu.Lock()
	s.closed = true
	for _, entry := range s.tasks {
		entry.cancel()
	}
	s.mu.Unlock()

	if !wait {
		log.Println("Graceful shutdown triggered without waiting")
		return
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All tasks completed gracefully")
	case <-time.After(timeout):
		log.Println("Graceful shutdown timed out")
	}
}
