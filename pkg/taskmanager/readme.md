# taskmanager

A lightweight Go package for running and managing multiple concurrent tasks
keyed by ID. Replace an in-flight task by starting a new one with the same
ID, stop tasks individually, or tear everything down with `GracefulShutdown`.

Features:

- Start tasks with a `context.Context`.
- Automatic cancellation of an existing task if a new one with the same ID
  is started (last-writer-wins).
- Automatic cleanup of tasks after completion.
- Panics inside a task are recovered and logged; the manager keeps running.
- `GracefulShutdown` cancels every running task and (optionally) waits for
  them to finish. After shutdown, `StartTask` is rejected with
  `ErrManagerClosed`.

Implementation uses a mutex-guarded map. Each entry is identified by
pointer, so a finishing task cannot evict the entry of a replacement task
started under the same ID.

## Install

```sh
go get github.com/joripage/goutil/pkg/taskmanager
```

Requires Go 1.18+.

## Usage

A runnable example with 6 patterns lives in [`cmd/taskmanager`](../../cmd/taskmanager).

```go
tm := taskmanager.NewTaskManager()

err := tm.StartTask(ctx, "task1", func(ctx context.Context) error {
    return nil
})

exist := tm.HasTask("task1")
count := tm.TaskCount()
ok := tm.StopTask("task1")

tm.GracefulShutdown(true, 3*time.Second)
```

## API

| Method | Purpose |
| --- | --- |
| `NewTaskManager() *TaskManager` | Construct an empty manager. |
| `StartTask(ctx, id, fn) error` | Start a task. Cancels and replaces any existing task with the same `id`. |
| `StopTask(id) bool` | Cancel a task by id. Returns whether it was running. |
| `HasTask(id) bool` | Whether a task with this id is currently tracked. |
| `TaskCount() int` | Number of tasks currently tracked. |
| `GracefulShutdown(wait bool, timeout time.Duration)` | Cancel every task; with `wait=true` block for up to `timeout`. Closes the manager. |

Exported errors: `ErrInvalidTaskID`, `ErrNilTaskFunc`, `ErrManagerClosed`.

## Gotchas

- A task function that ignores `ctx.Done()` will keep running after
  `StopTask` / `GracefulShutdown` until it returns on its own. Check
  `ctx.Done()` periodically inside any loop or between heavy operations.
- Return `ctx.Err()` from your task when canceled so the caller can tell
  why it ended.
- After `GracefulShutdown`, `StartTask` returns `ErrManagerClosed`. The
  manager is one-shot — make a new one instead of trying to restart.
- A panic inside a task is recovered and logged; it does not crash the
  process, but other tasks keep running unaffected.

## Cancellation pattern

Your task function must cooperate with `ctx` to be stoppable:

```go
// Before – no cancellation support
func processAllOrders() error {
    for _, order := range orders {
        process(order)
    }
    return nil
}

// After – stoppable
func processAllOrders(ctx context.Context) error {
    for _, order := range orders {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            process(order)
        }
    }
    return nil
}
```

See [`cmd/taskmanager/main.go`](../../cmd/taskmanager/main.go) for the full
set of cancellation patterns: by id, via shared parent context, by timeout,
replace-same-id, panic recovery, and graceful shutdown.

## Testing

```sh
go test -race ./...
```
