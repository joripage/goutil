package taskmanager

import "errors"

var (
	ErrInvalidTaskID    = errors.New("invalid task id")
	ErrNilTaskFunc      = errors.New("task function cannot be nil")
	ErrTaskAlreadyExist = errors.New("task with this ID is already running")
	ErrManagerClosed    = errors.New("task manager is closed")
)
