package task

import (
	"sync/atomic"

	"github.com/bug00n/StreamForge/engine/contracts"
	"github.com/bug00n/StreamForge/engine/shuffle_buffer"
)

type TaskStatus int32

const (
	TaskPending TaskStatus = iota
	TaskRunning
	TaskCompleted
	TaskFailed
)

type TaskMapper[T any, K contracts.Key, V any] struct {
	Partition   []T
	Mapper      func(T) ([]contracts.Result[K, V], error)
	PutRes      func([]contracts.Result[K, V])
	Notify      func()
	CanContinue func() bool
	Fail        func(error)
	WorkerID    int
	status      atomic.Int32
}

func MakeMapTask[T any, K contracts.Key, V any](
	partition []T,
	mapper func(T) ([]contracts.Result[K, V], error),
	putRes func([]contracts.Result[K, V]),
	notify func(),
	canContinue func() bool,
	fail func(error),
) *TaskMapper[T, K, V] {
	t := &TaskMapper[T, K, V]{
		Partition:   partition,
		Mapper:      mapper,
		PutRes:      putRes,
		Notify:      notify,
		CanContinue: canContinue,
		Fail:        fail,
		WorkerID:    -1,
	}
	t.status.Store(int32(TaskPending))
	return t
}

func (t *TaskMapper[T, K, V]) Status() TaskStatus {
	return TaskStatus(t.status.Load())
}

func (t *TaskMapper[T, K, V]) Run() {
	if !t.CanContinue() {
		t.status.Store(int32(TaskFailed))
		return
	}
	t.status.Store(int32(TaskRunning))

	for _, value := range t.Partition {
		if !t.CanContinue() {
			t.status.Store(int32(TaskFailed))
			return
		}

		results, err := t.Mapper(value)
		if err != nil {
			t.status.Store(int32(TaskFailed))
			t.Fail(err)
			return
		}
		if !t.CanContinue() {
			t.status.Store(int32(TaskFailed))
			return
		}
		t.PutRes(results)
	}

	t.status.Store(int32(TaskCompleted))
	t.Notify()
}

type TaskReducer[K contracts.Key, V any, R any] struct {
	Buffer      *buffer.Buffer[K, V]
	Reducer     func(K, []V) (R, error)
	PutRes      func(K, R)
	Notify      func()
	CanContinue func() bool
	Fail        func(error)
	WorkerID    int
	status      atomic.Int32
}

func MakeReduceTask[K contracts.Key, V any, R any](
	buff *buffer.Buffer[K, V],
	reducer func(K, []V) (R, error),
	putRes func(K, R),
	notify func(),
	canContinue func() bool,
	fail func(error),
) *TaskReducer[K, V, R] {
	t := &TaskReducer[K, V, R]{
		Buffer:      buff,
		Reducer:     reducer,
		PutRes:      putRes,
		Notify:      notify,
		CanContinue: canContinue,
		Fail:        fail,
		WorkerID:    -1,
	}
	t.status.Store(int32(TaskPending))
	return t
}

func (t *TaskReducer[K, V, R]) Status() TaskStatus {
	return TaskStatus(t.status.Load())
}

func (t *TaskReducer[K, V, R]) Run() {
	if !t.CanContinue() {
		t.status.Store(int32(TaskFailed))
		return
	}
	t.status.Store(int32(TaskRunning))

	for key, values := range t.Buffer.Shuffle {
		if !t.CanContinue() {
			t.status.Store(int32(TaskFailed))
			return
		}

		result, err := t.Reducer(key, values)
		if err != nil {
			t.status.Store(int32(TaskFailed))
			t.Fail(err)
			return
		}
		if !t.CanContinue() {
			t.status.Store(int32(TaskFailed))
			return
		}
		t.PutRes(key, result)
	}

	t.status.Store(int32(TaskCompleted))
	t.Notify()
}
