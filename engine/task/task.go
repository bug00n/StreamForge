package task

import (
	"github.com/bug00n/StreamForge/engine/contracts"
	"github.com/bug00n/StreamForge/engine/shuffle_buffer"
)

type TaskStatus int

const (
    TaskPending TaskStatus = iota
    TaskRunning
    TaskCompleted
    TaskFailed
)

type TaskMapper[T any, K contracts.Key, V any] struct {
	Partition []T
	Mapper	  func(T) []contracts.Result[K, V]
	PutRes	  func(resoults []contracts.Result[K, V])
	Notify	  func()
	WorkerID  int
	Status 	  TaskStatus
}

func MakeMapTask[T any, K contracts.Key, V any] (
	partition []T,
	mapper func(T) []contracts.Result[K, V],
	putRes func(resoults []contracts.Result[K, V]),
	notify func(),
) TaskMapper[T, K, V] {
	return TaskMapper[T, K, V]{
		Partition: partition,
		Mapper: mapper,
		PutRes: putRes,
		Notify: notify,
		WorkerID: -1,
		Status: TaskPending,
	}
}

// ---

type TaskReducer[K contracts.Key, V any, R any] struct {
	Buffer    *buffer.Buffer[K, V]
	Reducer	  func(K, []V) R
	PutRes	  func(K, R)
	Notify	  func()
	WorkerID  int
	Status 	  TaskStatus
}

func MakeReduceTask[K contracts.Key, V any, R any] (
	buffer *buffer.Buffer[K, V],
	reducer func(K, []V) R,
	putRes func(K, R),
	notify func(),
) TaskReducer[K, V, R] {
	return TaskReducer[K, V, R]{
		Buffer: buffer,
		Reducer: reducer,
		PutRes: putRes,
		Notify: notify,
		WorkerID: -1,
		Status: TaskPending,
	}
}
