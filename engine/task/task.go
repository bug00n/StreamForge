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

type Task_Mapper[T any, K contracts.Key, V contracts.Value[V]] struct {
	Partition []T
	Mapper	  func(T) []contracts.Resoult[K, V]
	PutRes	  func(resoults []contracts.Resoult[K, V])
	Notify	  func()
	WorkerID  int
	Status 	  TaskStatus
}

func MakeMapTask[T any, K contracts.Key, V contracts.Value[V]] (
	partition []T,
	mapper func(T) []contracts.Resoult[K, V],
	putRes func(resoults []contracts.Resoult[K, V]),
	notify func(),
) Task_Mapper[T, K, V] {
	return Task_Mapper[T, K, V]{
		Partition: partition,
		Mapper: mapper,
		PutRes: putRes,
		Notify: notify,
		WorkerID: -1,
		Status: TaskPending,
	}
}

// ---

type Task_Reducer[K contracts.Key, V contracts.Value[V], R any] struct {
	Buffer    *buffer.Buffer[K, V]
	Reducer	  func(K, []V) R
	PutRes	  func(K, R)
	Notify	  func()
	WorkerID  int
	Status 	  TaskStatus
}

func MakeReduceTask[K contracts.Key, V contracts.Value[V], R any] (
	buffer *buffer.Buffer[K, V],
	reducer func(K, []V) R,
	putRes func(K, R),
	notify func(),
) Task_Reducer[K, V, R] {
	return Task_Reducer[K, V, R]{
		Buffer: buffer,
		Reducer: reducer,
		PutRes: putRes,
		Notify: notify,
		WorkerID: -1,
		Status: TaskPending,
	}
}
