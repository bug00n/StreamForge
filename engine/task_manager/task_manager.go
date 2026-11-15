package taskmanager

import (
	"github.com/bug00n/StreamForge/engine/task"
	"github.com/bug00n/StreamForge/engine/contracts"
)

type TaskManager [T any, K contracts.Key, V contracts.Value[V], R any] struct {
	freeMapTasks	chan task.Task_Mapper[T, K, V]
	freeReduceTasks chan task.Task_Reducer[K, V, R]
}

func MakeManager[T any, K contracts.Key, V contracts.Value[V], R any] (
	mappersCnt int,
	reducersCnt int,
) TaskManager[T, K, V, R] {
	m := TaskManager[T, K, V, R]{
		freeMapTasks: make(chan task.Task_Mapper[T, K, V], mappersCnt),
		freeReduceTasks: make(chan task.Task_Reducer[K, V, R], reducersCnt),
	}
	return m
}

func (m *TaskManager[T, K, V, R]) PutMapTask(task task.Task_Mapper[T, K, V]) {
	m.freeMapTasks <- task
}

func (m *TaskManager[T, K, V, R]) GetMapTask() task.Task_Mapper[T, K, V] {
	return <- m.freeMapTasks
}

func (m *TaskManager[T, K, V, R]) PutReduceTask(task task.Task_Reducer[K, V, R]) {
	m.freeReduceTasks <- task
}

func (m *TaskManager[T, K, V, R]) GetReduceTask() task.Task_Reducer[K, V, R] {
	return <- m.freeReduceTasks
}
