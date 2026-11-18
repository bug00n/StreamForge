package taskmanager

import (
	"sync"

	"github.com/bug00n/StreamForge/engine/contracts"
	"github.com/bug00n/StreamForge/engine/task"
)

type TaskManager[T any, K contracts.Key, V contracts.Value[V], R any] struct {
	freeMapTasks    chan task.Task_Mapper[T, K, V]
	freeReduceTasks chan task.Task_Reducer[K, V, R]
	mapCloseOnce    sync.Once
	reduceCloseOnce sync.Once
}

func MakeManager[T any, K contracts.Key, V contracts.Value[V], R any](
	mappersCnt int,
	reducersCnt int,
) TaskManager[T, K, V, R] {
	m := TaskManager[T, K, V, R]{
		freeMapTasks:    make(chan task.Task_Mapper[T, K, V], mappersCnt),
		freeReduceTasks: make(chan task.Task_Reducer[K, V, R], reducersCnt),
	}
	return m
}

func (m *TaskManager[T, K, V, R]) PutMapTask(task task.Task_Mapper[T, K, V]) {
	m.freeMapTasks <- task
}

func (m *TaskManager[T, K, V, R]) GetMapTask() task.Task_Mapper[T, K, V] {
	return <-m.freeMapTasks
}

func (m *TaskManager[T, K, V, R]) GetMapTaskOrClosed() (task.Task_Mapper[T, K, V], bool) {
	currentTask, open := <-m.freeMapTasks
	return currentTask, open
}

func (m *TaskManager[T, K, V, R]) CloseMapTasks() {
	m.mapCloseOnce.Do(func() { close(m.freeMapTasks) })
}

func (m *TaskManager[T, K, V, R]) PutReduceTask(task task.Task_Reducer[K, V, R]) {
	m.freeReduceTasks <- task
}

func (m *TaskManager[T, K, V, R]) GetReduceTask() task.Task_Reducer[K, V, R] {
	return <-m.freeReduceTasks
}

func (m *TaskManager[T, K, V, R]) GetReduceTaskOrClosed() (task.Task_Reducer[K, V, R], bool) {
	currentTask, open := <-m.freeReduceTasks
	return currentTask, open
}

func (m *TaskManager[T, K, V, R]) CloseReduceTasks() {
	m.reduceCloseOnce.Do(func() { close(m.freeReduceTasks) })
}
