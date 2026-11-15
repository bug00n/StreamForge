package engine

import (
	"github.com/bug00n/StreamForge/engine/contracts"
	"github.com/bug00n/StreamForge/engine/job"
	"github.com/bug00n/StreamForge/engine/task"
	"github.com/bug00n/StreamForge/engine/task_manager"
)

type Engine [T any, K contracts.Key, V contracts.Value[V], R any] struct {
	taskManager	taskmanager.TaskManager[T, K, V, R]
	jobs		chan *job.Job[T, K, V, R]
	partSize	int
	_         	struct{}
}

func CreateEngine [T any, K contracts.Key, V contracts.Value[V], R any] (
	mappersCnt int,
	reducersCnt int,
) *Engine[T, K, V, R] {
	manager := taskmanager.MakeManager[T, K, V, R](mappersCnt, reducersCnt)

	e := &Engine[T, K, V, R]{
		taskManager: manager,
		jobs:	make(chan *job.Job[T, K, V, R]),
		partSize: 500,
	}

	go e.scheduler()

	for i := 0; i < mappersCnt; i++ {
		go e.worker_map()
	}

	for i := 0; i < reducersCnt; i++ {
		go e.worker_reduce()
	}

	return e
}

func (e *Engine[T, K, V, R]) Submit(
	partitions contracts.Data[T],
	mapper func(T) []contracts.Resoult[K, V],
	reducer func(K, []V) R,
) job.JobViewer[T, K, V, R] {
	job, job_viewer := job.MakeJob(partitions, mapper, reducer, e.startReduce, e.partSize)
	e.jobs <- job
	return job_viewer
}

func (e *Engine[T, K, V, R]) startReduce(j *job.Job[T, K, V, R]) {
	for i := range j.Buffers {
		e.taskManager.PutReduceTask(
			task.MakeReduceTask(
				&j.Buffers[i],
				j.Reducer,
				j.PutReduceResoult,
				j.NotifyReduce,
			),
		)
	}
}

func (e *Engine[T, K, V, R]) scheduler() {
	for {
		currentJob := <-e.jobs
		currentJob.Status = job.JobRunning
		for _, task := range currentJob.Tasks {
			e.taskManager.PutMapTask(task)
		}
	}
}

func (e *Engine[T, K, V, R]) worker_map() {
	for {
		currentTask := e.taskManager.GetMapTask()
		for _, value := range currentTask.Partition {
			resoults := currentTask.Mapper(value)
			currentTask.PutRes(resoults)
		}
		currentTask.Notify()
	}
}

func (e *Engine[T, K, V, R]) worker_reduce() {
	for {
		currentTask := e.taskManager.GetReduceTask()
		for key, values := range currentTask.Buffer.Shuffle {
			resoult := currentTask.Reducer(key, values)
			currentTask.PutRes(key, resoult)
		}
		currentTask.Notify()
	}
}
