package engine

import (
	"errors"
	"sync"

	"github.com/bug00n/StreamForge/engine/contracts"
	"github.com/bug00n/StreamForge/engine/job"
	"github.com/bug00n/StreamForge/engine/task"
	"github.com/bug00n/StreamForge/engine/task_manager"
)

const defaultPartSize = 500

var ErrEngineClosed = errors.New("engine is closed")
var ErrEmptyCallback_Mapper = errors.New("mapper is nil")
var ErrEmptyCallback_Reducer = errors.New("reducer is nil")

type Engine[T any, K contracts.Key, V any, R any] struct {
	taskManager          taskmanager.TaskManager[T, K, V, R]
	jobs                 chan *job.Job[T, K, V, R]
	submitMu             sync.Mutex
	closeOnce            sync.Once
	closed               bool
	schedulerWG          sync.WaitGroup
	mapWorkersWG         sync.WaitGroup
	reduceWorkersWG      sync.WaitGroup
	shufflePartitionsCnt int
	partSize             int
}

func CreateEngine[T any, K contracts.Key, V any, R any](
	mappersCnt int,
	reducersCnt int,
	shufflePartitionsCnt int,
) *Engine[T, K, V, R] {
	if mappersCnt <= 0 {
		panic("mappersCnt must be positive")
	}
	if reducersCnt <= 0 {
		panic("reducersCnt must be positive")
	}
	if shufflePartitionsCnt <= 0 {
		panic("shufflePartitionsCnt must be positive")
	}

	e := &Engine[T, K, V, R]{
		taskManager:          taskmanager.MakeManager[T, K, V, R](mappersCnt, reducersCnt),
		jobs:                 make(chan *job.Job[T, K, V, R]),
		shufflePartitionsCnt: shufflePartitionsCnt,
		partSize:             defaultPartSize,
	}

	e.schedulerWG.Add(1)
	e.mapWorkersWG.Add(mappersCnt)
	e.reduceWorkersWG.Add(reducersCnt)

	go e.scheduler()
	for i := 0; i < mappersCnt; i++ {
		go e.workerMap()
	}
	for i := 0; i < reducersCnt; i++ {
		go e.workerReduce()
	}

	return e
}

func (e *Engine[T, K, V, R]) Submit(
	partitions contracts.Data[T],
	mapper func(T) ([]contracts.Result[K, V], error),
	reducer func(K, []V) (R, error),
) (job.JobViewer[T, K, V, R], error) {
	e.submitMu.Lock()
	defer e.submitMu.Unlock()

	if e.closed {
		return job.JobViewer[T, K, V, R]{}, ErrEngineClosed
	}

	if mapper == nil {
		return job.JobViewer[T, K, V, R]{}, ErrEmptyCallback_Mapper
	}

	if reducer == nil {
		return job.JobViewer[T, K, V, R]{}, ErrEmptyCallback_Reducer
	}

	newJob, viewer := job.MakeJob(
		partitions,
		mapper,
		reducer,
		e.startReduce,
		e.shufflePartitionsCnt,
		e.partSize,
	)
	e.jobs <- newJob
	return viewer, nil
}

func (e *Engine[T, K, V, R]) Close() {
	e.closeOnce.Do(func() {
		e.submitMu.Lock()
		e.closed = true
		close(e.jobs)
		e.submitMu.Unlock()

		e.schedulerWG.Wait()
		e.mapWorkersWG.Wait()
		e.taskManager.CloseReduceTasks()
		e.reduceWorkersWG.Wait()
	})
}

func (e *Engine[T, K, V, R]) startReduce(j *job.Job[T, K, V, R]) {
	for i := range j.Buffers {
		e.taskManager.PutReduceTask(
			task.MakeReduceTask(
				&j.Buffers[i],
				j.Reducer,
				j.PutReduceResoult,
				j.NotifyReduce,
				j.CanContinue,
				j.Fail,
			),
		)
	}
}

func (e *Engine[T, K, V, R]) scheduler() {
	defer e.schedulerWG.Done()
	for currentJob := range e.jobs {
		currentJob.Mu.Lock()
		if currentJob.Status == job.JobPending {
			currentJob.Status = job.JobRunning
		}
		currentJob.Mu.Unlock()
		for _, mapTask := range currentJob.Tasks {
			e.taskManager.PutMapTask(mapTask)
		}
	}
	e.taskManager.CloseMapTasks()
}

func (e *Engine[T, K, V, R]) workerMap() {
	defer e.mapWorkersWG.Done()
	for {
		currentTask, open := e.taskManager.GetMapTaskOrClosed()
		if !open {
			return
		}
		currentTask.Run()
	}
}

func (e *Engine[T, K, V, R]) workerReduce() {
	defer e.reduceWorkersWG.Done()
	for {
		currentTask, open := e.taskManager.GetReduceTaskOrClosed()
		if !open {
			return
		}
		currentTask.Run()
	}
}
