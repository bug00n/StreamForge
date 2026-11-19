package job

import (
	"sync"
	"sync/atomic"

	"github.com/bug00n/StreamForge/engine/contracts"
	"github.com/bug00n/StreamForge/engine/shuffle_buffer"
	"github.com/bug00n/StreamForge/engine/task"
)

type JobStatus int

const (
	JobPending JobStatus = iota
	JobRunning
	JobCompleted
	JobFailed
)

type Job[T any, K contracts.Key, V any, R any] struct {
	Tasks         []*task.TaskMapper[T, K, V]
	Buffers       []buffer.Buffer[K, V]
	Status        JobStatus
	Error         error
	Done          chan struct{}
	Reducer       func(K, []V) (R, error)
	StartReduce   func(*Job[T, K, V, R])
	CounterMap    atomic.Int64
	CounterReduce atomic.Int64
	Results      map[K]R
	Mu            sync.Mutex
}

func MakeJob[T any, K contracts.Key, V any, R any](
	partitions contracts.Data[T],
	mapper func(T) ([]contracts.Result[K, V], error),
	reducer func(K, []V) (R, error),
	startReduce func(*Job[T, K, V, R]),
	shufflePartitionsCnt int,
	partSize int,
) (*Job[T, K, V, R], JobViewer[T, K, V, R]) {
	if shufflePartitionsCnt <= 0 {
		panic("shufflePartitionsCnt must be positive")
	}
	if partSize <= 0 {
		panic("partSize must be positive")
	}

	job := Job[T, K, V, R]{}
	data := partitions.Split()
	job.Reducer = reducer
	job.StartReduce = startReduce
	job.Buffers = make([]buffer.Buffer[K, V], shufflePartitionsCnt)
	for i := range job.Buffers {
		job.Buffers[i].Shuffle = make(map[K][]V)
	}
	job.Results = make(map[K]R)
	job.Done = make(chan struct{})

	if len(data) == 0 {
		job.Status = JobCompleted
		close(job.Done)
		return &job, JobViewer[T, K, V, R]{job: &job}
	}

	job.Tasks = make([]*task.TaskMapper[T, K, V], (len(data)+partSize-1)/partSize)
	for i := 0; i < (len(data)+partSize-1)/partSize; i++ {
		job.Tasks[i] = task.MakeMapTask(
			data[i*partSize:min(len(data), (i+1)*partSize)],
			mapper,
			job.putResult,
			job.notifyMap,
			job.CanContinue,
			job.Fail,
		)
	}

	return &job, JobViewer[T, K, V, R]{job: &job}
}

func (j *Job[T, K, V, R]) putResult(
	results []contracts.Result[K, V],
) {
	for i := range results {
		k, v := results[i].Key, results[i].Value

		idx := int(k.Hash() % uint32(len(j.Buffers)))

		j.Buffers[idx].Mu.Lock()
		j.Buffers[idx].Shuffle[k] = append(
			j.Buffers[idx].Shuffle[k],
			v,
		)
		j.Buffers[idx].Mu.Unlock()
	}
}

func (j *Job[T, K, V, R]) notifyMap() {
	if !j.CanContinue() {
		return
	}
	if j.CounterMap.Add(1) == int64(len(j.Tasks)) && j.CanContinue() {
		j.StartReduce(j)
	}
}

func (j *Job[T, K, V, R]) CanContinue() bool {
	j.Mu.Lock()
	defer j.Mu.Unlock()
	return j.Status != JobFailed && j.Status != JobCompleted
}

func (j *Job[T, K, V, R]) Fail(err error) {
	if err == nil {
		return
	}

	j.Mu.Lock()
	defer j.Mu.Unlock()
	if j.Status == JobFailed || j.Status == JobCompleted {
		return
	}
	j.Error = err
	j.Status = JobFailed
	close(j.Done)
}

func (j *Job[T, K, V, R]) PutReduceResult(
	key K, results R,
) {
	j.Mu.Lock()
	j.Results[key] = results
	j.Mu.Unlock()
}

func (j *Job[T, K, V, R]) NotifyReduce() {
	if !j.CanContinue() {
		return
	}
	if j.CounterReduce.Add(1) == int64(len(j.Buffers)) {
		j.Mu.Lock()
		if j.Status != JobFailed && j.Status != JobCompleted {
			j.Status = JobCompleted
			close(j.Done)
		}
		j.Mu.Unlock()
	}
}
