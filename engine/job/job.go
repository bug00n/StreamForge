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

type Job[T any, K contracts.Key, V contracts.Value[V], R any] struct {
	Tasks         []task.Task_Mapper[T, K, V]
	Buffers       []buffer.Buffer[K, V]
	Status        JobStatus
	Done          chan struct{}
	Reducer       func(K, []V) R
	StartReduce   func(*Job[T, K, V, R])
	CounterMap    atomic.Int64
	CounterReduce atomic.Int64
	Resoults      map[K]R
	Mu            sync.Mutex
}

func MakeJob[T any, K contracts.Key, V contracts.Value[V], R any](
	partitions contracts.Data[T],
	mapper func(T) []contracts.Resoult[K, V],
	reducer func(K, []V) R,
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
	job.Resoults = make(map[K]R)
	job.Done = make(chan struct{})

	if len(data) == 0 {
		job.Status = JobCompleted
		close(job.Done)
		return &job, JobViewer[T, K, V, R]{job: &job}
	}

	job.Tasks = make([]task.Task_Mapper[T, K, V], (len(data)+partSize-1)/partSize)
	for i := 0; i < (len(data)+partSize-1)/partSize; i++ {
		job.Tasks[i] = task.MakeMapTask(
			data[i*partSize:min(len(data), (i+1)*partSize)],
			mapper,
			job.putResoult,
			job.notifyMap,
		)
	}

	return &job, JobViewer[T, K, V, R]{job: &job}
}

func (j *Job[T, K, V, R]) putResoult(
	resoults []contracts.Resoult[K, V],
) {
	for i := range resoults {
		k, v := resoults[i].Key, resoults[i].Value

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
	if j.CounterMap.Add(1) == int64(len(j.Tasks)) {
		j.StartReduce(j)
	}
}

func (j *Job[T, K, V, R]) PutReduceResoult(
	key K, resoults R,
) {
	j.Mu.Lock()
	j.Resoults[key] = resoults
	j.Mu.Unlock()
}

func (j *Job[T, K, V, R]) NotifyReduce() {
	if j.CounterReduce.Add(1) == int64(len(j.Buffers)) {
		j.Mu.Lock()
		j.Status = JobCompleted
		close(j.Done)
		j.Mu.Unlock()
	}
}
