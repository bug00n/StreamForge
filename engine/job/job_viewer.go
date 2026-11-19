package job

import (
	"github.com/bug00n/StreamForge/engine/contracts"
)

type JobViewer[T any, K contracts.Key, V any, R any] struct {
	job *Job[T, K, V, R]
}

func (j *JobViewer[T, K, V, R]) Status() JobStatus {
	j.job.Mu.Lock()
	defer j.job.Mu.Unlock()
	return j.job.Status
}

func (j *JobViewer[T, K, V, R]) Err() error {
	j.job.Mu.Lock()
	defer j.job.Mu.Unlock()
	return j.job.Error
}

func (j *JobViewer[T, K, V, R]) Load() map[K]R {
	j.job.Mu.Lock()
	defer j.job.Mu.Unlock()

	if j.job.Status == JobCompleted {
		results := make(map[K]R, len(j.job.Results))
		for key, value := range j.job.Results {
			results[key] = value
		}
		return results
	}
	return nil
}

func (j *JobViewer[T, K, V, R]) Done() chan struct{} {
	return j.job.Done
}
