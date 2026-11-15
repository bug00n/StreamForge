package job

import (
	"github.com/bug00n/StreamForge/engine/contracts"
)

type JobViewer[T any, K contracts.Key, V contracts.Value[V], R any] struct {
	job	*Job[T, K, V, R]
}

func (j *JobViewer[T, K, V, R]) Status() JobStatus {
	return j.job.Status
}

func (j *JobViewer[T, K, V, R]) Load() map[K]R {
	if j.job.Status == JobCompleted {
		return j.job.Resoults
	} 
	return nil
}
