package job

import (
	"testing"

	"github.com/bug00n/StreamForge/engine/fixtures_wc"
)

func TestJob(t *testing.T) {
	var startedJob *Job[string, fixtures.Key, int, int]
	startReduceCalls := 0

	job, viewer := MakeJob(
		fixtures.Data("a a\nb"),
		fixtures.Mapper,
		fixtures.Reducer,
		func(started *Job[string, fixtures.Key, int, int]) {
			startedJob = started
			startReduceCalls++
		},
		1,
	)

	if viewer.Status() != JobPending {
		t.Fatalf("initial status = %v, want %v", viewer.Status(), JobPending)
	}
	if viewer.Load() != nil {
		t.Fatal("Load should return nil before the job completes")
	}
	if len(job.Tasks) != 2 {
		t.Fatalf("got %d map tasks, want 2", len(job.Tasks))
	}
	if len(job.Buffers) != 5 {
		t.Fatalf("got %d shuffle buffers, want 5", len(job.Buffers))
	}
	for i, want := range []string{"a a", "b"} {
		if len(job.Tasks[i].Partition) != 1 || job.Tasks[i].Partition[0] != want {
			t.Fatalf("task %d partition = %v, want [%q]", i, job.Tasks[i].Partition, want)
		}
	}
	for i := range job.Buffers {
		if job.Buffers[i].Shuffle == nil {
			t.Fatalf("shuffle buffer %d has a nil map", i)
		}
	}

	for _, mapTask := range job.Tasks {
		for _, line := range mapTask.Partition {
			mapTask.PutRes(mapTask.Mapper(line))
		}
		mapTask.Notify()
	}
	if startReduceCalls != 1 || startedJob != job {
		t.Fatalf("reduce started %d times for job %p, want once for %p", startReduceCalls, startedJob, job)
	}
	if got := job.CounterMap.Load(); got != int64(len(job.Tasks)) {
		t.Fatalf("map counter = %d, want %d", got, len(job.Tasks))
	}

	shuffledCounts := make(map[fixtures.Key]int)
	for i := range job.Buffers {
		for key, values := range job.Buffers[i].Shuffle {
			shuffledCounts[key] += len(values)
			job.PutReduceResoult(key, job.Reducer(key, values))
		}
		job.NotifyReduce()
	}
	if shuffledCounts["a"] != 2 || shuffledCounts["b"] != 1 {
		t.Fatalf("shuffle counts = %v, want map[a:2 b:1]", shuffledCounts)
	}

	select {
	case <-*viewer.Done():
	default:
		t.Fatal("Done channel should be closed after all reduce tasks finish")
	}
	if viewer.Status() != JobCompleted {
		t.Fatalf("final status = %v, want %v", viewer.Status(), JobCompleted)
	}
	if got := viewer.Load(); got["a"] != 2 || got["b"] != 1 || len(got) != 2 {
		t.Fatalf("Load = %v, want map[a:2 b:1]", got)
	}
	if got := job.CounterReduce.Load(); got != int64(len(job.Buffers)) {
		t.Fatalf("reduce counter = %d, want %d", got, len(job.Buffers))
	}
}
