package job

import (
	"sync"
	"testing"

	"github.com/bug00n/StreamForge/engine/fixtures_wc"
)

func TestJob(t *testing.T) {
	var startedJob *Job[string, fixtures.Key, int, int]
	startReduceCalls := 0
	var startReduceMu sync.Mutex

	job, viewer := MakeJob(
		fixtures.Data("a a\nb"),
		fixtures.Mapper,
		fixtures.Reducer,
		func(started *Job[string, fixtures.Key, int, int]) {
			startReduceMu.Lock()
			defer startReduceMu.Unlock()
			startedJob = started
			startReduceCalls++
		},
		3,
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
	if len(job.Buffers) != 3 {
		t.Fatalf("got %d shuffle buffers, want 3", len(job.Buffers))
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
	}
	var mapNotifyWG sync.WaitGroup
	for i := range job.Tasks {
		mapNotifyWG.Add(1)
		go func(i int) {
			defer mapNotifyWG.Done()
			job.Tasks[i].Notify()
		}(i)
	}
	mapNotifyWG.Wait()
	startReduceMu.Lock()
	gotStartReduceCalls, gotStartedJob := startReduceCalls, startedJob
	startReduceMu.Unlock()
	if gotStartReduceCalls != 1 || gotStartedJob != job {
		t.Fatalf("reduce started %d times for job %p, want once for %p", gotStartReduceCalls, gotStartedJob, job)
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
	}
	if shuffledCounts["a"] != 2 || shuffledCounts["b"] != 1 {
		t.Fatalf("shuffle counts = %v, want map[a:2 b:1]", shuffledCounts)
	}
	var reduceNotifyWG sync.WaitGroup
	for range job.Buffers {
		reduceNotifyWG.Add(1)
		go func() {
			defer reduceNotifyWG.Done()
			job.NotifyReduce()
		}()
	}
	reduceNotifyWG.Wait()

	select {
	case <-viewer.Done():
	default:
		t.Fatal("Done channel should be closed after all reduce tasks finish")
	}
	if viewer.Status() != JobCompleted {
		t.Fatalf("final status = %v, want %v", viewer.Status(), JobCompleted)
	}
	if got := viewer.Load(); got["a"] != 2 || got["b"] != 1 || len(got) != 2 {
		t.Fatalf("Load = %v, want map[a:2 b:1]", got)
	}
	loadedResults := viewer.Load()
	loadedResults["a"] = 99
	if got := viewer.Load()["a"]; got != 2 {
		t.Fatalf("mutating Load result changed job result to %d, want 2", got)
	}
	if got := job.CounterReduce.Load(); got != int64(len(job.Buffers)) {
		t.Fatalf("reduce counter = %d, want %d", got, len(job.Buffers))
	}
}

func TestMakeJobRejectsNonPositivePartitionAndPartSize(t *testing.T) {
	tests := []struct {
		name                 string
		shufflePartitionsCnt int
		partSize             int
	}{
		{name: "shuffle partitions", shufflePartitionsCnt: 0, partSize: 1},
		{name: "part size", shufflePartitionsCnt: 1, partSize: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("MakeJob should panic for non-positive partition and part sizes")
				}
			}()
			MakeJob(
				fixtures.Data("a"),
				fixtures.Mapper,
				fixtures.Reducer,
				func(*Job[string, fixtures.Key, int, int]) {},
				test.shufflePartitionsCnt,
				test.partSize,
			)
		})
	}
}
