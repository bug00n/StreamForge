package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/bug00n/StreamForge/engine/fixtures_wc"
)

func TestSimple(t *testing.T) {
	const text string = `a a a
	a b b`

	eng := CreateEngine[string, fixtures.Key, int, int](
		5,
		2,
		3,
	)
	t.Cleanup(eng.Close)

	partitions := fixtures.Data(text)
	job_view, err := eng.Submit(
		partitions,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-job_view.Done():
		res := job_view.Load()
		if res["a"] != 4 || res["b"] != 2 || len(res) != 2 {
			t.Fatalf("wrong answer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job execution timeout")
	}
}

func TestCreateEngineRejectsNonPositiveCounts(t *testing.T) {
	tests := []struct {
		name              string
		mappersCnt        int
		reducersCnt       int
		shufflePartitions int
	}{
		{name: "mappers", mappersCnt: 0, reducersCnt: 1, shufflePartitions: 1},
		{name: "reducers", mappersCnt: 1, reducersCnt: 0, shufflePartitions: 1},
		{name: "shuffle partitions", mappersCnt: 1, reducersCnt: 1, shufflePartitions: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("CreateEngine should panic for non-positive counts")
				}
			}()
			CreateEngine[string, fixtures.Key, int, int](
				test.mappersCnt,
				test.reducersCnt,
				test.shufflePartitions,
			)
		})
	}
}

func TestSomeJobs(t *testing.T) {
	const text_1 string = `a a a
	a b b a a a a b b b b b b
	c c c c c c d d e e e c c c`

	const text_2 string = `a b a
	a b b d d d d d
	a a a b b b e e e e e e e e`

	eng := CreateEngine[string, fixtures.Key, int, int](
		5,
		5,
		5,
	)
	t.Cleanup(eng.Close)

	partitions_1 := fixtures.Data(text_1)
	job1_view, err := eng.Submit(
		partitions_1,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	partitions_2 := fixtures.Data(text_2)
	job2_view, err := eng.Submit(
		partitions_2,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-job1_view.Done():
		res := job1_view.Load()
		if res["a"] != 8 || res["b"] != 8 || res["c"] != 9 || res["d"] != 2 || res["e"] != 3 || len(res) != 5 {
			t.Fatalf("wrong answer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job_1 execution timeout")
	}

	select {
	case <-job2_view.Done():
		res := job2_view.Load()
		if res["a"] != 6 || res["b"] != 6 || res["d"] != 5 || res["e"] != 8 || len(res) != 4 {
			t.Fatalf("wrong answer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job_2 execution timeout")
	}
}

type Data_ string

func (d Data_) Split() []string {
	return nil
}
func TestEmpty(t *testing.T) {
	

	const text string = ``

	eng := CreateEngine[string, fixtures.Key, int, int](
		5,
		5,
		5,
	)
	t.Cleanup(eng.Close)

	partitions := Data_(text)
	job_view, err := eng.Submit(
		partitions,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-job_view.Done():
		res := job_view.Load()
		if len(res) != 0 {
			t.Fatalf("wrong answer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job execution timeout")
	}
}

func TestStopping(t *testing.T) {
	const text string = `a a a a a`

	eng := CreateEngine[string, fixtures.Key, int, int](
		5,
		5,
		5,
	)
	t.Cleanup(eng.Close)

	partitions := fixtures.Data(text)
	doneChannels := make([]<-chan struct{}, 0, 100)
	for i := 0; i < 100; i++ {
		view, err := eng.Submit(
			partitions,
			fixtures.Mapper,
			fixtures.Reducer,
		)

		if err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
		doneChannels = append(doneChannels, view.Done())
	}

	eng.Close()
	for i, done := range doneChannels {
		select {
		case <-done:
		default:
			t.Errorf("job %d was not complete when Close returned", i)
		}
	}

	_, err := eng.Submit(
		partitions,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	if !errors.Is(err, ErrEngineClosed) {
		t.Fatalf("Submit after Close error = %v, want %v", err, ErrEngineClosed)
	}
}
