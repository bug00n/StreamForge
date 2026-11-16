package engine

import (
	"time"
	"testing"
	"github.com/bug00n/StreamForge/engine/fixtures_wc"
)

func TestSimple(t *testing.T) {
	const text string = `a a a
	a b b`

	eng := CreateEngine[string, fixtures.Key, int, int](
		5,
		5,
	)

	partitions := fixtures.Data(text)
	job_view := eng.Submit(
		partitions,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	select {
	case <-*job_view.Done():
		res := job_view.Load()
		if res["a"] != 4 || res["b"] != 2 || len(res) != 2 {
			t.Fatalf("wrong answer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job execution timeout")
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
	)

	partitions_1 := fixtures.Data(text_1)
	job1_view := eng.Submit(
		partitions_1,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	partitions_2 := fixtures.Data(text_2)
	job2_view := eng.Submit(
		partitions_2,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	select {
	case <-*job1_view.Done():
		res := job1_view.Load()
		if res["a"] != 8 || res["b"] != 8 || res["c"] != 9 || res["d"] != 2 || res["e"] != 3 || len(res) != 5 {
			t.Fatalf("wrong answer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job_1 execution timeout")
	}

	select {
	case <-*job2_view.Done():
		res := job2_view.Load()
		if res["a"] != 6 || res["b"] != 6 || res["d"] != 5 || res["e"] != 8 || len(res) != 4 {
			t.Fatalf("wrong answer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job_2 execution timeout")
	}
}

func TestEmpty(t *testing.T) {
	const text string = ``

	eng := CreateEngine[string, fixtures.Key, int, int](
		5,
		5,
	)

	partitions := fixtures.Data(text)
	job_view := eng.Submit(
		partitions,
		fixtures.Mapper,
		fixtures.Reducer,
	)

	select {
	case <-*job_view.Done():
		res := job_view.Load()
		if len(res) != 0 {
			t.Fatalf("wrong answer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job execution timeout")
	}
}
