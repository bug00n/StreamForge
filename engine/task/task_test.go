package task

import (
	"testing"

	"github.com/bug00n/StreamForge/engine/contracts"
	"github.com/bug00n/StreamForge/engine/fixtures_wc"
	"github.com/bug00n/StreamForge/engine/shuffle_buffer"
)

func TestMakeMapTask(t *testing.T) {
	partition := fixtures.Data("a a\nb").Split()
	var stored []contracts.Resoult[fixtures.Key, int]
	notifyCalls := 0

	mapTask := MakeMapTask[string, fixtures.Key, int](
		partition,
		fixtures.Mapper,
		func(results []contracts.Resoult[fixtures.Key, int]) {
			stored = results
		},
		func() { notifyCalls++ },
	)

	if len(mapTask.Partition) != 2 || mapTask.Partition[0] != "a a" || mapTask.Partition[1] != "b" {
		t.Fatalf("partition = %v, want [a a b]", mapTask.Partition)
	}
	if mapTask.WorkerID != -1 || mapTask.Status != TaskPending {
		t.Fatalf("initial worker/status = %d/%v, want -1/%v", mapTask.WorkerID, mapTask.Status, TaskPending)
	}
	if mapTask.Mapper == nil || mapTask.PutRes == nil || mapTask.Notify == nil {
		t.Fatal("map task callbacks should be initialized")
	}

	mapTask.PutRes(mapTask.Mapper(mapTask.Partition[0]))
	if len(stored) != 2 || stored[0].Key != "a" || stored[1].Key != "a" {
		t.Fatalf("stored mapper results = %v, want two results for a", stored)
	}
	mapTask.Notify()
	if notifyCalls != 1 {
		t.Fatalf("notify calls = %d, want 1", notifyCalls)
	}
}

func TestMakeReduceTask(t *testing.T) {
	shuffle := &buffer.Buffer[fixtures.Key, int]{
		Shuffle: map[fixtures.Key][]int{"a": {1, 2}},
	}
	var storedKey fixtures.Key
	var storedResult int
	notifyCalls := 0

	reduceTask := MakeReduceTask(
		shuffle,
		fixtures.Reducer,
		func(key fixtures.Key, result int) {
			storedKey, storedResult = key, result
		},
		func() { notifyCalls++ },
	)

	if reduceTask.Buffer != shuffle {
		t.Fatal("reduce task should keep the provided buffer")
	}
	if reduceTask.WorkerID != -1 || reduceTask.Status != TaskPending {
		t.Fatalf("initial worker/status = %d/%v, want -1/%v", reduceTask.WorkerID, reduceTask.Status, TaskPending)
	}
	if reduceTask.Reducer == nil || reduceTask.PutRes == nil || reduceTask.Notify == nil {
		t.Fatal("reduce task callbacks should be initialized")
	}

	reduceTask.PutRes("a", reduceTask.Reducer("a", reduceTask.Buffer.Shuffle["a"]))
	if storedKey != "a" || storedResult != 3 {
		t.Fatalf("stored reduce result = %s:%d, want a:3", storedKey, storedResult)
	}
	reduceTask.Notify()
	if notifyCalls != 1 {
		t.Fatalf("notify calls = %d, want 1", notifyCalls)
	}
}
