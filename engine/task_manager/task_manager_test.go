package taskmanager

import (
	"strings"
	"testing"

	"github.com/bug00n/StreamForge/engine/task"
	"github.com/bug00n/StreamForge/engine/contracts"
	"github.com/bug00n/StreamForge/engine/shuffle_buffer"
)

// ---

type Data string
type Key string

func (text Data) Split() []string {
	return strings.Split(string(text), "\n")
}

func (key Key) Hash() uint32 {
	return 0
}

func mapper(line string) []contracts.Resoult[Key, int] {
	return nil
}

func reducer(key Key, values []int) int {
	return 0
}

func putResMap(resoults []contracts.Resoult[Key, int]) {
	return
}

func putResReducer(key Key, res int) {
	return
}

func notify() {

}

// ---

func TestMapTask(t *testing.T) {
	text := `test`

	manager := MakeManager[string, Key, int, int](
		5,
		5,
	)

	taskMap := task.MakeMapTask(
		Data(text).Split(),
		mapper,
		putResMap,
		notify,
	)

	manager.PutMapTask(taskMap)
	if len(manager.freeMapTasks) != 1 {
		t.Fatal("MapTask-pipeline is incorect: failed putting into task-manager")
	}
	
	task_ := manager.GetMapTask()
	if len(manager.freeMapTasks) != 0 {
		t.Fatal("MapTask-pipeline is incorect: failed getting from task-manager")
	}
	if task_.Partition[0] != "test" {
		t.Fatal("MapTask-pipeline is incorect: failed getting from task-manager")
	}
}

func TestReduceTask(t *testing.T) {
	buff := buffer.Buffer[Key, int]{}
	buff.Shuffle = make(map[Key][]int)
	buff.Shuffle["test"] = append(buff.Shuffle["test"], 1)

	manager := MakeManager[string, Key, int, int](
		5,
		5,
	)

	taskReduce := task.MakeReduceTask(
		&buff,
		reducer,
		putResReducer,
		notify,
	)

	manager.PutReduceTask(taskReduce)
	if len(manager.freeReduceTasks) != 1 {
		t.Fatal("ReduceTask-pipeline is incorect: failed putting into task-manager")
	}
	
	task_ := manager.GetReduceTask()
	if len(manager.freeReduceTasks) != 0 {
		t.Fatal("ReduceTask-pipeline is incorect: failed getting from task-manager")
	}
	if len(task_.Buffer.Shuffle["test"]) != 1 {
		t.Fatal("ReduceTask-pipeline is incorect: failed getting from task-manager")
	}
	if task_.Buffer.Shuffle["test"][0] != 1 {
		t.Fatal("ReduceTask-pipeline is incorect: failed getting from task-manager")
	}
}
