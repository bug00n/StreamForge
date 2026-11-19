package taskmanager

import (
	"testing"

	"github.com/bug00n/StreamForge/engine/fixtures_wc"
	"github.com/bug00n/StreamForge/engine/shuffle_buffer"
	"github.com/bug00n/StreamForge/engine/task"
)

func TestMapTask(t *testing.T) {
	text := `test`

	manager := MakeManager[string, fixtures.Key, int, int](
		5,
		5,
	)

	taskMap := task.MakeMapTask(
		fixtures.Data(text).Split(),
		fixtures.Mapper,
		fixtures.PutResMap,
		fixtures.Notify,
		func() bool { return true },
		func(error) {},
	)

	manager.PutMapTask(taskMap)
	if len(manager.freeMapTasks) != 1 {
		t.Fatal("MapTask-pipeline is incorect: failed putting into task-manager")
	}

	task_, _ := manager.GetMapTaskOrClosed()
	if len(manager.freeMapTasks) != 0 {
		t.Fatal("MapTask-pipeline is incorect: failed getting from task-manager")
	}
	if task_.Partition[0] != "test" {
		t.Fatal("MapTask-pipeline is incorect: failed getting from task-manager")
	}
}

func TestReduceTask(t *testing.T) {
	buff := buffer.Buffer[fixtures.Key, int]{}
	buff.Shuffle = make(map[fixtures.Key][]int)
	buff.Shuffle["test"] = append(buff.Shuffle["test"], 1)

	manager := MakeManager[string, fixtures.Key, int, int](
		5,
		5,
	)

	taskReduce := task.MakeReduceTask(
		&buff,
		fixtures.Reducer,
		fixtures.PutResReducer,
		fixtures.Notify,
		func() bool { return true },
		func(error) {},
	)

	manager.PutReduceTask(taskReduce)
	if len(manager.freeReduceTasks) != 1 {
		t.Fatal("ReduceTask-pipeline is incorect: failed putting into task-manager")
	}

	task_, _ := manager.GetReduceTaskOrClosed()
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
