# StreamForge

StreamForge — экспериментальная библиотека на Go для выполнения MapReduce-задач в одном процессе. Она делит входные данные на map-задачи, группирует промежуточные значения по ключам и запускает reduce-задачи в пуле goroutine.

Библиотека хранит промежуточные данные в памяти и выполняет работу на одной машине. Она подходит для простых задач обработки данных.

## Требования

- Go 1.26.6 или новее — версия, указанная в `go.mod`.
- Сторонние Go-модули проекту не нужны.

## Установка

В проекте, который использует StreamForge, добавьте модуль:

```sh
go get github.com/bug00n/StreamForge
```

## Пример: подсчёт слов

Встроенный пример находится в пакете `engine/fixtures_wc`:

```go
package main

import (
	"fmt"

	"github.com/bug00n/StreamForge/engine"
	"github.com/bug00n/StreamForge/engine/fixtures_wc"
)

func main() {
	eng := engine.CreateEngine[string, fixtures.Key, int, int](
		4, // число map workers
		2, // число reduce workers
		3, // число shuffle-партиций
	)
	defer eng.Close()

	view, err := eng.Submit(
		fixtures.Data("go map reduce\ngo map"),
		fixtures.Mapper,
		fixtures.Reducer,
	)
	if err != nil {
		panic(err)
	}

	<-view.Done()
	if err := view.Err(); err != nil {
		panic(err)
	}

	fmt.Println(view.Load())
}
```

В результате будут пары `go: 2`, `map: 2` и `reduce: 1`. Порядок обхода и печати элементов Go map не гарантируется.

## Как выполняется задача

1. Входные данные реализуют `contracts.Data[T]`: метод `Split()` возвращает срез элементов типа `T`.
2. Движок делит срез на map-задачи размером не более 500 элементов.
3. Mapper обрабатывает каждый элемент и возвращает срез пар `contracts.Result[K, V]` и ошибку.
4. Движок отправляет значения одного ключа в shuffle-партицию по результату `Key.Hash()` и группирует значения по ключу.
5. Для каждой shuffle-партиции создаётся reduce-задача. Reduce workers выполняют эти задачи параллельно; лишние задачи ждут в очереди.
6. Когда все reduce-задачи завершены, канал `Done()` закрывается. Если callback вернул ошибку, задача завершается со статусом `JobFailed`, и `Done()` также закрывается.

Число shuffle-партиций определяет группировку ключей и не обязано совпадать с числом reduce workers.

## Настройка движка

```go
eng := engine.CreateEngine[T, K, V, R](mappersCnt, reducersCnt, shufflePartitionsCnt)
```

- `mappersCnt` — число map workers.
- `reducersCnt` — число reduce workers.
- `shufflePartitionsCnt` — число shuffle-партиций.

Все три значения должны быть положительными: при значении `0` или меньше `CreateEngine` вызывает panic.

`Shutdown()` прекращает приём новых задач. `Close()` делает то же самое и ждёт завершения уже принятых задач и workers. После остановки `Submit()` возвращает `ErrEngineClosed`.

## Требования к типам и функциям

- `T` — тип элемента, который возвращает `Data[T].Split()`.
- `K` должен быть сравнимым типом и реализовывать метод `Hash() uint32`. Равные ключи должны возвращать одинаковый хеш, чтобы их значения попали в одну shuffle-партицию.
- Mapper имеет сигнатуру `func(T) ([]contracts.Result[K, V], error)`.
- Reducer имеет сигнатуру `func(K, []V) (R, error)`.
- `V` и `R` могут быть любыми Go-типами.

Mapper и reducer могут вызываться одновременно несколькими workers. Если callbacks обращаются к общему изменяемому состоянию, вызывающий код должен синхронизировать к нему доступ. Порядок вызовов, значений в `[]V` и обхода результирующей map не гарантируется.

При ошибке callback задача получает статус `JobFailed`, а причину можно прочитать через `view.Err()`. Уже выполняющийся callback нельзя прервать; задача проверяет свой статус перед следующим вызовом.

## Получение результатов

`Submit()` возвращает `JobViewer[T, K, V, R]`. После закрытия `Done()` проверьте `Err()`: `nil` означает успешное выполнение. Текущий статус задачи доступен через `Status()`.

```go
view, err := eng.Submit(data, mapper, reducer)
if err != nil {
	panic(err)
}

<-view.Done()
if err := view.Err(); err != nil {
	panic(err)
}

results := view.Load()
```

`Load()` возвращает `nil`, пока задача не завершилась успешно. После успеха он возвращает копию результирующей map; сами значения `R` и вложенные в них данные не копируются. Если `Split()` вернул пустой срез, задача сразу завершается успешно с пустой map.

## Разработка

Чтобы запустить тесты из корня репозитория:

```sh
go test ./...
```
