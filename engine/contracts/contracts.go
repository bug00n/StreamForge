package contracts

type Data[T any] interface {
	Split()	[]T
}

type Key interface {
	comparable
	Hash()	uint32
}

type Value[T any] interface {
	Merge(other T) T
}

type Resoult[K Key, V Value[V]] struct {
	Key		K
	Value	V
}
