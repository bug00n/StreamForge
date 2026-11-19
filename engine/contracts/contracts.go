package contracts

type Data[T any] interface {
	Split()	[]T
}

type Key interface {
	comparable
	Hash()	uint32
}

type Result[K Key, V any] struct {
	Key		K
	Value	V
}
