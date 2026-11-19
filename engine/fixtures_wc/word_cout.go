package fixtures

import (
	"strings"
	"github.com/bug00n/StreamForge/engine/contracts"
)

type Data string
type Key string

func (text Data) Split() []string {
	return strings.Split(string(text), "\n")
}

func (key Key) Hash() uint32 {
	var p, hash uint32 = 67, 0
	for i := 0; i < len(key); i++ {
		hash *= p
		hash += uint32(key[i])
	}
	return hash
}

func Mapper(line string) ([]contracts.Result[Key, int], error) {
	words := strings.Fields(line)
	resoult := make([]contracts.Result[Key, int], len(words))
	for i, word := range words {
		resoult[i] = contracts.Result[Key, int]{
			Key: Key(word),
			Value: 1,
		}
	}
	return resoult, nil
}

func Reducer(key Key, values []int) (int, error) {
	sum := 0
	for _, val := range values {
		sum += val
	}
	return sum, nil
}

func PutResMap(resoults []contracts.Result[Key, int]) {
}

func PutResReducer(key Key, res int) {
}

func Notify() {

}
