package fixtures

import (
	"strings"

//	"github.com/bug00n/StreamForge/engine/task"
	"github.com/bug00n/StreamForge/engine/contracts"
//	"github.com/bug00n/StreamForge/engine/shuffle_buffer"
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

func Mapper(line string) []contracts.Resoult[Key, int] {
	words := strings.Fields(line)
	resoult := make([]contracts.Resoult[Key, int], len(words))
	for i, word := range words {
		resoult[i] = contracts.Resoult[Key, int]{
			Key: Key(word),
			Value: 1,
		}
	}
	return resoult
}

func Reducer(key Key, values []int) int {
	sum := 0
	for _, val := range values {
		sum += val
	}
	return sum
}

func PutResMap(resoults []contracts.Resoult[Key, int]) {
	return
}

func PutResReducer(key Key, res int) {
	return
}

func Notify() {

}
