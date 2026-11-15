package buffer

import (
	"sync"
	"github.com/bug00n/StreamForge/engine/contracts"
)

type Buffer[K contracts.Key, V contracts.Value[V]] struct {
	Mu 		sync.Mutex
	Shuffle	map[K][]V
}