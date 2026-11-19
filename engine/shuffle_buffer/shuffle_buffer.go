package buffer

import (
	"sync"
	"github.com/bug00n/StreamForge/engine/contracts"
)

type Buffer[K contracts.Key, V any] struct {
	Mu 		sync.Mutex
	Shuffle	map[K][]V
}