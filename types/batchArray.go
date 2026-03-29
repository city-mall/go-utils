package types

import (
	"sync"
)

type BatchArray[T any] struct {
	items sync.Map
}

func (m *BatchArray[T]) Get(key string) (T, bool) {
	itemRaw, ok := m.items.Load(key)
	var item T
	if ok {
		item = itemRaw.(T)
	}
	return item, ok
}

func (m *BatchArray[T]) Set(key string, val T) {
	m.items.Store(key, val)
}

func (m *BatchArray[T]) Keys() []string {
	keys := []string{}
	m.items.Range(func(key, value any) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}

func (m *BatchArray[T]) Iterate() map[string]T {
	allPairs := make(map[string]T)
	m.items.Range(func(key, value any) bool {
		allPairs[key.(string)] = value.(T)
		return true
	})
	return allPairs
}

func (m *BatchArray[T]) Delete(key string) {
	m.items.Delete(key)
}
