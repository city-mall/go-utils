package types

import "sync"

type AsyncMap[U comparable, T any] struct {
	items sync.Map
}

func (m *AsyncMap[U, T]) Get(key U) (T, bool) {
	itemRaw, ok := m.items.Load(key)
	var item T
	if ok {
		item = itemRaw.(T)
	}
	return item, ok
}

func (m *AsyncMap[U, T]) Set(key U, val T) {
	m.items.Store(key, val)
}

func (m *AsyncMap[U, T]) Keys() []U {
	keys := []U{}
	m.items.Range(func(key, value any) bool {
		keys = append(keys, key.(U))
		return true
	})
	return keys
}

func (m *AsyncMap[U, T]) Iterate() map[U]T {
	allPairs := make(map[U]T)
	m.items.Range(func(key, value any) bool {
		allPairs[key.(U)] = value.(T)
		return true
	})
	return allPairs
}

func (m *AsyncMap[U, T]) Delete(key U) {
	m.items.Delete(key)
}
