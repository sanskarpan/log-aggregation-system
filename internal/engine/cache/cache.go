package cache

import (
	"container/list"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/chunk"
)

type QueryStats struct {
	Entries   int    `json:"entries"`
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Evictions uint64 `json:"evictions"`
}

type ChunkStats struct {
	Entries   int    `json:"entries"`
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Evictions uint64 `json:"evictions"`
}

type QueryCache struct {
	mu       sync.Mutex
	maxItems int
	ttl      time.Duration
	items    map[string]*list.Element
	order    *list.List
	hits     uint64
	misses   uint64
	evicted  uint64
}

type queryEntry struct {
	key       string
	value     model.QueryResult
	expiresAt time.Time
}

func NewQueryCache(maxItems int, ttl time.Duration) *QueryCache {
	if maxItems <= 0 {
		maxItems = 256
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &QueryCache{
		maxItems: maxItems,
		ttl:      ttl,
		items:    map[string]*list.Element{},
		order:    list.New(),
	}
}

func (c *QueryCache) Get(key string) (model.QueryResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		c.misses++
		return model.QueryResult{}, false
	}
	entry := element.Value.(*queryEntry)
	if time.Now().After(entry.expiresAt) {
		c.removeElement(element)
		c.misses++
		return model.QueryResult{}, false
	}
	c.order.MoveToFront(element)
	c.hits++
	return entry.value, true
}

func (c *QueryCache) Put(key string, value model.QueryResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.items[key]; ok {
		entry := element.Value.(*queryEntry)
		entry.value = value
		entry.expiresAt = time.Now().Add(c.ttl)
		c.order.MoveToFront(element)
		return
	}
	if c.order.Len() >= c.maxItems {
		c.removeElement(c.order.Back())
	}
	entry := &queryEntry{key: key, value: value, expiresAt: time.Now().Add(c.ttl)}
	element := c.order.PushFront(entry)
	c.items[key] = element
}

func (c *QueryCache) Stats() QueryStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return QueryStats{
		Entries:   c.order.Len(),
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evicted,
	}
}

func (c *QueryCache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = map[string]*list.Element{}
	c.order.Init()
	c.hits = 0
	c.misses = 0
	c.evicted = 0
}

func (c *QueryCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}
	entry := element.Value.(*queryEntry)
	delete(c.items, entry.key)
	c.order.Remove(element)
	c.evicted++
}

type ChunkCache struct {
	mu       sync.Mutex
	maxItems int
	items    map[string]*list.Element
	order    *list.List
	hits     uint64
	misses   uint64
	evicted  uint64
}

type chunkEntry struct {
	key   string
	value *chunk.Chunk
}

func NewChunkCache(maxItems int) *ChunkCache {
	if maxItems <= 0 {
		maxItems = 512
	}
	return &ChunkCache{
		maxItems: maxItems,
		items:    map[string]*list.Element{},
		order:    list.New(),
	}
}

func (c *ChunkCache) Get(key string) (*chunk.Chunk, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}
	c.order.MoveToFront(element)
	c.hits++
	entry := element.Value.(*chunkEntry)
	if entry.value == nil {
		return nil, false
	}
	clone := *entry.value
	if len(entry.value.Events) > 0 {
		clone.Events = append([]model.Event(nil), entry.value.Events...)
	}
	return &clone, true
}

func (c *ChunkCache) Put(key string, value *chunk.Chunk) {
	if value == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.items[key]; ok {
		element.Value.(*chunkEntry).value = cloneChunk(value)
		c.order.MoveToFront(element)
		return
	}
	if c.order.Len() >= c.maxItems {
		c.removeElement(c.order.Back())
	}
	entry := &chunkEntry{key: key, value: cloneChunk(value)}
	element := c.order.PushFront(entry)
	c.items[key] = element
}

func (c *ChunkCache) Stats() ChunkStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ChunkStats{
		Entries:   c.order.Len(),
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evicted,
	}
}

func (c *ChunkCache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = map[string]*list.Element{}
	c.order.Init()
	c.hits = 0
	c.misses = 0
	c.evicted = 0
}

func (c *ChunkCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}
	entry := element.Value.(*chunkEntry)
	delete(c.items, entry.key)
	c.order.Remove(element)
	c.evicted++
}

func cloneChunk(ch *chunk.Chunk) *chunk.Chunk {
	if ch == nil {
		return nil
	}
	clone := *ch
	if len(ch.Events) > 0 {
		clone.Events = append([]model.Event(nil), ch.Events...)
	}
	return &clone
}
