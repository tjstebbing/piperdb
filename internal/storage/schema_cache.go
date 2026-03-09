package storage

import (
	"sync"
	"time"

	"github.com/tjstebbing/piper/piperdb/pkg/types"
)

// SchemaCache provides in-memory caching of schemas for fast access
type SchemaCache struct {
	schemas map[string]*cachedSchema
	mutex   sync.RWMutex
	maxSize int
}

type cachedSchema struct {
	schema    *types.Schema
	timestamp time.Time
	hits      int64
}

// NewSchemaCache creates a new schema cache
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		schemas: make(map[string]*cachedSchema),
		maxSize: 1000, // Cache up to 1000 schemas
	}
}

// Get retrieves a schema from cache
func (sc *SchemaCache) Get(listID string) (*types.Schema, bool) {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	cached, exists := sc.schemas[listID]
	if !exists {
		return nil, false
	}

	cached.hits++
	cached.timestamp = time.Now()
	
	// Return a copy to prevent mutation
	return cached.schema.Clone(), true
}

// Set stores a schema in cache
func (sc *SchemaCache) Set(listID string, schema *types.Schema) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	// If cache is full, remove least recently used item
	if len(sc.schemas) >= sc.maxSize {
		sc.evictLRU()
	}

	sc.schemas[listID] = &cachedSchema{
		schema:    schema.Clone(),
		timestamp: time.Now(),
		hits:      0,
	}
}

// Remove removes a schema from cache
func (sc *SchemaCache) Remove(listID string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	delete(sc.schemas, listID)
}

// Clear removes all schemas from cache
func (sc *SchemaCache) Clear() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.schemas = make(map[string]*cachedSchema)
}

// Stats returns cache statistics
func (sc *SchemaCache) Stats() map[string]interface{} {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	totalHits := int64(0)
	for _, cached := range sc.schemas {
		totalHits += cached.hits
	}

	return map[string]interface{}{
		"size":       len(sc.schemas),
		"max_size":   sc.maxSize,
		"total_hits": totalHits,
	}
}

// evictLRU removes the least recently used schema from cache
func (sc *SchemaCache) evictLRU() {
	var oldestID string
	var oldestTime time.Time

	for listID, cached := range sc.schemas {
		if oldestID == "" || cached.timestamp.Before(oldestTime) {
			oldestID = listID
			oldestTime = cached.timestamp
		}
	}

	if oldestID != "" {
		delete(sc.schemas, oldestID)
	}
}
