package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/bbolt"

	"github.com/tjstebbing/piperdb/pkg/types"
)

// IndexManager handles creation and management of field indexes
type IndexManager struct {
	indexes map[string]*indexMetadata
	mutex   sync.RWMutex
}

type indexMetadata struct {
	ListID      string    `json:"list_id"`
	FieldName   string    `json:"field_name"`
	IndexType   string    `json:"index_type"`
	CreatedAt   time.Time `json:"created_at"`
	LastUsed    time.Time `json:"last_used"`
	HitCount    int64     `json:"hit_count"`
	Size        int64     `json:"size"`
	Selectivity float64   `json:"selectivity"`
}

// NewIndexManager creates a new index manager
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*indexMetadata),
	}
}

// CreateIndex creates a new index for a field
func (im *IndexManager) CreateIndex(tx *bbolt.Tx, listID, fieldName, indexType string) error {
	indexKey := fmt.Sprintf("%s.%s", listID, fieldName)
	
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// Check if index already exists
	if _, exists := im.indexes[indexKey]; exists {
		return fmt.Errorf("index already exists for field %s in list %s", fieldName, listID)
	}

	// Create index bucket
	indexesBucket := tx.Bucket([]byte(listID + IndexesSuffix))
	if indexesBucket == nil {
		return fmt.Errorf("indexes bucket for list %s not found", listID)
	}

	indexBucket, err := indexesBucket.CreateBucketIfNotExists([]byte(fieldName))
	if err != nil {
		return fmt.Errorf("failed to create index bucket: %w", err)
	}

	// Build index by scanning all items
	itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
	if itemsBucket == nil {
		return fmt.Errorf("items bucket for list %s not found", listID)
	}

	indexEntries := 0
	uniqueValues := make(map[string]bool)

	cursor := itemsBucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		var storedItem StoredItem
		if err := json.Unmarshal(v, &storedItem); err != nil {
			continue // Skip malformed items
		}

		// Extract field value
		if fieldValue, exists := storedItem.Data[fieldName]; exists {
			valueStr := fmt.Sprintf("%v", fieldValue)
			indexKey := fmt.Sprintf("%s#%s", valueStr, storedItem.ID)
			
			// Store in index
			if err := indexBucket.Put([]byte(indexKey), k); err != nil {
				return fmt.Errorf("failed to store index entry: %w", err)
			}

			indexEntries++
			uniqueValues[valueStr] = true
		}
	}

	// Calculate selectivity
	selectivity := 1.0
	if indexEntries > 0 {
		selectivity = float64(len(uniqueValues)) / float64(indexEntries)
	}

	// Store index metadata
	metadata := &indexMetadata{
		ListID:      listID,
		FieldName:   fieldName,
		IndexType:   indexType,
		CreatedAt:   time.Now(),
		LastUsed:    time.Now(),
		HitCount:    0,
		Size:        int64(indexEntries),
		Selectivity: selectivity,
	}

	// Save metadata to database
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal index metadata: %w", err)
	}

	metaBucket, err := indexesBucket.CreateBucketIfNotExists([]byte("_meta"))
	if err != nil {
		return fmt.Errorf("failed to create meta bucket: %w", err)
	}

	if err := metaBucket.Put([]byte(fieldName), metadataJSON); err != nil {
		return fmt.Errorf("failed to store index metadata: %w", err)
	}

	// Store in memory
	im.indexes[fmt.Sprintf("%s.%s", listID, fieldName)] = metadata

	return nil
}

// DropIndex removes an index
func (im *IndexManager) DropIndex(tx *bbolt.Tx, listID, fieldName string) error {
	indexKey := fmt.Sprintf("%s.%s", listID, fieldName)
	
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// Remove from database
	indexesBucket := tx.Bucket([]byte(listID + IndexesSuffix))
	if indexesBucket == nil {
		return fmt.Errorf("indexes bucket for list %s not found", listID)
	}

	// Delete index bucket
	if err := indexesBucket.DeleteBucket([]byte(fieldName)); err != nil && err != bbolt.ErrBucketNotFound {
		return fmt.Errorf("failed to delete index bucket: %w", err)
	}

	// Delete metadata
	metaBucket := indexesBucket.Bucket([]byte("_meta"))
	if metaBucket != nil {
		metaBucket.Delete([]byte(fieldName))
	}

	// Remove from memory
	delete(im.indexes, indexKey)

	return nil
}

// HasIndex checks if an index exists for a field
func (im *IndexManager) HasIndex(listID, fieldName string) bool {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	indexKey := fmt.Sprintf("%s.%s", listID, fieldName)
	_, exists := im.indexes[indexKey]
	return exists
}

// GetIndexInfo returns information about an index
func (im *IndexManager) GetIndexInfo(listID, fieldName string) (*types.IndexInfo, error) {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	indexKey := fmt.Sprintf("%s.%s", listID, fieldName)
	metadata, exists := im.indexes[indexKey]
	if !exists {
		return nil, fmt.Errorf("index not found for field %s in list %s", fieldName, listID)
	}

	return &types.IndexInfo{
		FieldName:   metadata.FieldName,
		IndexType:   metadata.IndexType,
		Size:        metadata.Size,
		Selectivity: metadata.Selectivity,
		LastUsed:    metadata.LastUsed,
		HitCount:    metadata.HitCount,
		CreatedAt:   metadata.CreatedAt,
	}, nil
}

// ListIndexes returns all indexes for a list
func (im *IndexManager) ListIndexes(listID string) ([]*types.IndexInfo, error) {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	var indexes []*types.IndexInfo

	for _, metadata := range im.indexes {
		if metadata.ListID == listID {
			indexes = append(indexes, &types.IndexInfo{
				FieldName:   metadata.FieldName,
				IndexType:   metadata.IndexType,
				Size:        metadata.Size,
				Selectivity: metadata.Selectivity,
				LastUsed:    metadata.LastUsed,
				HitCount:    metadata.HitCount,
				CreatedAt:   metadata.CreatedAt,
			})
		}
	}

	return indexes, nil
}

// RecordIndexHit records that an index was used
func (im *IndexManager) RecordIndexHit(listID, fieldName string) {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	indexKey := fmt.Sprintf("%s.%s", listID, fieldName)
	if metadata, exists := im.indexes[indexKey]; exists {
		metadata.HitCount++
		metadata.LastUsed = time.Now()
	}
}

// ShouldCreateIndex determines if an index should be created based on usage patterns
func (im *IndexManager) ShouldCreateIndex(fieldUsage map[string]int64, threshold int64) bool {
	// Simple heuristic: create index if field is queried frequently
	for _, count := range fieldUsage {
		if count >= threshold {
			return true
		}
	}
	return false
}

// LoadIndexes loads index metadata from database
func (im *IndexManager) LoadIndexes(tx *bbolt.Tx, listID string) error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	indexesBucket := tx.Bucket([]byte(listID + IndexesSuffix))
	if indexesBucket == nil {
		return nil // No indexes bucket exists
	}

	metaBucket := indexesBucket.Bucket([]byte("_meta"))
	if metaBucket == nil {
		return nil // No metadata bucket exists
	}

	// Load all index metadata
	return metaBucket.ForEach(func(k, v []byte) error {
		var metadata indexMetadata
		if err := json.Unmarshal(v, &metadata); err != nil {
			return err // Skip malformed metadata
		}

		indexKey := fmt.Sprintf("%s.%s", listID, string(k))
		im.indexes[indexKey] = &metadata

		return nil
	})
}

// GetIndexStats returns statistics about all indexes
func (im *IndexManager) GetIndexStats() map[string]interface{} {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	totalIndexes := len(im.indexes)
	totalHits := int64(0)
	totalSize := int64(0)

	for _, metadata := range im.indexes {
		totalHits += metadata.HitCount
		totalSize += metadata.Size
	}

	return map[string]interface{}{
		"total_indexes": totalIndexes,
		"total_hits":    totalHits,
		"total_size":    totalSize,
	}
}
