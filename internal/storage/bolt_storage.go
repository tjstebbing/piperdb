package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
	"github.com/google/uuid"

	"github.com/tjstebbing/piperdb/pkg/types"
)

// BoltStorage implements the storage layer using BoltDB
type BoltStorage struct {
	db           *bbolt.DB
	schemaCache  *SchemaCache
	indexManager *IndexManager
	stats        *StorageStats
}

// StoredItem represents an item as stored in BoltDB
type StoredItem struct {
	ID       string                 `json:"id"`
	Position int64                  `json:"position"`
	Data     map[string]interface{} `json:"data"`
	Hash     uint64                 `json:"hash"`
	Created  time.Time              `json:"created"`
	Updated  time.Time              `json:"updated"`
}

// Bucket names
const (
	MetaBucket    = "_meta"
	ListsBucket   = "_lists"
	ItemsSuffix   = "-items"
	SchemaSuffix  = "-schema"
	IndexesSuffix = "-indexes"
)

// NewBoltStorage creates a new BoltDB-based storage engine
func NewBoltStorage(path string) (*BoltStorage, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	storage := &BoltStorage{
		db:           db,
		schemaCache:  NewSchemaCache(),
		indexManager: NewIndexManager(),
		stats:        NewStorageStats(),
	}

	// Initialize database structure
	if err := storage.initDatabase(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return storage, nil
}

// initDatabase creates the initial bucket structure
func (bs *BoltStorage) initDatabase() error {
	return bs.db.Update(func(tx *bbolt.Tx) error {
		// Create metadata bucket
		if _, err := tx.CreateBucketIfNotExists([]byte(MetaBucket)); err != nil {
			return err
		}

		// Create lists registry bucket
		if _, err := tx.CreateBucketIfNotExists([]byte(ListsBucket)); err != nil {
			return err
		}

		// Store database version and creation time
		metaBucket := tx.Bucket([]byte(MetaBucket))
		if metaBucket.Get([]byte("version")) == nil {
			now := time.Now()
			metaBucket.Put([]byte("version"), []byte("1.0.0"))
			metaBucket.Put([]byte("created"), []byte(now.Format(time.RFC3339)))
		}

		return nil
	})
}

// CreateList creates a new list with the given ID
func (bs *BoltStorage) CreateList(ctx context.Context, listID string) error {
	if listID == "" {
		return fmt.Errorf("listID cannot be empty")
	}

	return bs.db.Update(func(tx *bbolt.Tx) error {
		listsBucket := tx.Bucket([]byte(ListsBucket))

		// Check if list already exists
		if listsBucket.Get([]byte(listID)) != nil {
			return fmt.Errorf("list %s already exists", listID)
		}

		// Create list metadata
		list := &types.List{
			ID:        listID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Stats:     &types.ListStats{},
		}

		listJSON, err := json.Marshal(list)
		if err != nil {
			return fmt.Errorf("failed to marshal list: %w", err)
		}

		// Store list metadata
		if err := listsBucket.Put([]byte(listID), listJSON); err != nil {
			return err
		}

		// Create buckets for items, schema, and indexes
		bucketNames := []string{
			listID + ItemsSuffix,
			listID + SchemaSuffix,
			listID + IndexesSuffix,
		}

		for _, bucketName := range bucketNames {
			if _, err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
			}
		}

		// Initialize empty schema
		schema := &types.Schema{
			Fields:    make(map[string]*types.FieldDef),
			Version:   1,
			Inferred:  true,
			UpdatedAt: time.Now(),
		}

		schemaJSON, err := json.Marshal(schema)
		if err != nil {
			return fmt.Errorf("failed to marshal schema: %w", err)
		}

		schemaBucket := tx.Bucket([]byte(listID + SchemaSuffix))
		return schemaBucket.Put([]byte("current"), schemaJSON)
	})
}

// DeleteList removes a list and all its data
func (bs *BoltStorage) DeleteList(ctx context.Context, listID string) error {
	return bs.db.Update(func(tx *bbolt.Tx) error {
		// Remove from lists registry
		listsBucket := tx.Bucket([]byte(ListsBucket))
		if err := listsBucket.Delete([]byte(listID)); err != nil {
			return err
		}

		// Delete all associated buckets
		bucketNames := []string{
			listID + ItemsSuffix,
			listID + SchemaSuffix,
			listID + IndexesSuffix,
		}

		for _, bucketName := range bucketNames {
			if err := tx.DeleteBucket([]byte(bucketName)); err != nil && err != bbolt.ErrBucketNotFound {
				return fmt.Errorf("failed to delete bucket %s: %w", bucketName, err)
			}
		}

		// Clear from cache
		bs.schemaCache.Remove(listID)

		return nil
	})
}

// ListExists checks if a list exists
func (bs *BoltStorage) ListExists(ctx context.Context, listID string) (bool, error) {
	var exists bool

	err := bs.db.View(func(tx *bbolt.Tx) error {
		listsBucket := tx.Bucket([]byte(ListsBucket))
		exists = listsBucket.Get([]byte(listID)) != nil
		return nil
	})

	return exists, err
}

// GetListInfo returns the metadata for a list
func (bs *BoltStorage) GetListInfo(ctx context.Context, listID string) (*types.List, error) {
	var list *types.List

	err := bs.db.View(func(tx *bbolt.Tx) error {
		listsBucket := tx.Bucket([]byte(ListsBucket))
		listData := listsBucket.Get([]byte(listID))
		if listData == nil {
			return fmt.Errorf("list %s not found", listID)
		}

		list = &types.List{}
		return json.Unmarshal(listData, list)
	})

	if err != nil {
		return nil, err
	}

	// Get current stats
	stats, _ := bs.GetStats(ctx, listID)
	if stats != nil {
		list.Stats = stats
	}

	return list, nil
}

// ListAllLists returns all list IDs
func (bs *BoltStorage) ListAllLists(ctx context.Context) ([]string, error) {
	var listIDs []string

	err := bs.db.View(func(tx *bbolt.Tx) error {
		listsBucket := tx.Bucket([]byte(ListsBucket))
		
		return listsBucket.ForEach(func(k, v []byte) error {
			listIDs = append(listIDs, string(k))
			return nil
		})
	})

	return listIDs, err
}

// AddItem adds a new item to a list
func (bs *BoltStorage) AddItem(ctx context.Context, listID string, data map[string]interface{}) (string, error) {
	itemID := uuid.New().String()
	
	return itemID, bs.db.Update(func(tx *bbolt.Tx) error {
		itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
		if itemsBucket == nil {
			return fmt.Errorf("list %s not found", listID)
		}

		// Get next position
		position := bs.getNextPosition(itemsBucket)

		// Create stored item
		storedItem := &StoredItem{
			ID:       itemID,
			Position: position,
			Data:     data,
			Hash:     hashData(data),
			Created:  time.Now(),
			Updated:  time.Now(),
		}

		itemJSON, err := json.Marshal(storedItem)
		if err != nil {
			return fmt.Errorf("failed to marshal item: %w", err)
		}

		// Store item
		if err := itemsBucket.Put([]byte(itemID), itemJSON); err != nil {
			return err
		}

		// Update schema if needed
		if err := bs.updateSchemaForItem(tx, listID, data); err != nil {
			return fmt.Errorf("failed to update schema: %w", err)
		}

		// Update list stats
		return bs.updateListStats(tx, listID)
	})
}

// UpdateItem updates an existing item
func (bs *BoltStorage) UpdateItem(ctx context.Context, listID, itemID string, data map[string]interface{}) error {
	return bs.db.Update(func(tx *bbolt.Tx) error {
		itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
		if itemsBucket == nil {
			return fmt.Errorf("list %s not found", listID)
		}

		// Get existing item
		itemData := itemsBucket.Get([]byte(itemID))
		if itemData == nil {
			return fmt.Errorf("item %s not found", itemID)
		}

		var storedItem StoredItem
		if err := json.Unmarshal(itemData, &storedItem); err != nil {
			return fmt.Errorf("failed to unmarshal item: %w", err)
		}

		// Update item data
		storedItem.Data = data
		storedItem.Hash = hashData(data)
		storedItem.Updated = time.Now()

		itemJSON, err := json.Marshal(storedItem)
		if err != nil {
			return fmt.Errorf("failed to marshal updated item: %w", err)
		}

		// Store updated item
		if err := itemsBucket.Put([]byte(itemID), itemJSON); err != nil {
			return err
		}

		// Update schema if needed
		return bs.updateSchemaForItem(tx, listID, data)
	})
}

// DeleteItem removes an item from a list
func (bs *BoltStorage) DeleteItem(ctx context.Context, listID, itemID string) error {
	return bs.db.Update(func(tx *bbolt.Tx) error {
		itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
		if itemsBucket == nil {
			return fmt.Errorf("list %s not found", listID)
		}

		if err := itemsBucket.Delete([]byte(itemID)); err != nil {
			return err
		}

		// Update list stats
		return bs.updateListStats(tx, listID)
	})
}

// GetItem retrieves a single item
func (bs *BoltStorage) GetItem(ctx context.Context, listID, itemID string) (map[string]interface{}, error) {
	var data map[string]interface{}

	err := bs.db.View(func(tx *bbolt.Tx) error {
		itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
		if itemsBucket == nil {
			return fmt.Errorf("list %s not found", listID)
		}

		itemData := itemsBucket.Get([]byte(itemID))
		if itemData == nil {
			return fmt.Errorf("item %s not found", itemID)
		}

		var storedItem StoredItem
		if err := json.Unmarshal(itemData, &storedItem); err != nil {
			return fmt.Errorf("failed to unmarshal item: %w", err)
		}

		data = storedItem.Data
		return nil
	})

	return data, err
}

// GetItems retrieves all items from a list (basic implementation)
func (bs *BoltStorage) GetItems(ctx context.Context, listID string, opts *types.QueryOptions) (*types.ResultSet, error) {
	var items []map[string]interface{}
	var totalCount int64

	err := bs.db.View(func(tx *bbolt.Tx) error {
		itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
		if itemsBucket == nil {
			return fmt.Errorf("list %s not found", listID)
		}

		// Count total items
		stats := itemsBucket.Stats()
		totalCount = int64(stats.KeyN)

		// Iterate through items
		c := itemsBucket.Cursor()
		count := int64(0)
		skip := int64(0)
		limit := int64(1000) // default limit

		if opts != nil {
			skip = opts.Offset
			if opts.Limit > 0 {
				limit = opts.Limit
			}
		}

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if count < skip {
				count++
				continue
			}

			if int64(len(items)) >= limit {
				break
			}

			var storedItem StoredItem
			if err := json.Unmarshal(v, &storedItem); err != nil {
				continue // Skip malformed items
			}

			items = append(items, storedItem.Data)
			count++
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &types.ResultSet{
		Items:      items,
		TotalCount: totalCount,
		HasMore:    int64(len(items)) < totalCount,
	}, nil
}

// Helper functions

func (bs *BoltStorage) getNextPosition(bucket *bbolt.Bucket) int64 {
	stats := bucket.Stats()
	return int64(stats.KeyN)
}

func hashData(data map[string]interface{}) uint64 {
	// Simple hash implementation - could be improved
	json_bytes, _ := json.Marshal(data)
	var hash uint64 = 14695981039346656037 // FNV offset basis
	for _, b := range json_bytes {
		hash ^= uint64(b)
		hash *= 1099511628211 // FNV prime
	}
	return hash
}

func (bs *BoltStorage) updateSchemaForItem(tx *bbolt.Tx, listID string, data map[string]interface{}) error {
	// Get current schema
	schema, err := bs.getSchemaFromTx(tx, listID)
	if err != nil {
		return err
	}

	// Get total item count to properly update stats
	itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
	totalItems := int64(0)
	if itemsBucket != nil {
		totalItems = int64(itemsBucket.Stats().KeyN)
	}

	// Update schema with new fields
	for fieldName, value := range data {
		if _, exists := schema.Fields[fieldName]; !exists {
			// New field - infer type
			fieldType := inferFieldType(value)
			schema.Fields[fieldName] = &types.FieldDef{
				Type:        fieldType,
				Required:    false,
				SeenInCount: 1,
				TotalItems:  totalItems,
			}
		} else {
			// Existing field - update stats
			schema.Fields[fieldName].SeenInCount++
			// Update total items for all fields
			schema.Fields[fieldName].TotalItems = totalItems
		}
	}

	// Update TotalItems for all existing fields that weren't in this item
	for fieldName, field := range schema.Fields {
		if _, inCurrentItem := data[fieldName]; !inCurrentItem {
			field.TotalItems = totalItems
		}
	}

	// Always update schema to reflect current state
	schema.Version++
	schema.UpdatedAt = time.Now()

	// Save updated schema
	return bs.saveSchemaToTx(tx, listID, schema)
}

func (bs *BoltStorage) getSchemaFromTx(tx *bbolt.Tx, listID string) (*types.Schema, error) {
	schemaBucket := tx.Bucket([]byte(listID + SchemaSuffix))
	if schemaBucket == nil {
		return nil, fmt.Errorf("schema bucket for list %s not found", listID)
	}

	schemaData := schemaBucket.Get([]byte("current"))
	if schemaData == nil {
		// Return empty schema
		return &types.Schema{
			Fields:   make(map[string]*types.FieldDef),
			Version:  1,
			Inferred: true,
		}, nil
	}

	var schema types.Schema
	if err := json.Unmarshal(schemaData, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	return &schema, nil
}

func (bs *BoltStorage) saveSchemaToTx(tx *bbolt.Tx, listID string, schema *types.Schema) error {
	schemaBucket := tx.Bucket([]byte(listID + SchemaSuffix))
	if schemaBucket == nil {
		return fmt.Errorf("schema bucket for list %s not found", listID)
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	// Save to database
	if err := schemaBucket.Put([]byte("current"), schemaJSON); err != nil {
		return err
	}

	// Update cache
	bs.schemaCache.Set(listID, schema)

	return nil
}

func (bs *BoltStorage) updateListStats(tx *bbolt.Tx, listID string) error {
	// Simple stats update - count items
	itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
	if itemsBucket == nil {
		return nil
	}

	stats := itemsBucket.Stats()
	
	// Update list metadata with new stats
	listsBucket := tx.Bucket([]byte(ListsBucket))
	listData := listsBucket.Get([]byte(listID))
	if listData == nil {
		return nil
	}

	var list types.List
	if err := json.Unmarshal(listData, &list); err != nil {
		return err
	}

	list.UpdatedAt = time.Now()
	list.ItemCount = int64(stats.KeyN)

	listJSON, err := json.Marshal(list)
	if err != nil {
		return err
	}

	return listsBucket.Put([]byte(listID), listJSON)
}

func inferFieldType(value interface{}) types.FieldType {
	switch value.(type) {
	case string:
		return types.FieldString
	case int, int32, int64, float32, float64:
		return types.FieldNumber
	case bool:
		return types.FieldBoolean
	case []interface{}:
		return types.FieldArray
	case map[string]interface{}:
		return types.FieldObject
	default:
		return types.FieldString
	}
}

// GetStats returns statistics for a list
func (bs *BoltStorage) GetStats(ctx context.Context, listID string) (*types.ListStats, error) {
	var stats *types.ListStats

	err := bs.db.View(func(tx *bbolt.Tx) error {
		itemsBucket := tx.Bucket([]byte(listID + ItemsSuffix))
		if itemsBucket == nil {
			return fmt.Errorf("list %s not found", listID)
		}

		bucketStats := itemsBucket.Stats()
		
		// Calculate approximate size
		totalSize := int64(bucketStats.LeafPageN * 4096) // Page size approximation
		if totalSize == 0 && bucketStats.KeyN > 0 {
			totalSize = int64(bucketStats.KeyN * 1024) // Fallback estimate
		}
		
		// Get schema to count unique fields
		uniqueFields := 0
		if schema, err := bs.getSchemaFromTx(tx, listID); err == nil && schema != nil {
			uniqueFields = len(schema.Fields)
		}
		
		stats = &types.ListStats{
			ItemCount:     int64(bucketStats.KeyN),
			TotalSize:     totalSize,
			AvgItemSize:   0,
			UniqueFields:  uniqueFields,
			LastModified:  time.Now(), // TODO: Track actual modification time
			QueryCount:    0,          // TODO: Track query count
			LastQueried:   time.Now(),
			PopularFields: []string{}, // TODO: Track popular fields
			IndexCount:    0,          // TODO: Count indexes
		}
		
		if stats.ItemCount > 0 {
			stats.AvgItemSize = float64(stats.TotalSize) / float64(stats.ItemCount)
		}

		return nil
	})

	return stats, err
}

// GetSchema returns the schema for a list
func (bs *BoltStorage) GetSchema(ctx context.Context, listID string) (*types.Schema, error) {
	// Check cache first
	if schema, found := bs.schemaCache.Get(listID); found {
		return schema, nil
	}

	// Load from database
	var schema *types.Schema
	err := bs.db.View(func(tx *bbolt.Tx) error {
		var err error
		schema, err = bs.getSchemaFromTx(tx, listID)
		return err
	})

	if err != nil {
		return nil, err
	}

	// Cache the schema
	bs.schemaCache.Set(listID, schema)

	return schema, nil
}

// Close closes the database
func (bs *BoltStorage) Close() error {
	return bs.db.Close()
}
