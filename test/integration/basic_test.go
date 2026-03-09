package integration

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tjstebbing/piperdb/pkg/db"
)

func TestBasicOperations(t *testing.T) {
	// Create temporary directory for test database
	tempDir, err := os.MkdirTemp("", "piperdb_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Open database
	cfg := db.DefaultConfig()
	cfg.DataDir = tempDir

	database, err := db.Open(cfg)
	require.NoError(t, err)
	defer database.Close()

	ctx := context.Background()

	t.Run("CreateList", func(t *testing.T) {
		err := database.CreateList(ctx, "test-list")
		assert.NoError(t, err)

		// Verify list exists
		exists, err := database.ListExists(ctx, "test-list")
		assert.NoError(t, err)
		assert.True(t, exists)

		// Try to create same list again (should fail)
		err = database.CreateList(ctx, "test-list")
		assert.Error(t, err)
	})

	t.Run("ListAllLists", func(t *testing.T) {
		lists, err := database.ListAllLists(ctx)
		assert.NoError(t, err)
		assert.Contains(t, lists, "test-list")
	})

	t.Run("AddItems", func(t *testing.T) {
		// Add first item
		itemID1, err := database.AddItem(ctx, "test-list", map[string]interface{}{
			"name":  "iPhone",
			"price": 999,
			"brand": "Apple",
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, itemID1)

		// Add second item with different fields
		itemID2, err := database.AddItem(ctx, "test-list", map[string]interface{}{
			"name":     "MacBook",
			"price":    2499,
			"brand":    "Apple",
			"category": "laptop",
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, itemID2)
		assert.NotEqual(t, itemID1, itemID2)
	})

	t.Run("GetItems", func(t *testing.T) {
		result, err := database.GetItems(ctx, "test-list", nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), result.TotalCount)
		assert.Len(t, result.Items, 2)

		// BoltDB iterates keys in byte-sorted order (UUIDs), not insertion order.
		// Find items by name instead of assuming order.
		itemsByName := make(map[string]map[string]interface{})
		for _, item := range result.Items {
			itemsByName[item["name"].(string)] = item
		}

		iphone := itemsByName["iPhone"]
		require.NotNil(t, iphone)
		assert.Equal(t, float64(999), iphone["price"])
		assert.Equal(t, "Apple", iphone["brand"])

		macbook := itemsByName["MacBook"]
		require.NotNil(t, macbook)
		assert.Equal(t, float64(2499), macbook["price"])
		assert.Equal(t, "laptop", macbook["category"])
	})

	t.Run("GetSchema", func(t *testing.T) {
		schema, err := database.GetSchema(ctx, "test-list")
		assert.NoError(t, err)
		assert.NotNil(t, schema)
		assert.True(t, schema.Inferred)
		assert.True(t, schema.Version >= 1)

		// Should have inferred fields
		assert.Contains(t, schema.Fields, "name")
		assert.Contains(t, schema.Fields, "price")
		assert.Contains(t, schema.Fields, "brand")
		assert.Contains(t, schema.Fields, "category")

		// Check field types
		assert.Equal(t, "string", schema.Fields["name"].Type.String())
		assert.Equal(t, "number", schema.Fields["price"].Type.String())
		assert.Equal(t, "string", schema.Fields["brand"].Type.String())
		assert.Equal(t, "string", schema.Fields["category"].Type.String())

		// Name and brand should be seen in all items
		assert.Equal(t, int64(2), schema.Fields["name"].SeenInCount)
		assert.Equal(t, int64(2), schema.Fields["brand"].SeenInCount)

		// Category should only be seen in one item
		assert.Equal(t, int64(1), schema.Fields["category"].SeenInCount)
	})

	t.Run("GetStats", func(t *testing.T) {
		stats, err := database.GetStats(ctx, "test-list")
		assert.NoError(t, err)
		assert.Equal(t, int64(2), stats.ItemCount)
		assert.Greater(t, stats.TotalSize, int64(0))
	})

	t.Run("DeleteList", func(t *testing.T) {
		err := database.DeleteList(ctx, "test-list")
		assert.NoError(t, err)

		// Verify list no longer exists
		exists, err := database.ListExists(ctx, "test-list")
		assert.NoError(t, err)
		assert.False(t, exists)

		// Should not be in list of all lists
		lists, err := database.ListAllLists(ctx)
		assert.NoError(t, err)
		assert.NotContains(t, lists, "test-list")
	})
}

func TestItemOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "piperdb_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := db.DefaultConfig()
	cfg.DataDir = tempDir

	database, err := db.Open(cfg)
	require.NoError(t, err)
	defer database.Close()

	ctx := context.Background()

	// Create list
	err = database.CreateList(ctx, "items-test")
	require.NoError(t, err)

	t.Run("AddAndGetItem", func(t *testing.T) {
		data := map[string]interface{}{
			"title":       "Test Item",
			"description": "A test item",
			"count":       42,
		}

		itemID, err := database.AddItem(ctx, "items-test", data)
		assert.NoError(t, err)

		// Get the item back
		retrievedData, err := database.GetItem(ctx, "items-test", itemID)
		assert.NoError(t, err)
		assert.Equal(t, "Test Item", retrievedData["title"])
		assert.Equal(t, "A test item", retrievedData["description"])
		assert.Equal(t, float64(42), retrievedData["count"])
	})

	t.Run("UpdateItem", func(t *testing.T) {
		// Add an item first
		originalData := map[string]interface{}{
			"name":   "Original",
			"status": "draft",
		}

		itemID, err := database.AddItem(ctx, "items-test", originalData)
		require.NoError(t, err)

		// Update the item
		updatedData := map[string]interface{}{
			"name":   "Updated",
			"status": "published",
			"tags":   []string{"new", "updated"},
		}

		err = database.UpdateItem(ctx, "items-test", itemID, updatedData)
		assert.NoError(t, err)

		// Get updated item
		retrievedData, err := database.GetItem(ctx, "items-test", itemID)
		assert.NoError(t, err)
		assert.Equal(t, "Updated", retrievedData["name"])
		assert.Equal(t, "published", retrievedData["status"])
		assert.Contains(t, retrievedData["tags"], "new")
	})

	t.Run("DeleteItem", func(t *testing.T) {
		// Add an item first
		data := map[string]interface{}{"temp": "item"}
		itemID, err := database.AddItem(ctx, "items-test", data)
		require.NoError(t, err)

		// Delete the item
		err = database.DeleteItem(ctx, "items-test", itemID)
		assert.NoError(t, err)

		// Try to get deleted item (should fail)
		_, err = database.GetItem(ctx, "items-test", itemID)
		assert.Error(t, err)
	})
}

func TestSchemaInference(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "piperdb_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := db.DefaultConfig()
	cfg.DataDir = tempDir

	database, err := db.Open(cfg)
	require.NoError(t, err)
	defer database.Close()

	ctx := context.Background()

	err = database.CreateList(ctx, "schema-test")
	require.NoError(t, err)

	t.Run("TypeInference", func(t *testing.T) {
		// Add items with different types
		items := []map[string]interface{}{
			{
				"string_field": "hello",
				"number_field": 123,
				"bool_field":   true,
				"array_field":  []interface{}{"a", "b", "c"},
				"object_field": map[string]interface{}{"nested": "value"},
			},
			{
				"string_field": "world",
				"number_field": 456.78,
				"bool_field":   false,
				"array_field":  []interface{}{1, 2, 3},
			},
		}

		for _, item := range items {
			_, err := database.AddItem(ctx, "schema-test", item)
			require.NoError(t, err)
		}

		// Check inferred schema
		schema, err := database.GetSchema(ctx, "schema-test")
		require.NoError(t, err)

		assert.Equal(t, "string", schema.Fields["string_field"].Type.String())
		assert.Equal(t, "number", schema.Fields["number_field"].Type.String())
		assert.Equal(t, "boolean", schema.Fields["bool_field"].Type.String())
		assert.Equal(t, "array", schema.Fields["array_field"].Type.String())
		assert.Equal(t, "object", schema.Fields["object_field"].Type.String())

		// Check field occurrence counts
		assert.Equal(t, int64(2), schema.Fields["string_field"].SeenInCount)
		assert.Equal(t, int64(2), schema.Fields["number_field"].SeenInCount)
		assert.Equal(t, int64(1), schema.Fields["object_field"].SeenInCount) // Only in first item
	})
}

func BenchmarkBasicOperations(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "piperdb_bench_*")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)

	cfg := db.DefaultConfig()
	cfg.DataDir = tempDir

	database, err := db.Open(cfg)
	require.NoError(b, err)
	defer database.Close()

	ctx := context.Background()

	err = database.CreateList(ctx, "bench-list")
	require.NoError(b, err)

	b.Run("AddItem", func(b *testing.B) {
		data := map[string]interface{}{
			"name":  "Benchmark Item",
			"value": 42,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := database.AddItem(ctx, "bench-list", data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
