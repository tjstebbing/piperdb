package db

import (
	"context"
	"time"

	"github.com/tjstebbing/piperdb/pkg/types"
)

// PiperDB defines the pure list database interface
// No application concerns - only lists, items, and DSL queries
type PiperDB interface {
	// List management - operates on list IDs only
	CreateList(ctx context.Context, listID string) error
	DeleteList(ctx context.Context, listID string) error
	ListExists(ctx context.Context, listID string) (bool, error)
	GetListInfo(ctx context.Context, listID string) (*types.List, error)
	ListAllLists(ctx context.Context) ([]string, error)

	// Item management - items are pure data maps
	AddItem(ctx context.Context, listID string, data map[string]interface{}) (string, error)
	AddItems(ctx context.Context, listID string, items []map[string]interface{}) ([]string, error)
	UpdateItem(ctx context.Context, listID, itemID string, data map[string]interface{}) error
	DeleteItem(ctx context.Context, listID, itemID string) error
	GetItem(ctx context.Context, listID, itemID string) (map[string]interface{}, error)
	GetItems(ctx context.Context, listID string, opts *types.QueryOptions) (*types.ResultSet, error)

	// DSL pipe queries - the core feature
	ExecutePipe(ctx context.Context, listID, pipeExpr string, opts *types.QueryOptions) (*types.ResultSet, error)
	ValidatePipe(ctx context.Context, pipeExpr string) error
	ExplainPipe(ctx context.Context, listID, pipeExpr string) (*types.QueryPlan, error)

	// Schema operations - automatic and manual
	GetSchema(ctx context.Context, listID string) (*types.Schema, error)
	SetSchema(ctx context.Context, listID string, schema *types.Schema) error
	ResetSchema(ctx context.Context, listID string) error // Back to auto-inference

	// Performance and indexing
	CreateIndex(ctx context.Context, listID, field, indexType string) error
	DropIndex(ctx context.Context, listID, field string) error
	ListIndexes(ctx context.Context, listID string) ([]*types.IndexInfo, error)
	GetStats(ctx context.Context, listID string) (*types.ListStats, error)

	// Transaction support
	Begin(ctx context.Context) (Transaction, error)

	// Database administration
	GlobalStats(ctx context.Context) (*types.DatabaseStats, error)
	Optimize(ctx context.Context, listID string) error
	Compact(ctx context.Context, listID string) error
	Close() error
}

// Transaction interface for multi-operation consistency
type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	// All DB operations available within transaction
	PiperDB
}

// Note: Open function is implemented in database.go

// Config holds database configuration (pure performance and storage settings)
type Config struct {
	DataDir         string
	MaxMemory       int64
	WriteBufferSize int64

	// Performance tuning
	BTreeOrder      int
	IndexCacheSize  int64
	QueryTimeout    time.Duration
	MaxConcurrency  int

	// Durability settings
	SyncWrites         bool
	WALEnabled         bool
	CheckpointInterval time.Duration
	MaxWALSize         int64

	// Schema inference
	SchemaInference SchemaInferenceConfig

	// Auto-indexing
	AutoIndexing AutoIndexConfig

	// Administrative
	ReadOnly       bool
	MetricsEnabled bool
}

// SchemaInferenceConfig controls automatic schema detection
type SchemaInferenceConfig struct {
	Enabled          bool
	MinSampleSize    int
	ConfidenceLevel  float64
	AutoPromoteTypes bool
	MaxFields        int
}

// AutoIndexConfig controls automatic index creation
type AutoIndexConfig struct {
	Enabled            bool
	QueryThreshold     int64
	SelectivityMinimum float64
	UsageWindow        time.Duration
	MaxIndexesPerList  int
	IndexMemoryLimit   int64
	DropUnusedIndexes  bool
	UnusedThreshold    time.Duration
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		DataDir:            "./data",
		MaxMemory:          2 * 1024 * 1024 * 1024, // 2GB
		WriteBufferSize:    32 * 1024 * 1024,       // 32MB
		BTreeOrder:         64,
		IndexCacheSize:     128 * 1024 * 1024, // 128MB
		QueryTimeout:       30 * time.Second,
		MaxConcurrency:     100,
		SyncWrites:         true,
		WALEnabled:         true,
		CheckpointInterval: 5 * time.Minute,
		MaxWALSize:         128 * 1024 * 1024, // 128MB

		SchemaInference: SchemaInferenceConfig{
			Enabled:          true,
			MinSampleSize:    3,
			ConfidenceLevel:  0.8,
			AutoPromoteTypes: true,
			MaxFields:        1000,
		},

		AutoIndexing: AutoIndexConfig{
			Enabled:            true,
			QueryThreshold:     10,
			SelectivityMinimum: 0.1,
			UsageWindow:        24 * time.Hour,
			MaxIndexesPerList:  10,
			IndexMemoryLimit:   256 * 1024 * 1024, // 256MB
			DropUnusedIndexes:  true,
			UnusedThreshold:    7 * 24 * time.Hour, // 1 week
		},

		ReadOnly:       false,
		MetricsEnabled: true,
	}
}
