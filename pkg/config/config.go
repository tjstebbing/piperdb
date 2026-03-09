package config

import (
	"time"
)

// Config holds all configuration options for PiperDB
type Config struct {
	// Basic settings
	DataDir string `yaml:"data_dir" json:"data_dir"`
	
	// Memory and performance settings
	MaxMemory       int64 `yaml:"max_memory" json:"max_memory"`             // Maximum memory usage in bytes
	CacheSize       int64 `yaml:"cache_size" json:"cache_size"`             // Cache size in bytes
	WriteBufferSize int64 `yaml:"write_buffer_size" json:"write_buffer_size"` // Write buffer size
	
	// B-tree tuning
	BTreeOrder int `yaml:"btree_order" json:"btree_order"` // Branching factor for B+ trees
	
	// Query performance
	QueryTimeout    time.Duration `yaml:"query_timeout" json:"query_timeout"`
	IndexCacheSize  int64         `yaml:"index_cache_size" json:"index_cache_size"`
	MaxConcurrency  int           `yaml:"max_concurrency" json:"max_concurrency"`
	
	// Durability settings
	SyncWrites          bool          `yaml:"sync_writes" json:"sync_writes"`
	WALEnabled          bool          `yaml:"wal_enabled" json:"wal_enabled"`
	CheckpointInterval  time.Duration `yaml:"checkpoint_interval" json:"checkpoint_interval"`
	MaxWALSize          int64         `yaml:"max_wal_size" json:"max_wal_size"`
	
	// Connection and concurrency
	MaxConnections     int           `yaml:"max_connections" json:"max_connections"`
	ConnectionTimeout  time.Duration `yaml:"connection_timeout" json:"connection_timeout"`
	IdleTimeout        time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	ReadOnly           bool          `yaml:"read_only" json:"read_only"`
	
	// Schema inference settings
	SchemaInference SchemaInferenceConfig `yaml:"schema_inference" json:"schema_inference"`
	
	// Indexing settings
	Indexing IndexingConfig `yaml:"indexing" json:"indexing"`
	
	// Monitoring and debugging
	MetricsEnabled bool   `yaml:"metrics_enabled" json:"metrics_enabled"`
	LogLevel       string `yaml:"log_level" json:"log_level"`
	ProfileEnabled bool   `yaml:"profile_enabled" json:"profile_enabled"`
	
	// Advanced settings
	CompressionEnabled bool   `yaml:"compression_enabled" json:"compression_enabled"`
	CompressionLevel   int    `yaml:"compression_level" json:"compression_level"`
	EncryptionKey      string `yaml:"encryption_key" json:"encryption_key,omitempty"`
}

// SchemaInferenceConfig controls automatic schema detection
type SchemaInferenceConfig struct {
	Enabled          bool    `yaml:"enabled" json:"enabled"`
	MinSampleSize    int     `yaml:"min_sample_size" json:"min_sample_size"`       // Min items before inferring schema
	ConfidenceLevel  float64 `yaml:"confidence_level" json:"confidence_level"`     // Required confidence for type assignment
	AutoPromoteTypes bool    `yaml:"auto_promote_types" json:"auto_promote_types"` // Automatically promote conflicting types
	MaxFields        int     `yaml:"max_fields" json:"max_fields"`                 // Maximum fields to track per schema
}

// IndexingConfig controls automatic index creation
type IndexingConfig struct {
	AutoIndexEnabled    bool          `yaml:"auto_index_enabled" json:"auto_index_enabled"`
	QueryThreshold      int64         `yaml:"query_threshold" json:"query_threshold"`         // Create index after N queries
	SelectivityMinimum  float64       `yaml:"selectivity_minimum" json:"selectivity_minimum"` // Minimum selectivity for indexing
	UsageWindow         time.Duration `yaml:"usage_window" json:"usage_window"`               // Track usage over this window
	MaxIndexesPerList   int           `yaml:"max_indexes_per_list" json:"max_indexes_per_list"`
	IndexMemoryLimit    int64         `yaml:"index_memory_limit" json:"index_memory_limit"`
	DropUnusedIndexes   bool          `yaml:"drop_unused_indexes" json:"drop_unused_indexes"`
	UnusedThreshold     time.Duration `yaml:"unused_threshold" json:"unused_threshold"`
}

// Default returns a configuration with sensible defaults
func Default() *Config {
	return &Config{
		DataDir:            "./data",
		MaxMemory:          2 * 1024 * 1024 * 1024, // 2GB
		CacheSize:          256 * 1024 * 1024,      // 256MB
		WriteBufferSize:    32 * 1024 * 1024,       // 32MB
		BTreeOrder:         64,
		QueryTimeout:       30 * time.Second,
		IndexCacheSize:     64 * 1024 * 1024, // 64MB
		MaxConcurrency:     100,
		
		SyncWrites:         true,
		WALEnabled:         true,
		CheckpointInterval: 5 * time.Minute,
		MaxWALSize:         128 * 1024 * 1024, // 128MB
		
		MaxConnections:    1000,
		ConnectionTimeout: 30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		ReadOnly:          false,
		
		SchemaInference: SchemaInferenceConfig{
			Enabled:          true,
			MinSampleSize:    3,
			ConfidenceLevel:  0.8,
			AutoPromoteTypes: true,
			MaxFields:        1000,
		},
		
		Indexing: IndexingConfig{
			AutoIndexEnabled:   true,
			QueryThreshold:     10,
			SelectivityMinimum: 0.1,
			UsageWindow:        24 * time.Hour,
			MaxIndexesPerList:  10,
			IndexMemoryLimit:   128 * 1024 * 1024, // 128MB
			DropUnusedIndexes:  true,
			UnusedThreshold:    7 * 24 * time.Hour, // 1 week
		},
		
		MetricsEnabled:     true,
		LogLevel:           "info",
		ProfileEnabled:     false,
		CompressionEnabled: true,
		CompressionLevel:   1,
	}
}

// Development returns a configuration optimized for development
func Development() *Config {
	cfg := Default()
	cfg.MaxMemory = 512 * 1024 * 1024 // 512MB
	cfg.CacheSize = 64 * 1024 * 1024  // 64MB
	cfg.SyncWrites = false            // Faster writes for dev
	cfg.LogLevel = "debug"
	cfg.ProfileEnabled = true
	cfg.CheckpointInterval = 1 * time.Minute
	return cfg
}

// Production returns a configuration optimized for production
func Production() *Config {
	cfg := Default()
	cfg.MaxMemory = 8 * 1024 * 1024 * 1024 // 8GB
	cfg.CacheSize = 2 * 1024 * 1024 * 1024 // 2GB
	cfg.SyncWrites = true
	cfg.LogLevel = "warn"
	cfg.ProfileEnabled = false
	cfg.CompressionEnabled = true
	cfg.CompressionLevel = 3
	cfg.MaxConnections = 5000
	cfg.MaxConcurrency = 500
	return cfg
}

// Testing returns a configuration optimized for tests
func Testing() *Config {
	cfg := Default()
	cfg.DataDir = ":memory:"           // In-memory for tests
	cfg.MaxMemory = 128 * 1024 * 1024  // 128MB
	cfg.CacheSize = 32 * 1024 * 1024   // 32MB
	cfg.SyncWrites = false
	cfg.WALEnabled = false
	cfg.LogLevel = "error"
	cfg.MetricsEnabled = false
	cfg.CheckpointInterval = 0 // Disable checkpointing
	return cfg
}

// Validate checks if the configuration is valid and sets defaults for missing values
func (c *Config) Validate() error {
	if c.DataDir == "" {
		c.DataDir = "./data"
	}
	
	if c.MaxMemory <= 0 {
		c.MaxMemory = 2 * 1024 * 1024 * 1024 // 2GB
	}
	
	if c.CacheSize <= 0 {
		c.CacheSize = c.MaxMemory / 8 // 1/8 of max memory
	}
	
	if c.CacheSize > c.MaxMemory/2 {
		c.CacheSize = c.MaxMemory / 2 // No more than half of max memory
	}
	
	if c.WriteBufferSize <= 0 {
		c.WriteBufferSize = 32 * 1024 * 1024 // 32MB
	}
	
	if c.BTreeOrder <= 0 {
		c.BTreeOrder = 64
	}
	
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 30 * time.Second
	}
	
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 100
	}
	
	if c.MaxConnections <= 0 {
		c.MaxConnections = 1000
	}
	
	if c.SchemaInference.MinSampleSize <= 0 {
		c.SchemaInference.MinSampleSize = 3
	}
	
	if c.SchemaInference.ConfidenceLevel <= 0 || c.SchemaInference.ConfidenceLevel > 1 {
		c.SchemaInference.ConfidenceLevel = 0.8
	}
	
	if c.Indexing.QueryThreshold <= 0 {
		c.Indexing.QueryThreshold = 10
	}
	
	if c.Indexing.SelectivityMinimum < 0 || c.Indexing.SelectivityMinimum > 1 {
		c.Indexing.SelectivityMinimum = 0.1
	}
	
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	
	return nil
}

// MemoryLimits returns calculated memory limits for different components
func (c *Config) MemoryLimits() *MemoryLimits {
	return &MemoryLimits{
		Total:       c.MaxMemory,
		Cache:       c.CacheSize,
		WriteBuffer: c.WriteBufferSize,
		Indexes:     c.Indexing.IndexMemoryLimit,
		Query:       c.MaxMemory / 10, // 10% for query execution
		System:      c.MaxMemory / 20, // 5% for system overhead
	}
}

// MemoryLimits holds calculated memory limits for different components
type MemoryLimits struct {
	Total       int64 `json:"total"`
	Cache       int64 `json:"cache"`
	WriteBuffer int64 `json:"write_buffer"`
	Indexes     int64 `json:"indexes"`
	Query       int64 `json:"query"`
	System      int64 `json:"system"`
}
