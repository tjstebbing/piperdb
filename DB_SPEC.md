# PiperDB: High-Performance List Database

## Overview

PiperDB is a specialized database engine optimized for storing, querying, and transforming lists of heterogeneous data. It's designed as a pure list database with integrated pipe-based DSL execution, suitable for any application that needs high-performance list operations.

## Design Principles

1. **List-First**: Storage and indexing optimized for list operations
2. **Schema-Optional**: Implicit schema detection with optional explicit constraints
3. **Query-Integrated**: DSL execution built into the storage layer
4. **Performance-Critical**: Sub-10ms response times for simple queries
5. **Memory-Efficient**: Minimal overhead, smart caching strategies
6. **Application-Agnostic**: Pure database concerns only - no application metadata

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     PiperDB Engine                         │
├─────────────────────────────────────────────────────────────┤
│  DSL Parser & Executor                                      │
│  ├── Lexer/Parser (pipe syntax → AST)                       │
│  ├── Query Planner (optimization & execution plan)          │
│  ├── Filter Engine (field matching, text search)           │
│  ├── Transform Engine (map, group, aggregate)               │
│  └── Result Builder (JSON, streaming, pagination)          │
├─────────────────────────────────────────────────────────────┤
│  Schema Engine                                              │
│  ├── Schema Inference (automatic field detection)          │
│  ├── Type Detection (string, number, date, etc.)           │
│  ├── Index Recommendations (smart indexing)                │
│  └── Schema Evolution (versioning, migration)              │
├─────────────────────────────────────────────────────────────┤
│  Storage Engine                                             │
│  ├── List Store (B+tree indexed lists)                     │
│  ├── Item Store (fast item access & updates)               │
│  ├── Index Manager (field indexes, text search)            │
│  └── Transaction Manager (ACID, optimistic locking)        │
├─────────────────────────────────────────────────────────────┤
│  Persistence Layer                                          │
│  ├── Write-Ahead Log (WAL for durability)                  │
│  ├── Checkpointing (periodic state snapshots)              │
│  ├── Compression (list & item compression)                 │
│  └── Backup/Recovery (point-in-time recovery)              │
└─────────────────────────────────────────────────────────────┘
```

## Data Structures

### Core Types

```go
// List represents a pure list with only database concerns
type List struct {
    ID        string    `json:"id"`
    Schema    *Schema   `json:"schema,omitempty"`
    Stats     ListStats `json:"stats"`
    CreatedAt time.Time `json:"created_at"`
    UpdatedAt time.Time `json:"updated_at"`
    
    // Internal storage fields
    ItemCount  int64     `json:"-"`
    TotalSize  int64     `json:"-"`
    LastAccess time.Time `json:"-"`
}

// Individual list item with position and data
type ListItem struct {
    ID          string                 `json:"id"`
    ListID      string                 `json:"list_id"`
    Position    int64                  `json:"position"`
    Data        map[string]interface{} `json:"data"`
    CreatedAt   time.Time              `json:"created_at"`
    UpdatedAt   time.Time              `json:"updated_at"`
    
    // Internal fields
    Size        int32                  `json:"-"`
    Hash        uint64                 `json:"-"`  // For deduplication
}

// Automatically inferred schema
type Schema struct {
    Fields      map[string]*FieldDef   `json:"fields"`
    Version     int32                  `json:"version"`
    Inferred    bool                   `json:"inferred"`
    UpdatedAt   time.Time              `json:"updated_at"`
}

// Field definition with type information
type FieldDef struct {
    Type        FieldType              `json:"type"`
    Required    bool                   `json:"required"`
    Unique      bool                   `json:"unique"`
    Default     interface{}            `json:"default,omitempty"`
    Description string                 `json:"description,omitempty"`
    
    // Statistics for inference
    SeenInCount int64                  `json:"-"`
    TotalItems  int64                  `json:"-"`
    TypeHints   []string               `json:"type_hints,omitempty"`
    
    // Indexing hints
    Indexed     bool                   `json:"indexed"`
    IndexType   string                 `json:"index_type,omitempty"` // btree, hash, text
}

type FieldType int

const (
    FieldString FieldType = iota
    FieldNumber
    FieldBoolean
    FieldArray
    FieldObject
    FieldDate
    FieldURL
    FieldEmail
    FieldRichText
    FieldEnum
)
```

### Storage Layout

```go
// Physical storage organization
type StorageLayout struct {
    // List metadata stored in B+tree (ID -> List)
    ListIndex    *BTree
    
    // Items stored in separate B+tree (ListID+Position -> Item)
    ItemIndex    *BTree
    
    // Field indexes for fast filtering
    FieldIndexes map[string]*Index  // field_name -> index
    
    // Full-text search index
    TextIndex    *TextSearchIndex
    
    // Write-ahead log for durability
    WAL          *WriteAheadLog
}

// Index structure for field-based queries
type Index struct {
    Type        string                 // "btree", "hash", "text"
    FieldName   string
    ListID      string                 // Optional: per-list or global
    Data        interface{}            // Underlying index structure
    Stats       IndexStats
}

type IndexStats struct {
    Size        int64
    Entries     int64
    Selectivity float64               // Ratio of unique values
    LastUsed    time.Time
    HitCount    int64
}
```

## DSL Implementation

### Pipe AST Structure

```go
// Abstract Syntax Tree for pipe expressions
type PipeAST struct {
    Stages []PipeStage
}

type PipeStage interface {
    Execute(ctx *QueryContext, input *ResultSet) (*ResultSet, error)
    Optimize(*QueryPlanner) PipeStage
    EstimateCost(*QueryContext) float64
}

// Filter stage: @field:value, "search terms"
type FilterStage struct {
    Conditions []FilterCondition
}

type FilterCondition struct {
    Field     string        // Empty for full-text search
    Operator  FilterOp      // Equals, GreaterThan, Contains, etc.
    Value     interface{}
    Negate    bool         // For NOT conditions
}

type FilterOp int

const (
    OpEquals FilterOp = iota
    OpNotEquals
    OpGreaterThan
    OpLessThan
    OpGreaterEqual
    OpLessEqual
    OpContains
    OpStartsWith
    OpEndsWith
    OpMatches     // Regex
    OpIn          // Array membership
    OpRange       // [min max]
    OpExists      // Field exists
)

// Transform stage: map, select, pluck
type TransformStage struct {
    Type   TransformType
    Fields []FieldSpec
}

type FieldSpec struct {
    Source string      // Source field name
    Target string      // Target field name
    Expr   interface{} // Optional transformation expression
}

// Sort stage: sort field, sort -field
type SortStage struct {
    Fields []SortField
}

type SortField struct {
    Name string
    Desc bool
}

// Aggregate stage: count, group-by, sum, etc.
type AggregateStage struct {
    Type      AggregateType
    Field     string
    GroupBy   []string
}

type AggregateType int

const (
    AggCount AggregateType = iota
    AggSum
    AggAvg
    AggMin
    AggMax
    AggGroupBy
)
```

### Query Execution Engine

```go
// Query context with optimization hints
type QueryContext struct {
    ListID      string
    List        *List
    Schema      *Schema
    Indexes     map[string]*Index
    Stats       *QueryStats
    
    // Execution parameters
    Limit       int64
    Offset      int64
    Streaming   bool
    
    // Performance hints
    UseIndexes  bool
    MaxMemory   int64
    Timeout     time.Duration
}

// Result set with streaming support
type ResultSet struct {
    Items       []map[string]interface{}
    Schema      *Schema
    
    // Metadata
    TotalCount  int64
    HasMore     bool
    NextCursor  string
    
    // Performance info
    QueryTime   time.Duration
    IndexHits   int64
    MemoryUsed  int64
}

// Query planner for optimization
type QueryPlanner struct {
    Stats       *DatabaseStats
    Indexes     map[string]*Index
}

func (qp *QueryPlanner) Optimize(pipe *PipeAST, ctx *QueryContext) *ExecutionPlan {
    plan := &ExecutionPlan{}
    
    // Index selection for filters
    plan.FilterStrategy = qp.selectFilterStrategy(pipe, ctx)
    
    // Sort optimization (use index if possible)
    plan.SortStrategy = qp.selectSortStrategy(pipe, ctx)
    
    // Memory vs. streaming decision
    plan.ExecutionMode = qp.selectExecutionMode(pipe, ctx)
    
    return plan
}

type ExecutionPlan struct {
    FilterStrategy string  // "sequential", "index", "hybrid"
    SortStrategy   string  // "memory", "external", "index"
    ExecutionMode  string  // "memory", "streaming", "hybrid"
    
    EstimatedCost  float64
    EstimatedTime  time.Duration
    EstimatedMemory int64
}
```

## Storage Engine Details

### B+Tree Implementation

```go
// Specialized B+tree for list storage
type BTree struct {
    Root        *BTreeNode
    Order       int           // Branching factor
    KeyCompare  func(a, b interface{}) int
    
    // Statistics
    Height      int
    NodeCount   int64
    KeyCount    int64
}

type BTreeNode struct {
    IsLeaf      bool
    Keys        []interface{}
    Values      []interface{}  // Only for leaf nodes
    Children    []*BTreeNode   // Only for internal nodes
    Next        *BTreeNode     // Only for leaf nodes (for range scans)
    
    // Node metadata
    Size        int32
    Dirty       bool
    LastAccess  time.Time
}

// Optimized for list item access patterns
func (bt *BTree) RangeSearch(startKey, endKey interface{}) Iterator {
    // Find start position
    node := bt.findLeaf(startKey)
    return &BTreeIterator{
        tree:     bt,
        current:  node,
        position: bt.findPosition(node, startKey),
        endKey:   endKey,
    }
}
```

### Field Indexing

```go
// Smart indexing based on query patterns
type IndexManager struct {
    Indexes        map[string]*Index
    Stats          *IndexStats
    
    // Auto-indexing policy
    QueryThreshold int64           // Create index after N queries
    UsageWindow    time.Duration   // Track usage over time window
}

func (im *IndexManager) ShouldCreateIndex(field string, listID string) bool {
    usage := im.Stats.GetFieldUsage(field, listID)
    
    // Create index if:
    // 1. Field queried frequently
    // 2. High selectivity (many unique values)
    // 3. List is growing (benefit increases over time)
    
    return usage.QueryCount >= im.QueryThreshold &&
           usage.Selectivity > 0.1 &&
           usage.Growth > 0
}

// Adaptive index types based on data characteristics
func (im *IndexManager) SelectIndexType(field string, fieldDef *FieldDef) string {
    switch {
    case fieldDef.Type == FieldString && fieldDef.Unique:
        return "hash"  // Fast exact lookups
    case fieldDef.Type == FieldNumber || fieldDef.Type == FieldDate:
        return "btree" // Range queries
    case fieldDef.Type == FieldString && len(fieldDef.TypeHints) > 0:
        return "text"  // Full-text search
    default:
        return "btree" // General purpose
    }
}
```

### Text Search Integration

```go
// Full-text search for string fields
type TextSearchIndex struct {
    // Inverted index: token -> list of (item_id, positions)
    InvertedIndex map[string]*PostingList
    
    // Token analysis
    Tokenizer     *Tokenizer
    Stemmer       *Stemmer
    StopWords     map[string]bool
    
    // Performance
    MaxTokens     int
    MemoryLimit   int64
}

type PostingList struct {
    ItemIDs      []string
    Positions    [][]int  // Positions within each item
    Frequencies  []int    // Term frequency per item
}

func (tsi *TextSearchIndex) Search(query string) *SearchResult {
    tokens := tsi.Tokenizer.Tokenize(query)
    
    // Handle phrase queries, boolean operators, fuzzy matching
    return tsi.executeSearch(tokens)
}
```

## Schema Inference Engine

### Automatic Type Detection

```go
type SchemaInference struct {
    MinSampleSize    int          // Minimum items before inference
    ConfidenceLevel  float64      // Required confidence for type assignment
    TypeDetectors    []TypeDetector
}

type TypeDetector interface {
    DetectType(value interface{}) (FieldType, float64) // type, confidence
    ValidateType(value interface{}, fieldType FieldType) bool
}

// Built-in type detectors
type NumberDetector struct{}

func (nd *NumberDetector) DetectType(value interface{}) (FieldType, float64) {
    switch v := value.(type) {
    case int, int64, float64:
        return FieldNumber, 1.0
    case string:
        if _, err := strconv.ParseFloat(v, 64); err == nil {
            return FieldNumber, 0.8  // String that looks like number
        }
    }
    return FieldString, 0.0
}

type DateDetector struct {
    Formats []string // Common date formats to try
}

func (dd *DateDetector) DetectType(value interface{}) (FieldType, float64) {
    if s, ok := value.(string); ok {
        for _, format := range dd.Formats {
            if _, err := time.Parse(format, s); err == nil {
                return FieldDate, 0.9
            }
        }
    }
    return FieldString, 0.0
}

// Schema evolution when new items added
func (si *SchemaInference) UpdateSchema(schema *Schema, newItem map[string]interface{}) (*Schema, error) {
    updated := schema.Clone()
    updated.Version++
    
    for field, value := range newItem {
        if fieldDef, exists := updated.Fields[field]; exists {
            // Update existing field statistics
            fieldDef.SeenInCount++
            
            // Check if type is still consistent
            if !si.validateFieldType(value, fieldDef.Type) {
                // Type conflict - promote to more general type or mark as mixed
                fieldDef.Type = si.promoteType(fieldDef.Type, value)
            }
        } else {
            // New field - infer type
            fieldType, confidence := si.inferFieldType(value)
            updated.Fields[field] = &FieldDef{
                Type:        fieldType,
                SeenInCount: 1,
                TotalItems:  schema.getTotalItems(),
            }
        }
    }
    
    return updated, nil
}
```

## Performance Specifications

### Target Benchmarks

```yaml
Query Performance:
  Simple Filter: <10ms     # @price:<100
  Complex Pipe: <100ms     # Multiple stages, aggregation
  Full-Text Search: <50ms  # Text search with ranking
  
Storage Performance:
  Write Throughput: 10,000 items/sec
  Read Throughput: 100,000 items/sec
  Concurrent Users: 1,000+
  
Memory Usage:
  Overhead per List: <1KB
  Overhead per Item: <100 bytes
  Index Memory Ratio: <20% of data size
  
Durability:
  WAL Write Latency: <1ms
  Recovery Time: <10 seconds (1M items)
  Data Loss Window: <100ms
```

### Optimization Strategies

```go
// Query optimization techniques
type OptimizationStrategy int

const (
    OptPushdownFilters OptimizationStrategy = iota  // Push filters to storage layer
    OptIndexSelection                               // Choose optimal indexes
    OptSortElimination                             // Use index order for sorting
    OptProjectionPushdown                          // Only read needed fields
    OptLimitPushdown                               // Apply limits early
    OptCaching                                     // Cache frequent query results
)

// Memory management
type MemoryManager struct {
    CacheSize       int64
    ItemCache       *LRUCache
    IndexCache      *LRUCache
    QueryResultCache *LRUCache
}

// Connection pooling and concurrency
type ConnectionPool struct {
    MaxConnections  int
    IdleConnections chan *Connection
    ActiveConnections map[*Connection]bool
    Mutex          sync.RWMutex
}
```

## API Surface

### Core Database Interface

```go
// Pure list database interface - no application concerns
type PiperDB interface {
    // List management - operates on list IDs only
    CreateList(ctx context.Context, listID string) error
    DeleteList(ctx context.Context, listID string) error
    ListExists(ctx context.Context, listID string) (bool, error)
    GetListInfo(ctx context.Context, listID string) (*List, error)
    ListAllLists(ctx context.Context) ([]string, error)
    
    // Item management - items are pure data maps
    AddItem(ctx context.Context, listID string, data map[string]interface{}) (string, error)
    AddItems(ctx context.Context, listID string, items []map[string]interface{}) ([]string, error)
    UpdateItem(ctx context.Context, listID, itemID string, data map[string]interface{}) error
    DeleteItem(ctx context.Context, listID, itemID string) error
    GetItem(ctx context.Context, listID, itemID string) (map[string]interface{}, error)
    GetItems(ctx context.Context, listID string, opts *QueryOptions) (*ResultSet, error)
    
    // DSL pipe queries - the core feature
    ExecutePipe(ctx context.Context, listID, pipeExpr string, opts *QueryOptions) (*ResultSet, error)
    ValidatePipe(ctx context.Context, pipeExpr string) error
    ExplainPipe(ctx context.Context, listID, pipeExpr string) (*QueryPlan, error)
    
    // Schema operations - automatic and manual
    GetSchema(ctx context.Context, listID string) (*Schema, error)
    SetSchema(ctx context.Context, listID string, schema *Schema) error
    ResetSchema(ctx context.Context, listID string) error // Back to auto-inference
    
    // Performance and indexing
    CreateIndex(ctx context.Context, listID, field, indexType string) error
    DropIndex(ctx context.Context, listID, field string) error
    ListIndexes(ctx context.Context, listID string) ([]*IndexInfo, error)
    GetStats(ctx context.Context, listID string) (*ListStats, error)
    
    // Transaction support
    Begin(ctx context.Context) (Transaction, error)
    
    // Database administration
    GlobalStats(ctx context.Context) (*DatabaseStats, error)
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

// Database configuration - pure performance and storage settings
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
```

## Testing Strategy

### Unit Tests

```go
// Core functionality tests
func TestListCRUD(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()
    
    // Test list creation, reading, updating, deletion
    list := &List{
        ID:      "test-list",
        OwnerID: "user1",
        Title:   "Test List",
    }
    
    err := db.CreateList(context.Background(), list)
    assert.NoError(t, err)
    
    retrieved, err := db.GetList(context.Background(), list.ID)
    assert.NoError(t, err)
    assert.Equal(t, list.Title, retrieved.Title)
}

// DSL parsing and execution tests
func TestPipeExecution(t *testing.T) {
    db := setupTestDBWithData(t)
    
    tests := []struct {
        pipe     string
        expected int
    }{
        {"items | count", 100},
        {"items | @price:<50 | count", 25},
        {"items | sort -price | first", 1},
    }
    
    for _, test := range tests {
        result, err := db.ExecutePipe(context.Background(), "test-list", test.pipe, nil)
        assert.NoError(t, err)
        assert.Equal(t, test.expected, len(result.Items))
    }
}
```

### Performance Tests

```go
// Benchmark query performance
func BenchmarkSimpleFilter(b *testing.B) {
    db := setupBenchDB(b, 100000) // 100k items
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := db.ExecutePipe(context.Background(), "bench-list", "items | @category:electronics", nil)
        if err != nil {
            b.Fatal(err)
        }
    }
}

// Load testing with concurrent operations
func TestConcurrentQueries(t *testing.T) {
    db := setupTestDBWithData(t)
    
    var wg sync.WaitGroup
    errors := make(chan error, 100)
    
    // Run 100 concurrent queries
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            _, err := db.ExecutePipe(context.Background(), "test-list", "items | @id:exists | count", nil)
            if err != nil {
                errors <- err
            }
        }()
    }
    
    wg.Wait()
    close(errors)
    
    for err := range errors {
        t.Error(err)
    }
}
```

### Integration Tests

```go
// End-to-end workflow tests
func TestSchemaEvolution(t *testing.T) {
    db := setupTestDB(t)
    
    // Add items with evolving schema
    items := []map[string]interface{}{
        {"name": "Item 1"},
        {"name": "Item 2", "price": 99.99},
        {"name": "Item 3", "price": 149.99, "category": "electronics"},
    }
    
    for _, item := range items {
        err := db.AddItem(context.Background(), "test-list", &ListItem{
            Data: item,
        })
        assert.NoError(t, err)
    }
    
    // Verify schema was inferred correctly
    schema, err := db.GetSchema(context.Background(), "test-list")
    assert.NoError(t, err)
    assert.Equal(t, FieldString, schema.Fields["name"].Type)
    assert.Equal(t, FieldNumber, schema.Fields["price"].Type)
    assert.True(t, schema.Fields["name"].Required)
    assert.False(t, schema.Fields["price"].Required)
}
```

## Deployment & Operations

### Build Configuration

```go
// Build tags for different deployments
// +build production

package main

const (
    DefaultMaxMemory = 8 * 1024 * 1024 * 1024 // 8GB
    DefaultCacheSize = 1 * 1024 * 1024 * 1024 // 1GB
    WALEnabled      = true
    SyncWrites      = true
)
```

### Monitoring & Observability

```go
// Metrics collection
type Metrics struct {
    QueryCount        prometheus.Counter
    QueryDuration     prometheus.Histogram
    IndexHitRate      prometheus.Gauge
    MemoryUsage       prometheus.Gauge
    ActiveConnections prometheus.Gauge
    
    // Error tracking
    ErrorCount        prometheus.CounterVec
    PanicCount        prometheus.Counter
}

// Health check endpoint
func (db *PiperDB) HealthCheck() *HealthStatus {
    return &HealthStatus{
        Status:      "healthy",
        Version:     Version,
        Uptime:      time.Since(db.startTime),
        Connections: db.pool.ActiveCount(),
        Memory:      db.memoryManager.Usage(),
    }
}
```

## Migration & Backward Compatibility

### Schema Versioning

```go
type SchemaMigration struct {
    FromVersion int32
    ToVersion   int32
    Transform   func(*Schema) (*Schema, error)
}

// Built-in migrations for common schema changes
var builtinMigrations = []SchemaMigration{
    {
        FromVersion: 1,
        ToVersion:   2,
        Transform:   addTypeHintsToFields,
    },
    {
        FromVersion: 2,
        ToVersion:   3,
        Transform:   addIndexingMetadata,
    },
}
```

---

## Next Steps for Implementation

1. **Core Storage Engine** (Week 1-2)
   - Implement basic B+tree with list-optimized operations
   - Create simple key-value storage for lists and items
   - Add basic CRUD operations

2. **DSL Parser** (Week 2-3)
   - Build lexer for pipe syntax
   - Create AST structures and parser
   - Implement basic filter execution

3. **Schema Inference** (Week 3-4)
   - Type detection algorithms
   - Schema evolution logic
   - Field statistics tracking

4. **Query Optimization** (Week 4-5)
   - Query planner implementation
   - Index selection strategies
   - Performance benchmarking

5. **Full DSL Support** (Week 5-6)
   - All pipe stage implementations
   - Text search integration
   - Advanced aggregations

This specification provides a comprehensive blueprint for building PiperDB as a high-performance foundation for the Piper platform. The focus on lists, implicit schemas, and integrated DSL execution will enable the unique features that differentiate Piper from existing solutions.
