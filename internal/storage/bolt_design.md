# BoltDB Integration Design

## Storage Layout in BoltDB

```
PiperDB (root)
├── _meta                          // Database metadata
│   ├── version → "1.0.0"
│   ├── created → timestamp
│   └── stats → global stats JSON
├── _lists                         // List registry
│   ├── list-123 → List JSON       // List metadata
│   ├── list-456 → List JSON
│   └── ...
├── list-123-items                 // Items for list-123
│   ├── item-001 → JSON data       // {name: "iPhone", price: 999}
│   ├── item-002 → JSON data       // {name: "MacBook", price: 2499}
│   └── ...
├── list-123-schema                // Schema for list-123
│   └── current → Schema JSON
├── list-123-indexes               // Indexes for list-123
│   ├── @price → BTree data        // Price field index
│   ├── @name → BTree data         // Name field index
│   └── _meta → index metadata
└── list-456-items                 // Items for list-456
    └── ...
```

## Key Design Decisions

### 1. Bucket Structure
- **One bucket per list** for items: `list-{listID}-items`
- **Separate buckets** for schemas and indexes
- **Global buckets** for metadata and list registry

### 2. Key Format
- **Items**: `item-{UUID}` → ensures unique, sortable keys
- **Schema**: `current` → single schema per list
- **Indexes**: `@{fieldname}` → matches DSL syntax

### 3. Value Format
- **Items**: Raw JSON (`map[string]interface{}`)
- **Schema**: Schema struct as JSON
- **Indexes**: Custom binary format for performance

### 4. Concurrency Model
- **Single writer** (BoltDB constraint)
- **Multiple readers** via read-only transactions
- **Connection pool** for read transactions

## Performance Optimizations

### 1. Item Storage
```go
// Store items with position for ordering
type StoredItem struct {
    Position int64                  `json:"position"`
    Data     map[string]interface{} `json:"data"`
    Hash     uint64                 `json:"hash"`     // For dedup
}
```

### 2. Index Strategy
- **Lazy indexing**: Create indexes when queries demand them
- **Composite keys**: `{value}#{itemID}` for handling duplicates
- **Index metadata**: Track usage stats for index management

### 3. Schema Caching
- **In-memory cache** of schemas (small, frequently accessed)
- **Version tracking** for cache invalidation
- **Automatic inference** on writes

### 4. Query Optimization
- **Index selection**: Choose best index for filter predicates
- **Scan optimization**: Efficient iteration over BoltDB cursors
- **Result streaming**: Don't load all results into memory

## Implementation Phases

### Phase 1: Core Storage
- [x] BoltDB setup and bucket management
- [x] Basic list CRUD operations
- [x] Item storage and retrieval
- [ ] Schema storage and basic inference

### Phase 2: Indexing
- [ ] Index creation and management
- [ ] Query optimization with indexes
- [ ] Index usage statistics

### Phase 3: DSL Integration
- [ ] Pipe parser integration
- [ ] Filter execution with indexes
- [ ] Transform and aggregation stages
