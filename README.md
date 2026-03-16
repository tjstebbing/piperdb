# PiperDB

🚀 **High-Performance List Database with Expressive Query Language**

PiperDB is a specialized database engine optimized for storing and querying lists of heterogeneous data. It features a powerful pipe-based DSL inspired by jq, automatic schema inference, and sub-millisecond query performance.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Version](https://img.shields.io/badge/go-1.21+-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)

## ✨ Features

- 📋 **List-First Design**: Optimized storage for heterogeneous list data
- 🔍 **Expressive DSL**: jq-inspired pipe-based query language
- 🧠 **Smart Schema**: Automatic schema detection and evolution
- ⚡ **High Performance**: Sub-millisecond query execution
- 🛡️ **ACID Compliance**: Built on BoltDB with full transaction support
- 📦 **Zero Dependencies**: Pure Go implementation, no external services
- 🎯 **Type Safety**: Automatic type inference with manual overrides

## 🚀 Quick Start

### Installation

```bash
git clone https://github.com/tjstebbing/piperdb
cd piperdb
go build ./cmd/piperdb
```

### Basic Usage

```bash
# Create a list and add some data
./piperdb create-list products
./piperdb add-item products '{"name":"iPhone","price":999,"brand":"Apple","category":"phone"}'
./piperdb add-item products '{"name":"MacBook","price":2499,"brand":"Apple","category":"laptop"}'
./piperdb add-item products '{"name":"Pixel","price":799,"brand":"Google","category":"phone"}'

# Query with the DSL
./piperdb query products '@price<1000 | sort price'
./piperdb query products '@category=phone | select name price'
./piperdb query products '@brand=Apple | count'
```

## 📖 DSL Syntax Reference

PiperDB queries are built from **stages** separated by pipes (`|`). Data flows left to right through each stage.

```
<stage> | <stage> | <stage> ...
```

### Filter Expressions

Filters select items by field value. The `@` symbol marks the start of a field filter, followed by the field name, an operator, and a value:

```
@<field><operator><value>
```

| Syntax | Meaning | Example |
|--------|---------|---------|
| `@field=value` | Equals | `@brand=Apple` |
| `@field>value` | Greater than | `@price>100` |
| `@field<value` | Less than | `@price<1000` |
| `@field>=value` | Greater than or equal | `@rating>=4.5` |
| `@field<=value` | Less than or equal | `@stock<=10` |
| `@field!=value` | Not equal | `@status!=draft` |
| `@field~pattern` | Regex/fuzzy match | `@name~iPhone` |
| `@field^prefix` | Starts with | `@name^Mac` |
| `@field$suffix` | Ends with | `@sku$Pro` |
| `"text"` | Full-text search (all string fields) | `"wireless"` |

`=` is the **equals operator**. The comparison operators (`>`, `<`, `>=`, `<=`, `!=`) are standard and self-evident. The pattern operators (`~`, `^`, `$`) work the same way.

Multiple filters in the same stage are combined with implicit AND:

```bash
@category=phone @price<500          # both conditions must match
```

### Stage Keywords

| Stage | Syntax | Purpose |
|-------|--------|---------|
| `sort` | `sort field -field` | Sort ascending; prefix `-` for descending |
| `select` | `select field1 field2` | Keep only named fields |
| `map` | `map {field, old: new}` | Reshape and rename fields |
| `pluck` | `pluck field` | Extract a single field |
| `take` | `take N` | Limit to first N results |
| `skip` | `skip N` | Skip first N results |
| `first` | `first` | First item only |
| `last` | `last` | Last item only |
| `count` | `count` | Count items |
| `sum` | `sum field` | Sum numeric field |
| `avg` | `avg field` | Average of numeric field |
| `min` | `min field` | Minimum value |
| `max` | `max field` | Maximum value |
| `group-by` | `group-by field` | Group items by field value |

### Field Paths

Field names are identifiers that may contain letters, digits, underscores, and hyphens (e.g. `price`, `first_name`, `item-count`). They are case-sensitive and correspond to keys in the stored JSON data.

PiperDB supports **nested field access** using dot notation and bracket syntax for arrays:

| Syntax | Meaning | Example |
|--------|---------|---------|
| `field` | Top-level field | `@price>100` |
| `field.nested` | Nested object field | `@user.profile.name=Alice` |
| `field[N]` | Array element by index | `@tags[0]=golang` |
| `field[]` | Any array element (wildcard) | `@tags[]=golang` |
| `field[].nested` | Field within array elements | `@items[].price>100` |

Nested paths work everywhere — filters, sort, select, map, and aggregations:

```bash
@user.address.city=Sydney            # filter by nested field
@items[].status=shipped              # any item's status is shipped
sort -user.score                     # sort by nested field
select user.name user.email          # extract nested fields
avg items[].price                    # aggregate across array elements
map {user.profile.name: username}    # rename nested field in output
```

---

## 🎯 DSL Query Examples

PiperDB's query language is designed to be intuitive yet powerful. Here are comprehensive examples:

### 🔍 **Filtering**

```bash
# Field-based filtering
./piperdb query products '@price<1000'               # Price less than 1000
./piperdb query products '@price>=500'               # Price 500 or more  
./piperdb query products '@brand=Apple'              # Exact match
./piperdb query products '@category=phone'           # Category equals phone

# Range filtering
./piperdb query products '@price>100 @price<500'     # Price between 100-500
./piperdb query products '@rating>=4'                # High rated items

# Text pattern matching
./piperdb query products '@name~iPhone'             # Name contains iPhone (regex)
./piperdb query products '@name^Mac'                # Name starts with Mac
./piperdb query products '@description$Pro'         # Description ends with Pro
```

### 🔤 **Text Search**

```bash
# Full-text search across all fields
./piperdb query products '"Apple smartphone"'       # Search for Apple smartphone
./piperdb query blog-posts '"golang tutorial"'     # Find golang tutorials
./piperdb query documents '"machine learning"'     # ML-related documents

# Combined text search and filtering
./piperdb query products '"wireless" @price<200'    # Wireless items under $200
./piperdb query articles '"AI" @status=published'   # Published AI articles
```

### 🔄 **Transformations**

```bash
# Select specific fields only
./piperdb query products 'select name price'        # Only name and price
./piperdb query users 'select email username role'  # User essentials

# Map fields with renaming  
./piperdb query products 'map {name, price: cost, brand: manufacturer}'
./piperdb query events 'map {title, date: when, location: where}'

# Extract single field values
./piperdb query products 'pluck name'               # Just the names
./piperdb query users 'pluck email'                 # Email list
```

### 📊 **Sorting & Ordering**

```bash
# Single field sorting
./piperdb query products 'sort price'               # Ascending by price
./piperdb query products 'sort -price'              # Descending by price
./piperdb query articles 'sort -date'               # Newest first

# Multi-field sorting
./piperdb query products 'sort category -price'     # Category asc, price desc
./piperdb query students 'sort grade -score'        # Grade then score
./piperdb query events 'sort date -priority'        # Date then priority
```

### 🔢 **Aggregation & Analytics**

```bash
# Basic counting
./piperdb query products 'count'                    # Total product count
./piperdb query orders '@status=completed | count'   # Completed orders

# Numeric aggregations
./piperdb query products 'sum price'                # Total inventory value
./piperdb query orders 'avg total'                  # Average order value
./piperdb query products 'min price'                # Cheapest product
./piperdb query products 'max rating'               # Best rated product

# Grouping and analysis
./piperdb query products 'group-by category'        # Products by category
./piperdb query orders 'group-by customer'          # Orders by customer
./piperdb query sales 'group-by region month'       # Sales by region and month
```

### ✂️ **Slicing & Pagination**

```bash
# Limiting results
./piperdb query products 'sort -price | take 5'     # Top 5 most expensive
./piperdb query articles 'sort -date | take 10'     # 10 newest articles

# Pagination
./piperdb query products 'skip 20 | take 10'        # Page 3 (items 21-30)
./piperdb query users 'sort username | skip 100 | take 25'  # Page 5

# First/last items
./piperdb query products 'sort price | first'       # Cheapest product
./piperdb query events 'sort -date | last'          # Oldest event
```

### 🔗 **Complex Pipelines**

```bash
# E-commerce analytics
./piperdb query products '
  @category=electronics | 
  @price>100 @rating>=4 | 
  sort -rating -price | 
  take 10 | 
  select name price rating brand
'

# Content management
./piperdb query articles '
  @status=published | 
  "tutorial" | 
  sort -views | 
  map {title, author, views, url} | 
  take 20
'

# Inventory analysis  
./piperdb query inventory '
  @stock<10 | 
  @category=critical | 
  sort category -priority | 
  select sku name stock category supplier
'

# User engagement
./piperdb query users '
  @last_login>2024-01-01 | 
  @plan=premium | 
  sort -activity_score | 
  map {username, email, score: activity_score}
'
```

### 📈 **Real-World Use Cases**

#### 🏪 **E-commerce Product Catalog**
```bash
# Create and populate product catalog
./piperdb create-list products
./piperdb add-item products '{
  "name": "iPhone 15 Pro", "price": 999, "brand": "Apple", 
  "category": "smartphone", "rating": 4.8, "stock": 50,
  "features": ["5G", "ProRAW", "Titanium"], "release_date": "2023-09-22"
}'

# Business queries
./piperdb query products '@price<500 @rating>=4 | sort -rating | take 10'
./piperdb query products '@category=smartphone | group-by brand'  
./piperdb query products '@stock<10 | select name stock supplier'
./piperdb query products '"wireless" @category=accessories | sort price'
```

#### 📰 **Content Management System**
```bash
# Create blog/article system
./piperdb create-list articles
./piperdb add-item articles '{
  "title": "Getting Started with Go", "author": "John Doe",
  "status": "published", "tags": ["golang", "tutorial", "backend"],
  "views": 1542, "published_date": "2024-01-15", "reading_time": 8
}'

# Editorial queries
./piperdb query articles '@status=draft | sort -created_date'
./piperdb query articles '"golang" @status=published | sort -views | take 5'  
./piperdb query articles 'group-by author | map {author: .key, count: count()}'
./piperdb query articles '@published_date>2024-01-01 | avg views'
```

#### 👥 **User Management & Analytics**
```bash
# User data analysis
./piperdb create-list users
./piperdb add-item users '{
  "username": "alice_dev", "email": "alice@example.com",
  "plan": "premium", "signup_date": "2023-06-15", 
  "last_active": "2024-01-20", "total_projects": 12
}'

# Admin and analytics queries
./piperdb query users '@plan=premium @last_active>2024-01-01 | count'
./piperdb query users '@total_projects>5 | sort -last_active | select username email plan'
./piperdb query users 'group-by plan | map {plan: .key, users: count(), avg_projects: avg(.value.total_projects)}'
```

#### 📊 **Business Intelligence & Reporting**
```bash
# Sales and order analysis
./piperdb create-list orders
./piperdb add-item orders '{
  "order_id": "ORD-001", "customer_id": "CUST-123", 
  "total": 259.99, "status": "completed", "region": "north",
  "order_date": "2024-01-10", "items_count": 3
}'

# Business intelligence queries
./piperdb query orders '@status=completed | group-by region | map {region: .key, revenue: sum(.value.total)}'
./piperdb query orders '@order_date>2024-01-01 | avg total'
./piperdb query orders '@total>100 @region=north | sort -total | take 20'
./piperdb query orders 'group-by customer_id | map {customer: .key, orders: count(), lifetime_value: sum(.value.total)}'
```

## 🏗️ Architecture

```
piperdb/
├── cmd/
│   ├── piperdb/         # CLI tool
│   └── piperd/          # HTTP daemon
├── internal/
│   ├── dsl/            # Query language parser & executor
│   │   ├── lexer.go    # Tokenization
│   │   ├── parser.go   # AST generation  
│   │   ├── ast.go      # AST node definitions
│   │   └── executor.go # Query execution engine
│   ├── server/         # REST API server
│   │   └── server.go   # Routes and handlers
│   └── storage/        # BoltDB-based storage layer
│       ├── bolt_storage.go  # Main storage implementation
│       ├── schema_cache.go  # Schema caching
│       └── index_manager.go # Index management
├── pkg/
│   ├── db/             # Public database interface
│   ├── types/          # Core data structures
│   └── config/         # Configuration management
└── test/               # Tests and benchmarks
```

## 🌐 Daemon Mode

PiperDB ships a standalone HTTP daemon (`piperd`) that exposes the full feature set as a REST API. Zero external dependencies — built on Go's standard library `net/http`.

### Starting the Daemon

```bash
# Build and run
go build ./cmd/piperd
./piperd                                    # default: :8080, ./data
./piperd -addr :3000 -data-dir /var/lib/piperdb

# Or with environment variable
PIPERDB_DATA_DIR=/var/lib/piperdb ./piperd -addr :3000
```

### REST API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/lists` | Create a list |
| `GET` | `/lists` | List all lists |
| `GET` | `/lists/{id}` | Get list info |
| `DELETE` | `/lists/{id}` | Delete a list |
| `GET` | `/lists/{id}/schema` | Get inferred schema |
| `GET` | `/lists/{id}/stats` | Get list statistics |
| `POST` | `/lists/{id}/items` | Add item(s) |
| `GET` | `/lists/{id}/items` | Get items (`?limit=N&offset=N`) |
| `GET` | `/lists/{id}/items/{itemId}` | Get single item |
| `PUT` | `/lists/{id}/items/{itemId}` | Update item |
| `DELETE` | `/lists/{id}/items/{itemId}` | Delete item |
| `POST` | `/lists/{id}/query` | Execute pipe query |
| `POST` | `/query/validate` | Validate pipe syntax |

### curl Examples

#### Managing Lists

```bash
# Create a list
curl -X POST localhost:8080/lists -d '{"id": "products"}'

# List all lists
curl localhost:8080/lists

# Get list info
curl localhost:8080/lists/products

# Delete a list
curl -X DELETE localhost:8080/lists/products
```

#### Adding and Retrieving Items

```bash
# Add a single item
curl -X POST localhost:8080/lists/products/items \
  -d '{"name": "iPhone", "price": 999, "brand": "Apple", "tags": ["phone", "5g"]}'

# Add multiple items at once
curl -X POST localhost:8080/lists/products/items -d '[
  {"name": "MacBook", "price": 2499, "brand": "Apple", "tags": ["laptop"]},
  {"name": "Pixel", "price": 699, "brand": "Google", "tags": ["phone", "android"]}
]'

# Get items with pagination
curl "localhost:8080/lists/products/items?limit=10&offset=0"

# Get a single item by ID
curl localhost:8080/lists/products/items/369b94a5-4b8f-4b2e-9001-606e6926f1b1

# Update an item
curl -X PUT localhost:8080/lists/products/items/369b94a5-... \
  -d '{"name": "iPhone 16", "price": 1099, "brand": "Apple"}'

# Delete an item
curl -X DELETE localhost:8080/lists/products/items/369b94a5-...
```

#### Querying with the DSL

Pipe expressions are sent in a JSON body to avoid URL-encoding issues with `@`, `>`, `|`, `[]` etc.

```bash
# Filter and sort
curl -X POST localhost:8080/lists/products/query \
  -d '{"pipe": "@price<1000 | sort price"}'

# Nested field access
curl -X POST localhost:8080/lists/products/query \
  -d '{"pipe": "@user.profile.city=Sydney"}'

# Array wildcard — find items where any tag is "phone"
curl -X POST localhost:8080/lists/products/query \
  -d '{"pipe": "@tags[]=phone"}'

# Complex pipeline
curl -X POST localhost:8080/lists/products/query \
  -d '{"pipe": "@price>500 @brand=Apple | sort -price | select name price | take 5"}'

# Aggregation
curl -X POST localhost:8080/lists/products/query \
  -d '{"pipe": "avg price"}'

# Validate syntax without executing
curl -X POST localhost:8080/query/validate \
  -d '{"pipe": "@price<100 | sort -price"}'
# → {"valid": true}
```

#### Schema and Statistics

```bash
# View inferred schema
curl localhost:8080/lists/products/schema

# View list statistics
curl localhost:8080/lists/products/stats
```

## ⚡ Performance

### Index vs Sequential Scan (Apple M4)

Benchmarked with synthetic product data (10 brands × 10 categories × 4 statuses, varied prices/ratings). Indexes are equality-only on `brand`, `category`, and `status`. Times are per-query averages over 200 iterations.

#### 1,000 items

| Query | Sequential | Indexed | Speedup |
|-------|-----------|---------|---------|
| `@brand=Apple` | 2.6ms | 289μs | **9.0×** |
| `@brand=Apple @category=phone` | 2.7ms | 305μs | **8.8×** |
| `@brand=Samsung \| sort -price \| take 10` | 2.6ms | 272μs | **9.7×** |
| `@brand=Apple \| count` | 2.6ms | 291μs | **9.1×** |
| `@price>1000 @price<2000` (range, no index) | 2.6ms | — | — |
| `sort -price` (full sort) | 3.3ms | — | — |

#### 10,000 items

| Query | Sequential | Indexed | Speedup |
|-------|-----------|---------|---------|
| `@brand=Apple` | 2.6ms | 2.6ms | 1.0× |
| `@brand=Apple @category=phone` | 2.7ms | 2.7ms | 1.0× |
| `@brand=Samsung \| sort -price \| take 10` | 2.7ms | 3.3ms | 0.8× |
| `@brand=Apple \| count` | 2.7ms | 2.6ms | 1.0× |
| `@price>1000 @price<2000` (range) | 2.6ms | — | — |
| `sort -price` (full sort) | 3.3ms | — | — |

**Key observations:**
- At 1k items, indexes deliver **~9× speedup** by avoiding the full-table scan.
- At 10k items, index benefit flattens because the index returns ~1k matching items (1/10 selectivity), each requiring individual BoltDB deserialization — approaching full-scan cost.
- Sequential scan time is dominated by BoltDB read-transaction overhead, not item count — 1k and 10k scan in similar time (~2.6ms).
- Indexes are most effective with **high selectivity** (few matches relative to total items).

### Running Benchmarks

```bash
# Full performance test with comparison table
go test -v -run=TestQueryPerformance -timeout=30m ./test/benchmarks/

# Go benchmarks (1k items, quick)
go test -bench=. -benchmem ./test/benchmarks/
```

## 🛠️ API Usage

### Go API

```go
package main

import (
    "context"
    "fmt"
    "github.com/tjstebbing/piperdb/pkg/db"
)

func main() {
    // Initialize database
    database, err := db.Open(db.DefaultConfig())
    if err != nil {
        panic(err)
    }
    defer database.Close()

    ctx := context.Background()

    // Create list
    err = database.CreateList(ctx, "my-list")
    if err != nil {
        panic(err)
    }

    // Add data
    itemID, err := database.AddItem(ctx, "my-list", map[string]interface{}{
        "name":     "Example Item",
        "category": "test", 
        "value":    42,
    })
    if err != nil {
        panic(err)
    }

    // Query with DSL
    results, err := database.ExecutePipe(ctx, "my-list", 
        "@category=test | @value>10 | sort -value", nil)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Found %d items\n", len(results.Items))
    for _, item := range results.Items {
        fmt.Printf("- %s: %v\n", item["name"], item["value"])
    }
}
```

### Configuration

```go
config := &db.Config{
    DataDir:            "./data",
    MaxMemory:          2 * 1024 * 1024 * 1024, // 2GB
    QueryTimeout:       30 * time.Second,
    WALEnabled:         true,
    SyncWrites:         true,
    SchemaInference: db.SchemaInferenceConfig{
        Enabled:         true,
        MinSampleSize:   3,
        ConfidenceLevel: 0.8,
    },
}

database, err := db.Open(config)
```

## 🧪 Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific test suites
go test ./test/integration/ -v
go test ./test/dsl/ -v

# Benchmark performance (quick, 1k items)
go test -bench=. -benchmem ./test/benchmarks/

# Full index vs sequential comparison (1k + 10k items)
go test -v -run=TestQueryPerformance -timeout=30m ./test/benchmarks/

# Test DSL parsing
go test ./internal/dsl/ -v
```

## 📖 Documentation

- [Technical Specification](./DB_SPEC.md) - Detailed technical design
- [DSL Reference](./internal/dsl/README.md) - Complete language reference
- [API Documentation](./pkg/) - Go API reference
- [Configuration Guide](./pkg/config/) - Configuration options

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🚀 Roadmap

- [ ] **Advanced DSL Features**
  - [ ] Set operations (`union`, `intersect`, `diff`)
  - [ ] Nested object access (`@user.profile.name`)
  - [ ] Advanced text search (fuzzy, regex)
  - [ ] Custom aggregation functions

- [ ] **Performance & Optimization**  
  - [ ] Query planning and optimization
  - [ ] Automatic index creation
  - [ ] Query result caching
  - [ ] Parallel query execution

- [ ] **Developer Experience**
  - [ ] Web-based query interface
  - [ ] Query syntax highlighting
  - [ ] Performance profiling tools
  - [ ] Schema visualization

- [ ] **Enterprise Features**
  - [ ] Replication and clustering
  - [ ] Backup and restore
  - [ ] Authentication and authorization
  - [ ] Audit logging

---

**Built with ❤️ by the PiperDB team**

*PiperDB: Where lists meet powerful queries*
