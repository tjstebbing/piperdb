# PiperDB DSL Implementation

## Syntax Overview

```
pipe := list-name '|' stage ('|' stage)*
stage := filter | transform | sort | aggregate | slice | setop

# Filter stages
filter := '@' field op value | text-search | boolean-expr
op := ':' | ':<' | ':>' | ':>=' | ':<=' | ':!=' | '~' | '^' | '$'
text-search := '"' text '"' | word+
boolean-expr := filter 'AND' filter | filter 'OR' filter | 'NOT' filter

# Transform stages  
transform := 'map' '{' field-list '}' | 'select' field-list | 'pluck' field
field-list := field (',' field)*

# Sort stages
sort := 'sort' ('-'?) field

# Aggregate stages
aggregate := 'count' | 'sum' field | 'avg' field | 'group-by' field

# Slice stages
slice := 'take' number | 'skip' number | 'first' | 'last'

# Set operations (future)
setop := 'union' list | 'diff' list | 'intersect' list
```

## Examples

```
# Simple filtering
products | @price:<1000 | @brand:Apple

# Text search with filtering  
blog-posts | "golang tutorial" @status:published | sort -date

# Aggregation and grouping
orders | @status:completed | group-by customer | map {customer: .key, total: count()}

# Complex pipeline
products | @category:electronics @price:>100 | sort price | take 10 | select name price
```

## Implementation Architecture

```
Input String
     ↓
   Lexer     → Token stream: [FIELD, COLON, NUMBER, PIPE, SORT, ...]
     ↓
   Parser    → AST: PipeExpr{stages: [FilterStage{...}, SortStage{...}]}  
     ↓
  Executor   → Execute against storage: ResultSet
```

## Token Types

- **Literals**: FIELD, NUMBER, STRING, BOOLEAN
- **Operators**: PIPE, COLON, LT, GT, EQ, etc.
- **Keywords**: MAP, SELECT, SORT, COUNT, etc.
- **Symbols**: LBRACE, RBRACE, COMMA, MINUS, etc.

## AST Nodes

- **PipeExpr**: Root node containing stages
- **FilterStage**: Field filtering and text search
- **TransformStage**: Field selection and mapping  
- **SortStage**: Ordering operations
- **AggregateStage**: Counting, summing, grouping
- **SliceStage**: Limiting and pagination
