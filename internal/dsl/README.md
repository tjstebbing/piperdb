# PiperDB DSL Implementation

## Syntax Overview

```
pipe := stage ('|' stage)*
stage := filter | transform | sort | aggregate | slice | setop

# Filter stages
filter := '@' field-path op value | text-search | boolean-expr
op := '=' | '<' | '>' | '>=' | '<=' | '!=' | '~' | '^' | '$'
text-search := '"' text '"'
boolean-expr := filter 'AND' filter | filter 'OR' filter | 'NOT' filter

# Field paths (nested access)
field-path := field ('.' field)* ('[' index ']' | '[]')*
field := identifier
index := number

# Transform stages  
transform := 'map' '{' field-spec (',' field-spec)* '}' | 'select' field-path+ | 'pluck' field-path
field-spec := field-path (':' identifier)?

# Sort stages
sort := 'sort' ('-'? field-path)+

# Aggregate stages
aggregate := 'count' | 'sum' field-path | 'avg' field-path | 'min' field-path | 'max' field-path | 'group-by' field-path+

# Slice stages
slice := 'take' number | 'skip' number | 'first' | 'last'

# Set operations (future)
setop := 'union' list | 'diff' list | 'intersect' list
```

## Examples

```
# Simple filtering
@price<1000 | @brand=Apple

# Nested field access
@user.profile.name=Alice | @user.address.city=Sydney

# Array access
@tags[0]=golang                    # first element equals "golang"
@tags[]=phone                      # any element equals "phone"
@items[].price>100                 # any item's price > 100

# Text search with filtering  
"golang tutorial" | @status=published | sort -date

# Aggregation and grouping
@status=completed | group-by customer

# Complex pipeline
@category=electronics @price>100 | sort price | take 10 | select name price
```

## Implementation Architecture

```
Input String
     ↓
   Lexer     → Token stream: [AT, FIELD, LT, NUMBER, PIPE, SORT, ...]
     ↓
   Parser    → AST: PipeExpr{stages: [FilterStage{...}, SortStage{...}]}  
     ↓
  Executor   → Resolves field paths, executes against storage → ResultSet
```

## Token Types

- **Literals**: FIELD, NUMBER, STRING, BOOLEAN
- **Operators**: PIPE, EQ, LT, GT, LTE, GTE, NEQ, MATCH, PREFIX, SUFFIX
- **Keywords**: MAP, SELECT, SORT, COUNT, SUM, AVG, MIN, MAX, GROUP_BY, TAKE, SKIP, FIRST, LAST, AND, OR, NOT
- **Symbols**: LBRACE, RBRACE, LBRACKET, RBRACKET, LPAREN, RPAREN, COMMA, MINUS, DOT, AT, COLON

## AST Nodes

- **PipeExpr**: Root node containing stages
- **FilterStage**: Field filtering and text search
  - Uses `FieldPath` for nested access (`user.profile.name`, `tags[]`, `items[].price`)
  - Wildcard `[]` matches if ANY array element satisfies the condition
- **TransformStage**: Field selection and mapping (map, select, pluck)
- **SortStage**: Ordering by field paths, prefix `-` for descending
- **AggregateStage**: count, sum, avg, min, max, group-by
- **SliceStage**: take, skip, first, last

## FieldPath

The `FieldPath` type represents paths through nested JSON structures:

| Segment Type | Syntax | Example |
|-------------|--------|---------|
| `SegmentField` | `name` | Object key access |
| `SegmentIndex` | `[0]` | Array index access |
| `SegmentWildcard` | `[]` | All array elements |

Paths are resolved by `resolveFieldPath()` in the executor, which traverses the data and returns all matching values. For wildcards, filter conditions match if any resolved value satisfies the comparison.
