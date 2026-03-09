package types

import (
	"time"
)

// List represents a pure list with only database concerns
type List struct {
	ID        string     `json:"id"`
	Schema    *Schema    `json:"schema,omitempty"`
	Stats     *ListStats `json:"stats,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`

	// Internal storage fields
	ItemCount  int64     `json:"-"`
	TotalSize  int64     `json:"-"`
	LastAccess time.Time `json:"-"`
}

// ListItem represents a single item within a list
type ListItem struct {
	ID        string                 `json:"id"`
	ListID    string                 `json:"list_id"`
	Position  int64                  `json:"position"`
	Data      map[string]interface{} `json:"data"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`

	// Internal fields for optimization
	Size int32  `json:"-"`
	Hash uint64 `json:"-"` // For deduplication
}

// Schema represents the structure and types of fields in a list
type Schema struct {
	Fields    map[string]*FieldDef `json:"fields"`
	Version   int32                `json:"version"`
	Inferred  bool                 `json:"inferred"`
	UpdatedAt time.Time            `json:"updated_at"`
}

// FieldDef defines the characteristics of a field within a schema
type FieldDef struct {
	Type        FieldType   `json:"type"`
	Required    bool        `json:"required"`
	Unique      bool        `json:"unique"`
	Default     interface{} `json:"default,omitempty"`
	Description string      `json:"description,omitempty"`

	// Statistics for inference
	SeenInCount int64 `json:"seen_in_count"`
	TotalItems  int64 `json:"total_items"`

	// Type hints for UI generation
	TypeHints []string `json:"type_hints,omitempty"`

	// Indexing information
	Indexed   bool   `json:"indexed"`
	IndexType string `json:"index_type,omitempty"` // btree, hash, text
}

// FieldType represents the data type of a field
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

// String returns the string representation of a FieldType
func (ft FieldType) String() string {
	switch ft {
	case FieldString:
		return "string"
	case FieldNumber:
		return "number"
	case FieldBoolean:
		return "boolean"
	case FieldArray:
		return "array"
	case FieldObject:
		return "object"
	case FieldDate:
		return "date"
	case FieldURL:
		return "url"
	case FieldEmail:
		return "email"
	case FieldRichText:
		return "rich_text"
	case FieldEnum:
		return "enum"
	default:
		return "unknown"
	}
}

// ListStats contains performance and usage statistics for a list
type ListStats struct {
	ItemCount     int64     `json:"item_count"`
	TotalSize     int64     `json:"total_size"`
	AvgItemSize   float64   `json:"avg_item_size"`
	UniqueFields  int       `json:"unique_fields"`
	LastModified  time.Time `json:"last_modified"`
	QueryCount    int64     `json:"query_count"`
	LastQueried   time.Time `json:"last_queried"`
	PopularFields []string  `json:"popular_fields,omitempty"`
	IndexCount    int       `json:"index_count"`
}

// ResultSet represents the result of a pipe query execution
type ResultSet struct {
	Items      []map[string]interface{} `json:"items"`
	Schema     *Schema                  `json:"schema,omitempty"`
	TotalCount int64                    `json:"total_count"`
	HasMore    bool                     `json:"has_more"`
	NextCursor string                   `json:"next_cursor,omitempty"`

	// Query performance metadata
	QueryTime   time.Duration `json:"query_time"`
	IndexHits   int64         `json:"index_hits"`
	MemoryUsed  int64         `json:"memory_used"`
	PlanUsed    string        `json:"plan_used,omitempty"`
}

// QueryOptions provides configuration for pipe query execution
type QueryOptions struct {
	Limit     int64         `json:"limit,omitempty"`
	Offset    int64         `json:"offset,omitempty"`
	Cursor    string        `json:"cursor,omitempty"`
	Streaming bool          `json:"streaming,omitempty"`
	Timeout   time.Duration `json:"timeout,omitempty"`

	// Performance hints
	UseIndexes     bool  `json:"use_indexes"`
	MaxMemory      int64 `json:"max_memory,omitempty"`
	ForceFullScan  bool  `json:"force_full_scan,omitempty"`
	DisableCache   bool  `json:"disable_cache,omitempty"`
}

// IndexInfo provides information about a list index
type IndexInfo struct {
	FieldName   string    `json:"field_name"`
	IndexType   string    `json:"index_type"`   // btree, hash, text
	Size        int64     `json:"size"`
	Entries     int64     `json:"entries"`
	Selectivity float64   `json:"selectivity"`
	LastUsed    time.Time `json:"last_used"`
	HitCount    int64     `json:"hit_count"`
	CreatedAt   time.Time `json:"created_at"`
}

// QueryPlan provides information about query execution strategy
type QueryPlan struct {
	PipeExpr        string        `json:"pipe_expr"`
	EstimatedCost   float64       `json:"estimated_cost"`
	EstimatedTime   time.Duration `json:"estimated_time"`
	EstimatedMemory int64         `json:"estimated_memory"`
	IndexesUsed     []string      `json:"indexes_used"`
	Strategy        string        `json:"strategy"` // sequential, index, hybrid
	Stages          []StageInfo   `json:"stages"`
}

// StageInfo describes execution details for a pipe stage
type StageInfo struct {
	Type            string        `json:"type"`            // filter, transform, sort, etc.
	EstimatedCost   float64       `json:"estimated_cost"`
	EstimatedTime   time.Duration `json:"estimated_time"`
	IndexUsed       string        `json:"index_used,omitempty"`
	Description     string        `json:"description"`
}

// DatabaseStats provides overall database statistics
type DatabaseStats struct {
	Lists         int64         `json:"lists"`
	TotalItems    int64         `json:"total_items"`
	TotalSize     int64         `json:"total_size"`
	Indexes       int64         `json:"indexes"`
	QueryCount    int64         `json:"query_count"`
	AvgQueryTime  time.Duration `json:"avg_query_time"`
	CacheHitRate  float64       `json:"cache_hit_rate"`
	MemoryUsage   int64         `json:"memory_usage"`
	ActiveConns   int           `json:"active_connections"`
	Uptime        time.Duration `json:"uptime"`
}

// Clone creates a deep copy of the Schema
func (s *Schema) Clone() *Schema {
	if s == nil {
		return nil
	}
	
	clone := &Schema{
		Version:   s.Version,
		Inferred:  s.Inferred,
		UpdatedAt: s.UpdatedAt,
		Fields:    make(map[string]*FieldDef),
	}
	
	for k, v := range s.Fields {
		fieldClone := &FieldDef{
			Type:        v.Type,
			Required:    v.Required,
			Unique:      v.Unique,
			Default:     v.Default,
			Description: v.Description,
			SeenInCount: v.SeenInCount,
			TotalItems:  v.TotalItems,
			Indexed:     v.Indexed,
			IndexType:   v.IndexType,
		}
		
		// Clone slices
		if v.TypeHints != nil {
			fieldClone.TypeHints = make([]string, len(v.TypeHints))
			copy(fieldClone.TypeHints, v.TypeHints)
		}
		
		clone.Fields[k] = fieldClone
	}
	
	return clone
}

// GetTotalItems returns the total number of items this schema has seen
func (s *Schema) GetTotalItems() int64 {
	if s == nil || len(s.Fields) == 0 {
		return 0
	}
	
	// Return the max TotalItems from any field
	var max int64
	for _, field := range s.Fields {
		if field.TotalItems > max {
			max = field.TotalItems
		}
	}
	return max
}

// GetFieldNames returns a sorted list of field names
func (s *Schema) GetFieldNames() []string {
	if s == nil {
		return nil
	}
	
	names := make([]string, 0, len(s.Fields))
	for name := range s.Fields {
		names = append(names, name)
	}
	return names
}

// HasField checks if a field exists in the schema
func (s *Schema) HasField(name string) bool {
	if s == nil || s.Fields == nil {
		return false
	}
	_, exists := s.Fields[name]
	return exists
}

// GetField returns the field definition for a given field name
func (s *Schema) GetField(name string) (*FieldDef, bool) {
	if s == nil || s.Fields == nil {
		return nil, false
	}
	field, exists := s.Fields[name]
	return field, exists
}

// UpdateField updates or adds a field definition
func (s *Schema) UpdateField(name string, field *FieldDef) {
	if s.Fields == nil {
		s.Fields = make(map[string]*FieldDef)
	}
	s.Fields[name] = field
	s.UpdatedAt = time.Now()
}

// RemoveField removes a field from the schema
func (s *Schema) RemoveField(name string) {
	if s.Fields != nil {
		delete(s.Fields, name)
		s.UpdatedAt = time.Now()
	}
}
