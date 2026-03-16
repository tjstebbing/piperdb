package dsl

import (
	"fmt"
	"strings"
)

// PathSegmentType represents the type of a path segment
type PathSegmentType int

const (
	SegmentField    PathSegmentType = iota // field name access
	SegmentIndex                           // array index [N]
	SegmentWildcard                        // array wildcard []
)

// PathSegment represents one segment of a field path
type PathSegment struct {
	Type  PathSegmentType
	Name  string // for SegmentField
	Index int    // for SegmentIndex
}

// FieldPath represents a path to a value in nested data
type FieldPath struct {
	Segments []PathSegment
}

// IsEmpty returns true if the path has no segments (used for text search)
func (fp FieldPath) IsEmpty() bool {
	return len(fp.Segments) == 0
}

// Simple returns the field name for single-segment field paths
func (fp FieldPath) Simple() string {
	if len(fp.Segments) == 1 && fp.Segments[0].Type == SegmentField {
		return fp.Segments[0].Name
	}
	return fp.String()
}

// String returns the string representation of a field path
func (fp FieldPath) String() string {
	if len(fp.Segments) == 0 {
		return ""
	}

	var parts []string
	for i, seg := range fp.Segments {
		switch seg.Type {
		case SegmentField:
			if i == 0 {
				parts = append(parts, seg.Name)
			} else {
				parts = append(parts, "."+seg.Name)
			}
		case SegmentIndex:
			parts = append(parts, fmt.Sprintf("[%d]", seg.Index))
		case SegmentWildcard:
			parts = append(parts, "[]")
		}
	}
	return strings.Join(parts, "")
}

// NewSimplePath creates a FieldPath for a simple field name
func NewSimplePath(name string) FieldPath {
	if name == "" {
		return FieldPath{}
	}
	return FieldPath{Segments: []PathSegment{{Type: SegmentField, Name: name}}}
}

// PipeExpr represents a complete pipe expression
type PipeExpr struct {
	Stages []Stage
}

// Stage represents a single stage in a pipe
type Stage interface {
	String() string
	Type() StageType
}

// StageType represents the type of stage
type StageType int

const (
	FilterStageType StageType = iota
	TransformStageType
	SortStageType
	AggregateStageType
	SliceStageType
	SetOpStageType
)

// FilterStage represents filtering operations
type FilterStage struct {
	Conditions []FilterCondition
	Logic      LogicOp // AND, OR for multiple conditions
}

// FilterCondition represents a single filter condition
type FilterCondition struct {
	Path     FieldPath   // Path to field (empty for text search)
	Operator FilterOp   // Comparison operator
	Value    interface{} // Comparison value
	Negate   bool       // NOT condition
}

// FilterOp represents filter operators
type FilterOp int

const (
	OpEquals FilterOp = iota
	OpNotEquals
	OpLessThan
	OpLessThanEqual
	OpGreaterThan
	OpGreaterThanEqual
	OpMatch      // ~ regex/fuzzy match
	OpPrefix     // ^ starts with
	OpSuffix     // $ ends with
	OpExists     // field exists
	OpContains   // for text search
)

// LogicOp represents logical operators
type LogicOp int

const (
	LogicAnd LogicOp = iota
	LogicOr
)

// TransformStage represents field transformation
type TransformStage struct {
	TransformType TransformType
	Fields        []FieldSpec
}

// TransformType represents the type of transformation
type TransformType int

const (
	TransformMap TransformType = iota
	TransformSelect
	TransformPluck
)

// FieldSpec represents a field specification in transformations
type FieldSpec struct {
	Source FieldPath   // Source field path
	Target string      // Target field name (for map)
	Expr   interface{} // Optional expression (future)
}

// SortStage represents sorting operations
type SortStage struct {
	Fields []SortField
}

// SortField represents a single sort field
type SortField struct {
	Path       FieldPath
	Descending bool
}

// AggregateStage represents aggregation operations
type AggregateStage struct {
	AggregateType AggregateType
	Field         FieldPath // Field to aggregate (if applicable)
	GroupBy       []FieldPath // Fields to group by
}

// AggregateType represents the type of aggregation
type AggregateType int

const (
	AggCount AggregateType = iota
	AggSum
	AggAvg
	AggMin
	AggMax
	AggGroupBy
)

// SliceStage represents slicing/pagination operations
type SliceStage struct {
	SliceType SliceType
	Amount    int64 // For take/skip
}

// SliceType represents the type of slice operation
type SliceType int

const (
	SliceTake SliceType = iota
	SliceSkip
	SliceFirst
	SliceLast
)

// SetOpStage represents set operations (future)
type SetOpStage struct {
	Operation SetOperation
	OtherList string
}

// SetOperation represents set operations
type SetOperation int

const (
	SetUnion SetOperation = iota
	SetDiff
	SetIntersect
)

// Implementation of Stage interface

func (f *FilterStage) Type() StageType     { return FilterStageType }
func (t *TransformStage) Type() StageType  { return TransformStageType }
func (s *SortStage) Type() StageType       { return SortStageType }
func (a *AggregateStage) Type() StageType  { return AggregateStageType }
func (s *SliceStage) Type() StageType      { return SliceStageType }
func (s *SetOpStage) Type() StageType      { return SetOpStageType }

// String implementations for debugging

func (p *PipeExpr) String() string {
	var stages []string
	for _, stage := range p.Stages {
		stages = append(stages, stage.String())
	}
	return strings.Join(stages, " | ")
}

func (f *FilterStage) String() string {
	var conditions []string
	for _, cond := range f.Conditions {
		condStr := ""
		if cond.Negate {
			condStr += "NOT "
		}
		if !cond.Path.IsEmpty() {
			condStr += fmt.Sprintf("@%s%s%v", cond.Path.String(), cond.Operator.String(), cond.Value)
		} else {
			condStr += fmt.Sprintf("\"%v\"", cond.Value) // Text search
		}
		conditions = append(conditions, condStr)
	}
	
	logicStr := "AND"
	if f.Logic == LogicOr {
		logicStr = "OR"
	}
	
	return strings.Join(conditions, " "+logicStr+" ")
}

func (t *TransformStage) String() string {
	switch t.TransformType {
	case TransformMap:
		var fields []string
		for _, field := range t.Fields {
			srcStr := field.Source.String()
			if field.Target != "" && field.Target != srcStr {
				fields = append(fields, fmt.Sprintf("%s: %s", field.Target, srcStr))
			} else {
				fields = append(fields, srcStr)
			}
		}
		return fmt.Sprintf("map {%s}", strings.Join(fields, ", "))
	case TransformSelect:
		var fields []string
		for _, field := range t.Fields {
			fields = append(fields, field.Source.String())
		}
		return fmt.Sprintf("select %s", strings.Join(fields, " "))
	case TransformPluck:
		if len(t.Fields) > 0 {
			return fmt.Sprintf("pluck %s", t.Fields[0].Source.String())
		}
		return "pluck"
	default:
		return "transform"
	}
}

func (s *SortStage) String() string {
	var fields []string
	for _, field := range s.Fields {
		fieldStr := field.Path.String()
		if field.Descending {
			fieldStr = "-" + fieldStr
		}
		fields = append(fields, fieldStr)
	}
	return fmt.Sprintf("sort %s", strings.Join(fields, " "))
}

func (a *AggregateStage) String() string {
	switch a.AggregateType {
	case AggCount:
		return "count"
	case AggSum:
		return fmt.Sprintf("sum %s", a.Field.String())
	case AggAvg:
		return fmt.Sprintf("avg %s", a.Field.String())
	case AggMin:
		return fmt.Sprintf("min %s", a.Field.String())
	case AggMax:
		return fmt.Sprintf("max %s", a.Field.String())
	case AggGroupBy:
		var fields []string
		for _, f := range a.GroupBy {
			fields = append(fields, f.String())
		}
		return fmt.Sprintf("group-by %s", strings.Join(fields, " "))
	default:
		return "aggregate"
	}
}

func (s *SliceStage) String() string {
	switch s.SliceType {
	case SliceTake:
		return fmt.Sprintf("take %d", s.Amount)
	case SliceSkip:
		return fmt.Sprintf("skip %d", s.Amount)
	case SliceFirst:
		return "first"
	case SliceLast:
		return "last"
	default:
		return "slice"
	}
}

func (s *SetOpStage) String() string {
	switch s.Operation {
	case SetUnion:
		return fmt.Sprintf("union %s", s.OtherList)
	case SetDiff:
		return fmt.Sprintf("diff %s", s.OtherList)
	case SetIntersect:
		return fmt.Sprintf("intersect %s", s.OtherList)
	default:
		return "setop"
	}
}

// String methods for operators

func (op FilterOp) String() string {
	switch op {
	case OpEquals:
		return "="
	case OpNotEquals:
		return "!="
	case OpLessThan:
		return "<"
	case OpLessThanEqual:
		return "<="
	case OpGreaterThan:
		return ">"
	case OpGreaterThanEqual:
		return ">="
	case OpMatch:
		return "~"
	case OpPrefix:
		return "^"
	case OpSuffix:
		return "$"
	case OpExists:
		return "=exists"
	case OpContains:
		return "contains"
	default:
		return "="
	}
}

// Helper functions for AST construction

// NewPipeExpr creates a new pipe expression
func NewPipeExpr(stages ...Stage) *PipeExpr {
	return &PipeExpr{Stages: stages}
}

// NewFilterStage creates a new filter stage
func NewFilterStage(conditions ...FilterCondition) *FilterStage {
	return &FilterStage{
		Conditions: conditions,
		Logic:      LogicAnd, // Default to AND
	}
}

// NewFilterCondition creates a new filter condition
func NewFilterCondition(field string, op FilterOp, value interface{}) FilterCondition {
	return FilterCondition{
		Path:     NewSimplePath(field),
		Operator: op,
		Value:    value,
		Negate:   false,
	}
}

// NewTextSearch creates a text search condition
func NewTextSearch(text string) FilterCondition {
	return FilterCondition{
		Path:     FieldPath{}, // Empty path indicates text search
		Operator: OpContains,
		Value:    text,
		Negate:   false,
	}
}

// NewTransformStage creates a new transform stage
func NewTransformStage(transformType TransformType, fields ...FieldSpec) *TransformStage {
	return &TransformStage{
		TransformType: transformType,
		Fields:        fields,
	}
}

// NewFieldSpec creates a new field specification
func NewFieldSpec(source string) FieldSpec {
	return FieldSpec{
		Source: NewSimplePath(source),
		Target: source, // Default target same as source
	}
}

// NewFieldSpecWithTarget creates a field spec with different target
func NewFieldSpecWithTarget(source, target string) FieldSpec {
	return FieldSpec{
		Source: NewSimplePath(source),
		Target: target,
	}
}

// NewSortStage creates a new sort stage
func NewSortStage(fields ...SortField) *SortStage {
	return &SortStage{Fields: fields}
}

// NewSortField creates a new sort field
func NewSortField(field string, descending bool) SortField {
	return SortField{
		Path:       NewSimplePath(field),
		Descending: descending,
	}
}

// NewAggregateStage creates a new aggregate stage
func NewAggregateStage(aggType AggregateType, field string) *AggregateStage {
	return &AggregateStage{
		AggregateType: aggType,
		Field:         NewSimplePath(field),
	}
}

// NewGroupByStage creates a new group-by stage
func NewGroupByStage(fields ...string) *AggregateStage {
	var paths []FieldPath
	for _, f := range fields {
		paths = append(paths, NewSimplePath(f))
	}
	return &AggregateStage{
		AggregateType: AggGroupBy,
		GroupBy:       paths,
	}
}

// NewSliceStage creates a new slice stage
func NewSliceStage(sliceType SliceType, amount int64) *SliceStage {
	return &SliceStage{
		SliceType: sliceType,
		Amount:    amount,
	}
}

// Validation methods

// Validate checks if the pipe expression is valid
func (p *PipeExpr) Validate() error {
	if len(p.Stages) == 0 {
		return fmt.Errorf("pipe expression cannot be empty")
	}
	
	for i, stage := range p.Stages {
		if err := p.validateStage(stage, i); err != nil {
			return fmt.Errorf("stage %d: %w", i+1, err)
		}
	}
	
	return nil
}

// validateStage validates a single stage
func (p *PipeExpr) validateStage(stage Stage, position int) error {
	switch s := stage.(type) {
	case *FilterStage:
		return p.validateFilterStage(s)
	case *TransformStage:
		return p.validateTransformStage(s)
	case *SortStage:
		return p.validateSortStage(s)
	case *AggregateStage:
		return p.validateAggregateStage(s)
	case *SliceStage:
		return p.validateSliceStage(s)
	case *SetOpStage:
		return p.validateSetOpStage(s)
	default:
		return fmt.Errorf("unknown stage type")
	}
}

func (p *PipeExpr) validateFilterStage(stage *FilterStage) error {
	if len(stage.Conditions) == 0 {
		return fmt.Errorf("filter stage must have at least one condition")
	}
	return nil
}

func (p *PipeExpr) validateTransformStage(stage *TransformStage) error {
	if len(stage.Fields) == 0 {
		return fmt.Errorf("transform stage must specify at least one field")
	}
	return nil
}

func (p *PipeExpr) validateSortStage(stage *SortStage) error {
	if len(stage.Fields) == 0 {
		return fmt.Errorf("sort stage must specify at least one field")
	}
	return nil
}

func (p *PipeExpr) validateAggregateStage(stage *AggregateStage) error {
	switch stage.AggregateType {
	case AggSum, AggAvg, AggMin, AggMax:
		if stage.Field.IsEmpty() {
			return fmt.Errorf("aggregate stage requires a field")
		}
	case AggGroupBy:
		if len(stage.GroupBy) == 0 {
			return fmt.Errorf("group-by stage requires at least one field")
		}
	}
	return nil
}

func (p *PipeExpr) validateSliceStage(stage *SliceStage) error {
	switch stage.SliceType {
	case SliceTake, SliceSkip:
		if stage.Amount <= 0 {
			return fmt.Errorf("slice amount must be positive")
		}
	}
	return nil
}

func (p *PipeExpr) validateSetOpStage(stage *SetOpStage) error {
	if stage.OtherList == "" {
		return fmt.Errorf("set operation requires another list")
	}
	return nil
}
