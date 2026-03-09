package dsl

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tjstebbing/piperdb/pkg/types"
)

// Executor executes parsed DSL expressions against data
type Executor struct {
	storage StorageInterface
}

// StorageInterface defines what the executor needs from storage
type StorageInterface interface {
	GetItems(ctx context.Context, listID string, opts *types.QueryOptions) (*types.ResultSet, error)
	GetSchema(ctx context.Context, listID string) (*types.Schema, error)
}

// ExecutionContext holds context for query execution
type ExecutionContext struct {
	ListID     string
	Schema     *types.Schema
	Items      []map[string]interface{}
	StartTime  time.Time
	MemoryUsed int64
}

// NewExecutor creates a new query executor
func NewExecutor(storage StorageInterface) *Executor {
	return &Executor{storage: storage}
}

// Execute executes a parsed pipe expression
func (e *Executor) Execute(ctx context.Context, listID string, pipe *PipeExpr, opts *types.QueryOptions) (*types.ResultSet, error) {
	startTime := time.Now()
	
	// Get initial data from storage
	result, err := e.storage.GetItems(ctx, listID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get items: %w", err)
	}
	
	// Get schema for type information
	schema, err := e.storage.GetSchema(ctx, listID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	
	// Create execution context
	execCtx := &ExecutionContext{
		ListID:    listID,
		Schema:    schema,
		Items:     result.Items,
		StartTime: startTime,
	}
	
	// Execute each stage
	for i, stage := range pipe.Stages {
		err := e.executeStage(ctx, execCtx, stage)
		if err != nil {
			return nil, fmt.Errorf("stage %d: %w", i+1, err)
		}
		
		// Apply pagination limits if this is the last stage
		if i == len(pipe.Stages)-1 && opts != nil {
			e.applyPagination(execCtx, opts)
		}
	}
	
	// Build result
	finalResult := &types.ResultSet{
		Items:      execCtx.Items,
		Schema:     execCtx.Schema,
		TotalCount: int64(len(execCtx.Items)),
		HasMore:    false,
		QueryTime:  time.Since(startTime),
		IndexHits:  0, // TODO: Track index usage
		MemoryUsed: execCtx.MemoryUsed,
		PlanUsed:   "sequential", // TODO: Add query planning
	}
	
	return finalResult, nil
}

// executeStage executes a single stage
func (e *Executor) executeStage(ctx context.Context, execCtx *ExecutionContext, stage Stage) error {
	switch s := stage.(type) {
	case *FilterStage:
		return e.executeFilterStage(execCtx, s)
	case *TransformStage:
		return e.executeTransformStage(execCtx, s)
	case *SortStage:
		return e.executeSortStage(execCtx, s)
	case *AggregateStage:
		return e.executeAggregateStage(execCtx, s)
	case *SliceStage:
		return e.executeSliceStage(execCtx, s)
	case *SetOpStage:
		return fmt.Errorf("set operations not implemented yet")
	default:
		return fmt.Errorf("unknown stage type: %T", stage)
	}
}

// executeFilterStage executes filtering
func (e *Executor) executeFilterStage(execCtx *ExecutionContext, stage *FilterStage) error {
	var filteredItems []map[string]interface{}
	
	for _, item := range execCtx.Items {
		if e.evaluateFilterConditions(item, stage.Conditions, stage.Logic) {
			filteredItems = append(filteredItems, item)
		}
	}
	
	execCtx.Items = filteredItems
	return nil
}

// evaluateFilterConditions evaluates filter conditions for an item
func (e *Executor) evaluateFilterConditions(item map[string]interface{}, conditions []FilterCondition, logic LogicOp) bool {
	if len(conditions) == 0 {
		return true
	}
	
	results := make([]bool, len(conditions))
	
	for i, condition := range conditions {
		result := e.evaluateFilterCondition(item, condition)
		if condition.Negate {
			result = !result
		}
		results[i] = result
	}
	
	// Apply logic operator
	if logic == LogicOr {
		for _, result := range results {
			if result {
				return true
			}
		}
		return false
	} else { // LogicAnd
		for _, result := range results {
			if !result {
				return false
			}
		}
		return true
	}
}

// evaluateFilterCondition evaluates a single filter condition
func (e *Executor) evaluateFilterCondition(item map[string]interface{}, condition FilterCondition) bool {
	// Text search (empty field)
	if condition.Field == "" {
		return e.evaluateTextSearch(item, condition)
	}
	
	// Field-based filtering
	fieldValue, exists := item[condition.Field]
	
	// Handle field existence check
	if condition.Operator == OpExists {
		return exists
	}
	
	if !exists {
		return false
	}
	
	return e.compareValues(fieldValue, condition.Operator, condition.Value)
}

// evaluateTextSearch performs full-text search across all string fields
func (e *Executor) evaluateTextSearch(item map[string]interface{}, condition FilterCondition) bool {
	searchTerm := strings.ToLower(fmt.Sprintf("%v", condition.Value))
	
	for _, value := range item {
		if str, ok := value.(string); ok {
			if strings.Contains(strings.ToLower(str), searchTerm) {
				return true
			}
		}
	}
	
	return false
}

// compareValues compares two values using the specified operator
func (e *Executor) compareValues(fieldValue interface{}, op FilterOp, compareValue interface{}) bool {
	switch op {
	case OpEquals:
		return e.valuesEqual(fieldValue, compareValue)
	case OpNotEquals:
		return !e.valuesEqual(fieldValue, compareValue)
	case OpLessThan:
		return e.compareNumeric(fieldValue, compareValue) < 0
	case OpLessThanEqual:
		return e.compareNumeric(fieldValue, compareValue) <= 0
	case OpGreaterThan:
		return e.compareNumeric(fieldValue, compareValue) > 0
	case OpGreaterThanEqual:
		return e.compareNumeric(fieldValue, compareValue) >= 0
	case OpMatch:
		return e.matchesPattern(fieldValue, compareValue)
	case OpPrefix:
		return e.hasPrefix(fieldValue, compareValue)
	case OpSuffix:
		return e.hasSuffix(fieldValue, compareValue)
	case OpContains:
		return e.contains(fieldValue, compareValue)
	default:
		return false
	}
}

// valuesEqual checks if two values are equal
func (e *Executor) valuesEqual(a, b interface{}) bool {
	// Handle numeric comparisons
	if e.isNumeric(a) && e.isNumeric(b) {
		return e.toFloat64(a) == e.toFloat64(b)
	}
	
	// Handle string comparisons
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr == bStr
}

// compareNumeric compares two values numerically
func (e *Executor) compareNumeric(a, b interface{}) int {
	aVal := e.toFloat64(a)
	bVal := e.toFloat64(b)
	
	if aVal < bVal {
		return -1
	} else if aVal > bVal {
		return 1
	}
	return 0
}

// isNumeric checks if a value can be treated as a number
func (e *Executor) isNumeric(v interface{}) bool {
	switch v.(type) {
	case int, int32, int64, float32, float64:
		return true
	case string:
		_, err := strconv.ParseFloat(v.(string), 64)
		return err == nil
	default:
		return false
	}
}

// toFloat64 converts a value to float64
func (e *Executor) toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
		return 0
	default:
		return 0
	}
}

// matchesPattern checks if value matches a regex pattern
func (e *Executor) matchesPattern(value, pattern interface{}) bool {
	valueStr := fmt.Sprintf("%v", value)
	patternStr := fmt.Sprintf("%v", pattern)
	
	regex, err := regexp.Compile(patternStr)
	if err != nil {
		return false
	}
	
	return regex.MatchString(valueStr)
}

// hasPrefix checks if value starts with prefix
func (e *Executor) hasPrefix(value, prefix interface{}) bool {
	valueStr := fmt.Sprintf("%v", value)
	prefixStr := fmt.Sprintf("%v", prefix)
	return strings.HasPrefix(strings.ToLower(valueStr), strings.ToLower(prefixStr))
}

// hasSuffix checks if value ends with suffix
func (e *Executor) hasSuffix(value, suffix interface{}) bool {
	valueStr := fmt.Sprintf("%v", value)
	suffixStr := fmt.Sprintf("%v", suffix)
	return strings.HasSuffix(strings.ToLower(valueStr), strings.ToLower(suffixStr))
}

// contains checks if value contains substring
func (e *Executor) contains(value, substring interface{}) bool {
	valueStr := fmt.Sprintf("%v", value)
	subStr := fmt.Sprintf("%v", substring)
	return strings.Contains(strings.ToLower(valueStr), strings.ToLower(subStr))
}

// executeTransformStage executes field transformations
func (e *Executor) executeTransformStage(execCtx *ExecutionContext, stage *TransformStage) error {
	var transformedItems []map[string]interface{}
	
	for _, item := range execCtx.Items {
		transformed, err := e.transformItem(item, stage)
		if err != nil {
			return err
		}
		transformedItems = append(transformedItems, transformed)
	}
	
	execCtx.Items = transformedItems
	return nil
}

// transformItem transforms a single item based on the transform stage
func (e *Executor) transformItem(item map[string]interface{}, stage *TransformStage) (map[string]interface{}, error) {
	switch stage.TransformType {
	case TransformMap:
		return e.mapTransform(item, stage.Fields)
	case TransformSelect:
		return e.selectTransform(item, stage.Fields)
	case TransformPluck:
		return e.pluckTransform(item, stage.Fields)
	default:
		return nil, fmt.Errorf("unknown transform type")
	}
}

// mapTransform creates a new object with specified fields and mappings
func (e *Executor) mapTransform(item map[string]interface{}, fields []FieldSpec) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	
	for _, field := range fields {
		if value, exists := item[field.Source]; exists {
			result[field.Target] = value
		}
	}
	
	return result, nil
}

// selectTransform selects only specified fields
func (e *Executor) selectTransform(item map[string]interface{}, fields []FieldSpec) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	
	for _, field := range fields {
		if value, exists := item[field.Source]; exists {
			result[field.Source] = value
		}
	}
	
	return result, nil
}

// pluckTransform extracts a single field value
func (e *Executor) pluckTransform(item map[string]interface{}, fields []FieldSpec) (map[string]interface{}, error) {
	if len(fields) == 0 {
		return make(map[string]interface{}), nil
	}
	
	field := fields[0]
	if value, exists := item[field.Source]; exists {
		return map[string]interface{}{field.Source: value}, nil
	}
	
	return make(map[string]interface{}), nil
}

// executeSortStage executes sorting
func (e *Executor) executeSortStage(execCtx *ExecutionContext, stage *SortStage) error {
	if len(stage.Fields) == 0 {
		return nil
	}
	
	sort.Slice(execCtx.Items, func(i, j int) bool {
		return e.compareItems(execCtx.Items[i], execCtx.Items[j], stage.Fields)
	})
	
	return nil
}

// compareItems compares two items for sorting
func (e *Executor) compareItems(a, b map[string]interface{}, sortFields []SortField) bool {
	for _, field := range sortFields {
		aVal, aExists := a[field.Field]
		bVal, bExists := b[field.Field]
		
		// Handle missing values
		if !aExists && !bExists {
			continue
		}
		if !aExists {
			return !field.Descending // Missing values sort last (or first if desc)
		}
		if !bExists {
			return field.Descending
		}
		
		// Compare values
		cmp := e.compareForSort(aVal, bVal)
		if cmp != 0 {
			result := cmp < 0
			if field.Descending {
				result = !result
			}
			return result
		}
	}
	
	return false // Equal
}

// compareForSort compares two values for sorting purposes
func (e *Executor) compareForSort(a, b interface{}) int {
	// Try numeric comparison first
	if e.isNumeric(a) && e.isNumeric(b) {
		return e.compareNumeric(a, b)
	}
	
	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// executeAggregateStage executes aggregation operations
func (e *Executor) executeAggregateStage(execCtx *ExecutionContext, stage *AggregateStage) error {
	switch stage.AggregateType {
	case AggCount:
		return e.executeCount(execCtx)
	case AggSum:
		return e.executeSum(execCtx, stage.Field)
	case AggAvg:
		return e.executeAvg(execCtx, stage.Field)
	case AggMin:
		return e.executeMin(execCtx, stage.Field)
	case AggMax:
		return e.executeMax(execCtx, stage.Field)
	case AggGroupBy:
		return e.executeGroupBy(execCtx, stage.GroupBy)
	default:
		return fmt.Errorf("unknown aggregate type")
	}
}

// executeCount counts items
func (e *Executor) executeCount(execCtx *ExecutionContext) error {
	count := len(execCtx.Items)
	execCtx.Items = []map[string]interface{}{
		{"count": count},
	}
	return nil
}

// executeSum sums numeric values in a field
func (e *Executor) executeSum(execCtx *ExecutionContext, field string) error {
	sum := 0.0
	count := 0
	
	for _, item := range execCtx.Items {
		if value, exists := item[field]; exists && e.isNumeric(value) {
			sum += e.toFloat64(value)
			count++
		}
	}
	
	execCtx.Items = []map[string]interface{}{
		{"sum": sum, "field": field, "count": count},
	}
	return nil
}

// executeAvg calculates average of numeric values
func (e *Executor) executeAvg(execCtx *ExecutionContext, field string) error {
	sum := 0.0
	count := 0
	
	for _, item := range execCtx.Items {
		if value, exists := item[field]; exists && e.isNumeric(value) {
			sum += e.toFloat64(value)
			count++
		}
	}
	
	avg := 0.0
	if count > 0 {
		avg = sum / float64(count)
	}
	
	execCtx.Items = []map[string]interface{}{
		{"avg": avg, "field": field, "count": count},
	}
	return nil
}

// executeMin finds minimum value
func (e *Executor) executeMin(execCtx *ExecutionContext, field string) error {
	var min interface{}
	found := false
	
	for _, item := range execCtx.Items {
		if value, exists := item[field]; exists {
			if !found || e.compareForSort(value, min) < 0 {
				min = value
				found = true
			}
		}
	}
	
	result := map[string]interface{}{"field": field}
	if found {
		result["min"] = min
	} else {
		result["min"] = nil
	}
	
	execCtx.Items = []map[string]interface{}{result}
	return nil
}

// executeMax finds maximum value
func (e *Executor) executeMax(execCtx *ExecutionContext, field string) error {
	var max interface{}
	found := false
	
	for _, item := range execCtx.Items {
		if value, exists := item[field]; exists {
			if !found || e.compareForSort(value, max) > 0 {
				max = value
				found = true
			}
		}
	}
	
	result := map[string]interface{}{"field": field}
	if found {
		result["max"] = max
	} else {
		result["max"] = nil
	}
	
	execCtx.Items = []map[string]interface{}{result}
	return nil
}

// executeGroupBy groups items by specified fields
func (e *Executor) executeGroupBy(execCtx *ExecutionContext, groupFields []string) error {
	groups := make(map[string][]map[string]interface{})
	
	for _, item := range execCtx.Items {
		key := e.buildGroupKey(item, groupFields)
		groups[key] = append(groups[key], item)
	}
	
	var result []map[string]interface{}
	for key, items := range groups {
		groupItem := map[string]interface{}{
			"key":   key,
			"count": len(items),
			"items": items,
		}
		result = append(result, groupItem)
	}
	
	execCtx.Items = result
	return nil
}

// buildGroupKey builds a key for grouping
func (e *Executor) buildGroupKey(item map[string]interface{}, fields []string) string {
	var keyParts []string
	
	for _, field := range fields {
		if value, exists := item[field]; exists {
			keyParts = append(keyParts, fmt.Sprintf("%v", value))
		} else {
			keyParts = append(keyParts, "<null>")
		}
	}
	
	return strings.Join(keyParts, "|")
}

// executeSliceStage executes slicing operations
func (e *Executor) executeSliceStage(execCtx *ExecutionContext, stage *SliceStage) error {
	itemCount := len(execCtx.Items)
	
	switch stage.SliceType {
	case SliceTake:
		if stage.Amount < int64(itemCount) {
			execCtx.Items = execCtx.Items[:stage.Amount]
		}
	case SliceSkip:
		if stage.Amount < int64(itemCount) {
			execCtx.Items = execCtx.Items[stage.Amount:]
		} else {
			execCtx.Items = []map[string]interface{}{}
		}
	case SliceFirst:
		if itemCount > 0 {
			execCtx.Items = execCtx.Items[:1]
		}
	case SliceLast:
		if itemCount > 0 {
			execCtx.Items = execCtx.Items[itemCount-1:]
		}
	}
	
	return nil
}

// applyPagination applies pagination options
func (e *Executor) applyPagination(execCtx *ExecutionContext, opts *types.QueryOptions) {
	if opts.Offset > 0 {
		if int64(len(execCtx.Items)) > opts.Offset {
			execCtx.Items = execCtx.Items[opts.Offset:]
		} else {
			execCtx.Items = []map[string]interface{}{}
		}
	}
	
	if opts.Limit > 0 && int64(len(execCtx.Items)) > opts.Limit {
		execCtx.Items = execCtx.Items[:opts.Limit]
	}
}

// ExecuteExpression is a convenience function to parse and execute an expression
func (e *Executor) ExecuteExpression(ctx context.Context, listID, expression string, opts *types.QueryOptions) (*types.ResultSet, error) {
	// Parse the expression
	pipe, err := ParseExpression(expression)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	
	// Execute the parsed expression
	return e.Execute(ctx, listID, pipe, opts)
}
