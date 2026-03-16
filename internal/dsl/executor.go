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
	HasIndex(listID, field string) bool
	IndexEstimate(ctx context.Context, listID, field string, value interface{}) (matches, total int64, err error)
	IndexLookup(ctx context.Context, listID, field string, value interface{}) ([]map[string]interface{}, error)
	IndexLookupIDs(ctx context.Context, listID, field string, value interface{}) ([]string, error)
	FetchItemsByIDs(ctx context.Context, listID string, ids []string) ([]map[string]interface{}, error)
}

// ExecutionContext holds context for query execution
type ExecutionContext struct {
	ListID     string
	Schema     *types.Schema
	Items      []map[string]interface{}
	StartTime  time.Time
	MemoryUsed int64
	IndexHits  int64
}

// NewExecutor creates a new query executor
func NewExecutor(storage StorageInterface) *Executor {
	return &Executor{storage: storage}
}

// Execute executes a parsed pipe expression
func (e *Executor) Execute(ctx context.Context, listID string, pipe *PipeExpr, opts *types.QueryOptions) (*types.ResultSet, error) {
	startTime := time.Now()

	// Get schema for type information
	schema, err := e.storage.GetSchema(ctx, listID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Create execution context
	execCtx := &ExecutionContext{
		ListID:    listID,
		Schema:    schema,
		StartTime: startTime,
	}

	// Query planning: check if the first stage is a filter with an indexed equality condition
	planUsed := "sequential"
	startStage := 0

	if len(pipe.Stages) > 0 {
		if items, usedIndex, remaining := e.tryIndexScan(ctx, listID, pipe.Stages[0]); usedIndex {
			execCtx.Items = items
			execCtx.IndexHits++
			planUsed = "index"
			startStage = 1
			// If the index only resolved some conditions, apply remaining as a filter
			if remaining != nil {
				if err := e.executeStage(ctx, execCtx, remaining); err != nil {
					return nil, fmt.Errorf("stage 1 (post-index filter): %w", err)
				}
			}
		}
	}

	// Fall back to full scan if no index was used
	if startStage == 0 {
		result, err := e.storage.GetItems(ctx, listID, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get items: %w", err)
		}
		execCtx.Items = result.Items
	}

	// Execute remaining stages
	for i := startStage; i < len(pipe.Stages); i++ {
		err := e.executeStage(ctx, execCtx, pipe.Stages[i])
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
		IndexHits:  execCtx.IndexHits,
		MemoryUsed: execCtx.MemoryUsed,
		PlanUsed:   planUsed,
	}

	return finalResult, nil
}

// indexSelectivityThreshold is the maximum fraction of matching items (matches/total)
// at or above which the planner skips the index and falls back to sequential scan.
// At 5% or more, the cost of random-access item lookups approaches a sequential scan.
const indexSelectivityThreshold = 0.05

// indexedCondition tracks a filter condition that has an available index.
type indexedCondition struct {
	index     int   // position in filterStage.Conditions
	field     string
	value     interface{}
	matches   int64
	total     int64
}

// tryIndexScan checks if a stage can be resolved via an index lookup.
// It supports single-index scans and multi-index intersection: when multiple
// equality conditions have indexes but each individually exceeds the selectivity
// threshold, their ID sets are intersected to produce a smaller result.
// Returns the items from the index, whether an index was used, and any
// remaining filter conditions that still need to be applied.
func (e *Executor) tryIndexScan(ctx context.Context, listID string, stage Stage) ([]map[string]interface{}, bool, Stage) {
	filterStage, ok := stage.(*FilterStage)
	if !ok {
		return nil, false, nil
	}

	// Collect all equality conditions that have indexes
	var candidates []indexedCondition
	for i, cond := range filterStage.Conditions {
		if cond.Operator != OpEquals || cond.Negate || cond.Path.IsEmpty() {
			continue
		}
		field := cond.Path.Simple()
		if !e.storage.HasIndex(listID, field) {
			continue
		}
		matches, total, err := e.storage.IndexEstimate(ctx, listID, field, cond.Value)
		if err != nil {
			continue
		}
		candidates = append(candidates, indexedCondition{
			index: i, field: field, value: cond.Value,
			matches: matches, total: total,
		})
	}

	if len(candidates) == 0 {
		return nil, false, nil
	}

	// Try single-index scan: use the most selective candidate if it passes threshold
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.total > 0 && best.total > 0 &&
			float64(c.matches)/float64(c.total) < float64(best.matches)/float64(best.total) {
			best = c
		}
	}

	if best.total > 0 && float64(best.matches)/float64(best.total) < indexSelectivityThreshold {
		// Single index is selective enough
		items, err := e.storage.IndexLookup(ctx, listID, best.field, best.value)
		if err != nil {
			return nil, false, nil
		}
		remaining := e.buildRemainingFilter(filterStage, best.index)
		return items, true, remaining
	}

	// Try multi-index intersection if we have 2+ candidates
	if len(candidates) >= 2 {
		items, usedIdxs, ok := e.tryMultiIndexIntersection(ctx, listID, candidates)
		if ok {
			remaining := e.buildRemainingFilterMulti(filterStage, usedIdxs)
			return items, true, remaining
		}
	}

	return nil, false, nil
}

// tryMultiIndexIntersection intersects ID sets from multiple indexes.
// Returns the fetched items, the condition indexes that were resolved, and success.
func (e *Executor) tryMultiIndexIntersection(ctx context.Context, listID string, candidates []indexedCondition) ([]map[string]interface{}, []int, bool) {
	if len(candidates) < 2 || candidates[0].total == 0 {
		return nil, nil, false
	}

	// Get IDs from first candidate
	ids, err := e.storage.IndexLookupIDs(ctx, listID, candidates[0].field, candidates[0].value)
	if err != nil || len(ids) == 0 {
		return nil, nil, false
	}
	usedIdxs := []int{candidates[0].index}

	// Intersect with each subsequent candidate
	for _, c := range candidates[1:] {
		otherIDs, err := e.storage.IndexLookupIDs(ctx, listID, c.field, c.value)
		if err != nil {
			continue
		}
		ids = intersectSorted(ids, otherIDs)
		usedIdxs = append(usedIdxs, c.index)
		if len(ids) == 0 {
			break
		}
	}

	// Check if the intersection is selective enough
	total := candidates[0].total
	if float64(len(ids))/float64(total) >= indexSelectivityThreshold {
		return nil, nil, false
	}

	// Fetch the intersected items
	items, err := e.storage.FetchItemsByIDs(ctx, listID, ids)
	if err != nil {
		return nil, nil, false
	}

	return items, usedIdxs, true
}

// intersectSorted returns the intersection of two sorted string slices.
func intersectSorted(a, b []string) []string {
	var result []string
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// buildRemainingFilter creates a filter stage with one condition removed.
func (e *Executor) buildRemainingFilter(stage *FilterStage, skipIdx int) Stage {
	remaining := make([]FilterCondition, 0, len(stage.Conditions)-1)
	for j, c := range stage.Conditions {
		if j != skipIdx {
			remaining = append(remaining, c)
		}
	}
	if len(remaining) == 0 {
		return nil
	}
	return &FilterStage{Conditions: remaining, Logic: stage.Logic}
}

// buildRemainingFilterMulti creates a filter stage with multiple conditions removed.
func (e *Executor) buildRemainingFilterMulti(stage *FilterStage, skipIdxs []int) Stage {
	skip := make(map[int]bool, len(skipIdxs))
	for _, idx := range skipIdxs {
		skip[idx] = true
	}
	remaining := make([]FilterCondition, 0, len(stage.Conditions)-len(skipIdxs))
	for j, c := range stage.Conditions {
		if !skip[j] {
			remaining = append(remaining, c)
		}
	}
	if len(remaining) == 0 {
		return nil
	}
	return &FilterStage{Conditions: remaining, Logic: stage.Logic}
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

// resolveFieldPath resolves a field path to all matching values in the item
func resolveFieldPath(data interface{}, path FieldPath) ([]interface{}, bool) {
	if path.IsEmpty() {
		return nil, false
	}

	current := []interface{}{data}

	for _, seg := range path.Segments {
		var next []interface{}
		for _, val := range current {
			switch seg.Type {
			case SegmentField:
				if m, ok := val.(map[string]interface{}); ok {
					if v, exists := m[seg.Name]; exists {
						next = append(next, v)
					}
				}
			case SegmentIndex:
				if arr, ok := val.([]interface{}); ok {
					if seg.Index >= 0 && seg.Index < len(arr) {
						next = append(next, arr[seg.Index])
					}
				}
			case SegmentWildcard:
				if arr, ok := val.([]interface{}); ok {
					next = append(next, arr...)
				}
			}
		}
		if len(next) == 0 {
			return nil, false
		}
		current = next
	}

	return current, true
}

// resolveFieldPathSingle resolves a field path to a single value (first match)
func resolveFieldPathSingle(data interface{}, path FieldPath) (interface{}, bool) {
	values, ok := resolveFieldPath(data, path)
	if !ok || len(values) == 0 {
		return nil, false
	}
	return values[0], true
}

// evaluateFilterCondition evaluates a single filter condition
func (e *Executor) evaluateFilterCondition(item map[string]interface{}, condition FilterCondition) bool {
	// Text search (empty path)
	if condition.Path.IsEmpty() {
		return e.evaluateTextSearch(item, condition)
	}
	
	// Resolve field path
	values, exists := resolveFieldPath(item, condition.Path)
	
	// Handle field existence check
	if condition.Operator == OpExists {
		return exists
	}
	
	if !exists {
		return false
	}
	
	// For wildcard paths (multiple values), match if ANY value satisfies
	for _, val := range values {
		if e.compareValues(val, condition.Operator, condition.Value) {
			return true
		}
	}
	return false
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
		if value, exists := resolveFieldPathSingle(item, field.Source); exists {
			result[field.Target] = value
		}
	}
	
	return result, nil
}

// selectTransform selects only specified fields
func (e *Executor) selectTransform(item map[string]interface{}, fields []FieldSpec) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	
	for _, field := range fields {
		if value, exists := resolveFieldPathSingle(item, field.Source); exists {
			result[field.Target] = value
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
	if value, exists := resolveFieldPathSingle(item, field.Source); exists {
		return map[string]interface{}{field.Target: value}, nil
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
		aVal, aExists := resolveFieldPathSingle(a, field.Path)
		bVal, bExists := resolveFieldPathSingle(b, field.Path)
		
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
func (e *Executor) executeSum(execCtx *ExecutionContext, field FieldPath) error {
	sum := 0.0
	count := 0
	
	for _, item := range execCtx.Items {
		if value, exists := resolveFieldPathSingle(item, field); exists && e.isNumeric(value) {
			sum += e.toFloat64(value)
			count++
		}
	}
	
	execCtx.Items = []map[string]interface{}{
		{"sum": sum, "field": field.String(), "count": count},
	}
	return nil
}

// executeAvg calculates average of numeric values
func (e *Executor) executeAvg(execCtx *ExecutionContext, field FieldPath) error {
	sum := 0.0
	count := 0
	
	for _, item := range execCtx.Items {
		if value, exists := resolveFieldPathSingle(item, field); exists && e.isNumeric(value) {
			sum += e.toFloat64(value)
			count++
		}
	}
	
	avg := 0.0
	if count > 0 {
		avg = sum / float64(count)
	}
	
	execCtx.Items = []map[string]interface{}{
		{"avg": avg, "field": field.String(), "count": count},
	}
	return nil
}

// executeMin finds minimum value
func (e *Executor) executeMin(execCtx *ExecutionContext, field FieldPath) error {
	var min interface{}
	found := false
	
	for _, item := range execCtx.Items {
		if value, exists := resolveFieldPathSingle(item, field); exists {
			if !found || e.compareForSort(value, min) < 0 {
				min = value
				found = true
			}
		}
	}
	
	result := map[string]interface{}{"field": field.String()}
	if found {
		result["min"] = min
	} else {
		result["min"] = nil
	}
	
	execCtx.Items = []map[string]interface{}{result}
	return nil
}

// executeMax finds maximum value
func (e *Executor) executeMax(execCtx *ExecutionContext, field FieldPath) error {
	var max interface{}
	found := false
	
	for _, item := range execCtx.Items {
		if value, exists := resolveFieldPathSingle(item, field); exists {
			if !found || e.compareForSort(value, max) > 0 {
				max = value
				found = true
			}
		}
	}
	
	result := map[string]interface{}{"field": field.String()}
	if found {
		result["max"] = max
	} else {
		result["max"] = nil
	}
	
	execCtx.Items = []map[string]interface{}{result}
	return nil
}

// executeGroupBy groups items by specified fields
func (e *Executor) executeGroupBy(execCtx *ExecutionContext, groupFields []FieldPath) error {
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
func (e *Executor) buildGroupKey(item map[string]interface{}, fields []FieldPath) string {
	var keyParts []string
	
	for _, field := range fields {
		if value, exists := resolveFieldPathSingle(item, field); exists {
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
