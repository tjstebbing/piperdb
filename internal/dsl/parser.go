package dsl

import (
	"fmt"
	"strconv"
)

// Parser converts tokens into an AST
type Parser struct {
	lexer  *Lexer
	tokens []Token
	pos    int
	errors []string
}

// NewParser creates a new parser
func NewParser(input string) *Parser {
	lexer := NewLexer(input)
	tokens := lexer.TokenizeAll()
	
	return &Parser{
		lexer:  lexer,
		tokens: tokens,
		pos:    0,
		errors: []string{},
	}
}

// Parse parses the input and returns a PipeExpr
func (p *Parser) Parse() (*PipeExpr, error) {
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parser errors: %v", p.errors)
	}
	
	pipe := &PipeExpr{Stages: []Stage{}}
	
	// Parse stages separated by pipes
	for !p.isAtEnd() {
		if p.currentToken().Type == EOF {
			break
		}
		
		stage, err := p.parseStage()
		if err != nil {
			return nil, err
		}
		
		if stage != nil {
			pipe.Stages = append(pipe.Stages, stage)
		}
		
		// Expect pipe separator or EOF
		if !p.isAtEnd() && p.currentToken().Type != EOF {
			if !p.match(PIPE) {
				if p.currentToken().Type == EOF {
					break
				}
				return nil, fmt.Errorf("expected '|' after stage, got %s", p.currentToken().Type)
			}
		}
	}
	
	if err := pipe.Validate(); err != nil {
		return nil, err
	}
	
	return pipe, nil
}

// parseStage parses a single stage
func (p *Parser) parseStage() (Stage, error) {
	token := p.currentToken()
	
	switch token.Type {
	case AT:
		return p.parseFilterStage()
	case STRING:
		return p.parseTextSearchStage()
	case MAP, SELECT, PLUCK:
		return p.parseTransformStage()
	case SORT:
		return p.parseSortStage()
	case COUNT, SUM, AVG, MIN, MAX, GROUP_BY:
		return p.parseAggregateStage()
	case TAKE, SKIP, FIRST, LAST:
		return p.parseSliceStage()
	case UNION, DIFF, INTERSECT:
		return p.parseSetOpStage()
	default:
		return nil, fmt.Errorf("unexpected token %s at start of stage", token.Type)
	}
}

// parseFilterStage parses filter expressions like @field:value
func (p *Parser) parseFilterStage() (Stage, error) {
	conditions := []FilterCondition{}
	
	for {
		// Parse single condition
		condition, err := p.parseFilterCondition()
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, condition)
		
		// Check for logic operators or implicit AND (adjacent @filters)
		if p.match(AND) {
			continue // Parse next condition with AND
		} else if p.match(OR) {
			// For now, treat all as AND for simplicity
			// TODO: Handle mixed AND/OR with precedence
			continue
		} else if p.check(AT) {
			continue // Implicit AND: @field:val @field2:val2
		} else {
			break
		}
	}
	
	return &FilterStage{
		Conditions: conditions,
		Logic:      LogicAnd,
	}, nil
}

// parseFilterCondition parses a single filter condition
func (p *Parser) parseFilterCondition() (FilterCondition, error) {
	negate := false
	if p.match(NOT) {
		negate = true
	}
	
	if !p.match(AT) {
		return FilterCondition{}, fmt.Errorf("expected '@' at start of filter condition")
	}
	
	if p.currentToken().Type != FIELD {
		return FilterCondition{}, fmt.Errorf("expected field name after '@'")
	}
	
	path := p.parseFieldPath()
	
	// Parse operator
	op, err := p.parseFilterOperator()
	if err != nil {
		return FilterCondition{}, err
	}
	
	// Parse value
	value, err := p.parseValue()
	if err != nil {
		return FilterCondition{}, err
	}
	
	return FilterCondition{
		Path:     path,
		Operator: op,
		Value:    value,
		Negate:   negate,
	}, nil
}

// parseFieldPath parses a field path like user.profile.name, tags[0], items[].price
func (p *Parser) parseFieldPath() FieldPath {
	path := FieldPath{}
	
	// First segment must be a field name
	if p.currentToken().Type != FIELD {
		return path
	}
	path.Segments = append(path.Segments, PathSegment{Type: SegmentField, Name: p.advance().Value})
	
	// Continue parsing dot access and bracket access
	for {
		if p.check(DOT) {
			p.advance() // consume '.'
			if p.currentToken().Type != FIELD {
				break
			}
			path.Segments = append(path.Segments, PathSegment{Type: SegmentField, Name: p.advance().Value})
		} else if p.check(LBRACKET) {
			p.advance() // consume '['
			if p.check(RBRACKET) {
				// Wildcard: []
				p.advance() // consume ']'
				path.Segments = append(path.Segments, PathSegment{Type: SegmentWildcard})
			} else if p.currentToken().Type == NUMBER {
				// Index: [N]
				idxTok := p.advance()
				idx, _ := strconv.ParseInt(idxTok.Value, 10, 64)
				if !p.match(RBRACKET) {
					break
				}
				path.Segments = append(path.Segments, PathSegment{Type: SegmentIndex, Index: int(idx)})
			} else {
				break
			}
		} else {
			break
		}
	}
	
	return path
}

// parseFilterOperator parses filter operators
func (p *Parser) parseFilterOperator() (FilterOp, error) {
	token := p.currentToken()
	
	switch token.Type {
	case EQ:
		p.advance()
		return OpEquals, nil
	case LTE:
		p.advance()
		return OpLessThanEqual, nil
	case GTE:
		p.advance()
		return OpGreaterThanEqual, nil
	case LT:
		p.advance()
		return OpLessThan, nil
	case GT:
		p.advance()
		return OpGreaterThan, nil
	case NEQ:
		p.advance()
		return OpNotEquals, nil
	case MATCH:
		p.advance()
		return OpMatch, nil
	case PREFIX:
		p.advance()
		return OpPrefix, nil
	case SUFFIX:
		p.advance()
		return OpSuffix, nil
	default:
		return OpEquals, fmt.Errorf("expected operator, got %s", token.Type)
	}
}

// parseValue parses a value (string, number, boolean)
func (p *Parser) parseValue() (interface{}, error) {
	token := p.currentToken()
	
	switch token.Type {
	case STRING:
		p.advance()
		return token.Value, nil
	case NUMBER:
		p.advance()
		// Try to parse as int first, then float
		if val, err := strconv.ParseInt(token.Value, 10, 64); err == nil {
			return val, nil
		}
		if val, err := strconv.ParseFloat(token.Value, 64); err == nil {
			return val, nil
		}
		return token.Value, nil // Fallback to string
	case BOOLEAN:
		p.advance()
		return token.Value == "true", nil
	case FIELD:
		p.advance()
		return token.Value, nil
	default:
		return nil, fmt.Errorf("expected value, got %s", token.Type)
	}
}

// parseTextSearchStage parses text search like "search term"
func (p *Parser) parseTextSearchStage() (Stage, error) {
	if p.currentToken().Type != STRING {
		return nil, fmt.Errorf("expected string for text search")
	}
	
	searchText := p.advance().Value
	
	condition := FilterCondition{
		Path:     FieldPath{}, // Empty path for text search
		Operator: OpContains,
		Value:    searchText,
		Negate:   false,
	}
	
	return &FilterStage{
		Conditions: []FilterCondition{condition},
		Logic:      LogicAnd,
	}, nil
}

// parseTransformStage parses transformation stages
func (p *Parser) parseTransformStage() (Stage, error) {
	token := p.advance()
	
	switch token.Type {
	case MAP:
		return p.parseMapTransform()
	case SELECT:
		return p.parseSelectTransform()
	case PLUCK:
		return p.parsePluckTransform()
	default:
		return nil, fmt.Errorf("unexpected transform type %s", token.Type)
	}
}

// parseMapTransform parses map { field1, field2: new_name }
func (p *Parser) parseMapTransform() (Stage, error) {
	if !p.match(LBRACE) {
		return nil, fmt.Errorf("expected '{' after 'map'")
	}
	
	fields := []FieldSpec{}
	
	for !p.check(RBRACE) && !p.isAtEnd() {
		field, err := p.parseFieldSpec()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
		
		if !p.match(COMMA) {
			break
		}
	}
	
	if !p.match(RBRACE) {
		return nil, fmt.Errorf("expected '}' after map fields")
	}
	
	return &TransformStage{
		TransformType: TransformMap,
		Fields:        fields,
	}, nil
}

// parseSelectTransform parses select field1 field2
func (p *Parser) parseSelectTransform() (Stage, error) {
	fields := []FieldSpec{}
	
	for p.currentToken().Type == FIELD {
		path := p.parseFieldPath()
		fields = append(fields, FieldSpec{Source: path, Target: path.Simple()})
	}
	
	if len(fields) == 0 {
		return nil, fmt.Errorf("select requires at least one field")
	}
	
	return &TransformStage{
		TransformType: TransformSelect,
		Fields:        fields,
	}, nil
}

// parsePluckTransform parses pluck field
func (p *Parser) parsePluckTransform() (Stage, error) {
	if p.currentToken().Type != FIELD {
		return nil, fmt.Errorf("pluck requires a field name")
	}
	
	path := p.parseFieldPath()
	
	return &TransformStage{
		TransformType: TransformPluck,
		Fields:        []FieldSpec{{Source: path, Target: path.Simple()}},
	}, nil
}

// parseFieldSpec parses field specifications for map
func (p *Parser) parseFieldSpec() (FieldSpec, error) {
	if p.currentToken().Type != FIELD {
		return FieldSpec{}, fmt.Errorf("expected field name")
	}
	
	source := p.parseFieldPath()
	target := source.Simple()
	
	// Check for rename: source: target
	if p.match(COLON) {
		if p.currentToken().Type != FIELD {
			return FieldSpec{}, fmt.Errorf("expected target field name after ':'")
		}
		target = p.advance().Value
	}
	
	return FieldSpec{Source: source, Target: target}, nil
}

// parseSortStage parses sort field1 -field2
func (p *Parser) parseSortStage() (Stage, error) {
	p.advance() // consume SORT
	
	fields := []SortField{}
	
	for p.currentToken().Type == FIELD || p.currentToken().Type == MINUS {
		descending := false
		
		if p.match(MINUS) {
			descending = true
		}
		
		if p.currentToken().Type != FIELD {
			return nil, fmt.Errorf("expected field name in sort")
		}
		
		path := p.parseFieldPath()
		fields = append(fields, SortField{Path: path, Descending: descending})
	}
	
	if len(fields) == 0 {
		return nil, fmt.Errorf("sort requires at least one field")
	}
	
	return &SortStage{Fields: fields}, nil
}

// parseAggregateStage parses aggregation operations
func (p *Parser) parseAggregateStage() (Stage, error) {
	token := p.advance()
	
	switch token.Type {
	case COUNT:
		return &AggregateStage{AggregateType: AggCount}, nil
	case SUM:
		field, err := p.parseAggregateField()
		if err != nil {
			return nil, err
		}
		return &AggregateStage{AggregateType: AggSum, Field: field}, nil
	case AVG:
		field, err := p.parseAggregateField()
		if err != nil {
			return nil, err
		}
		return &AggregateStage{AggregateType: AggAvg, Field: field}, nil
	case MIN:
		field, err := p.parseAggregateField()
		if err != nil {
			return nil, err
		}
		return &AggregateStage{AggregateType: AggMin, Field: field}, nil
	case MAX:
		field, err := p.parseAggregateField()
		if err != nil {
			return nil, err
		}
		return &AggregateStage{AggregateType: AggMax, Field: field}, nil
	case GROUP_BY:
		fields, err := p.parseGroupByFields()
		if err != nil {
			return nil, err
		}
		return &AggregateStage{AggregateType: AggGroupBy, GroupBy: fields}, nil
	default:
		return nil, fmt.Errorf("unexpected aggregate type %s", token.Type)
	}
}

// parseAggregateField parses field path for aggregation
func (p *Parser) parseAggregateField() (FieldPath, error) {
	if p.currentToken().Type != FIELD {
		return FieldPath{}, fmt.Errorf("expected field name for aggregation")
	}
	return p.parseFieldPath(), nil
}

// parseGroupByFields parses multiple fields for group-by
func (p *Parser) parseGroupByFields() ([]FieldPath, error) {
	var fields []FieldPath
	
	for p.currentToken().Type == FIELD {
		fields = append(fields, p.parseFieldPath())
	}
	
	if len(fields) == 0 {
		return nil, fmt.Errorf("group-by requires at least one field")
	}
	
	return fields, nil
}

// parseSliceStage parses slice operations
func (p *Parser) parseSliceStage() (Stage, error) {
	token := p.advance()
	
	switch token.Type {
	case TAKE:
		amount, err := p.parseNumber()
		if err != nil {
			return nil, fmt.Errorf("take requires a number: %w", err)
		}
		return &SliceStage{SliceType: SliceTake, Amount: amount}, nil
	case SKIP:
		amount, err := p.parseNumber()
		if err != nil {
			return nil, fmt.Errorf("skip requires a number: %w", err)
		}
		return &SliceStage{SliceType: SliceSkip, Amount: amount}, nil
	case FIRST:
		return &SliceStage{SliceType: SliceFirst, Amount: 1}, nil
	case LAST:
		return &SliceStage{SliceType: SliceLast, Amount: 1}, nil
	default:
		return nil, fmt.Errorf("unexpected slice type %s", token.Type)
	}
}

// parseNumber parses a number token
func (p *Parser) parseNumber() (int64, error) {
	if p.currentToken().Type != NUMBER {
		return 0, fmt.Errorf("expected number")
	}
	
	token := p.advance()
	value, err := strconv.ParseInt(token.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %s", token.Value)
	}
	
	return value, nil
}

// parseSetOpStage parses set operations (future)
func (p *Parser) parseSetOpStage() (Stage, error) {
	token := p.advance()
	
	if p.currentToken().Type != FIELD {
		return nil, fmt.Errorf("expected list name after %s", token.Type)
	}
	
	listName := p.advance().Value
	
	var op SetOperation
	switch token.Type {
	case UNION:
		op = SetUnion
	case DIFF:
		op = SetDiff
	case INTERSECT:
		op = SetIntersect
	default:
		return nil, fmt.Errorf("unexpected set operation %s", token.Type)
	}
	
	return &SetOpStage{Operation: op, OtherList: listName}, nil
}

// Helper methods

// currentToken returns the current token
func (p *Parser) currentToken() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: EOF}
	}
	return p.tokens[p.pos]
}

// advance consumes and returns the current token
func (p *Parser) advance() Token {
	token := p.currentToken()
	if !p.isAtEnd() {
		p.pos++
	}
	return token
}

// match checks if current token matches any of the given types
func (p *Parser) match(types ...TokenType) bool {
	for _, tokenType := range types {
		if p.check(tokenType) {
			p.advance()
			return true
		}
	}
	return false
}

// check returns true if current token is of the given type
func (p *Parser) check(tokenType TokenType) bool {
	if p.isAtEnd() {
		return false
	}
	return p.currentToken().Type == tokenType
}

// isAtEnd returns true if we're at the end of tokens
func (p *Parser) isAtEnd() bool {
	return p.pos >= len(p.tokens) || p.currentToken().Type == EOF
}

// error records a parse error
func (p *Parser) error(message string) {
	p.errors = append(p.errors, message)
}

// GetErrors returns all parse errors
func (p *Parser) GetErrors() []string {
	return p.errors
}

// ParseExpression is a convenience function to parse an expression
func ParseExpression(input string) (*PipeExpr, error) {
	parser := NewParser(input)
	return parser.Parse()
}
