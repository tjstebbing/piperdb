package dsl

import "fmt"

// TokenType represents the type of a token
type TokenType int

const (
	// Literals
	FIELD TokenType = iota
	NUMBER
	STRING
	BOOLEAN
	
	// Operators
	PIPE       // |
	COLON      // :
	LT         // <
	GT         // >
	LTE        // <=
	GTE        // >=
	EQ         // = (for explicit equality)
	NEQ        // !=
	MATCH      // ~ (regex/fuzzy match)
	PREFIX     // ^ (starts with)
	SUFFIX     // $ (ends with)
	
	// Keywords
	MAP
	SELECT
	PLUCK
	SORT
	COUNT
	SUM
	AVG
	MIN
	MAX
	GROUP_BY
	TAKE
	SKIP
	FIRST
	LAST
	UNION
	DIFF
	INTERSECT
	AND
	OR
	NOT
	
	// Symbols
	LBRACE     // {
	RBRACE     // }
	LBRACKET   // [
	RBRACKET   // ]
	LPAREN     // (
	RPAREN     // )
	COMMA      // ,
	MINUS      // -
	DOT        // .
	AT         // @
	
	// Special
	EOF
	ILLEGAL
	WHITESPACE
)

// Token represents a single token
type Token struct {
	Type     TokenType
	Value    string
	Position int
	Line     int
	Column   int
}

// String returns a string representation of the token type
func (tt TokenType) String() string {
	switch tt {
	case FIELD:
		return "FIELD"
	case NUMBER:
		return "NUMBER"
	case STRING:
		return "STRING"
	case BOOLEAN:
		return "BOOLEAN"
	case PIPE:
		return "PIPE"
	case COLON:
		return ":"
	case LT:
		return "<"
	case GT:
		return ">"
	case LTE:
		return "<="
	case GTE:
		return ">="
	case EQ:
		return "="
	case NEQ:
		return "!="
	case MATCH:
		return "~"
	case PREFIX:
		return "^"
	case SUFFIX:
		return "$"
	case MAP:
		return "MAP"
	case SELECT:
		return "SELECT"
	case PLUCK:
		return "PLUCK"
	case SORT:
		return "SORT"
	case COUNT:
		return "COUNT"
	case SUM:
		return "SUM"
	case AVG:
		return "AVG"
	case MIN:
		return "MIN"
	case MAX:
		return "MAX"
	case GROUP_BY:
		return "GROUP_BY"
	case TAKE:
		return "TAKE"
	case SKIP:
		return "SKIP"
	case FIRST:
		return "FIRST"
	case LAST:
		return "LAST"
	case UNION:
		return "UNION"
	case DIFF:
		return "DIFF"
	case INTERSECT:
		return "INTERSECT"
	case AND:
		return "AND"
	case OR:
		return "OR"
	case NOT:
		return "NOT"
	case LBRACE:
		return "{"
	case RBRACE:
		return "}"
	case LBRACKET:
		return "["
	case RBRACKET:
		return "]"
	case LPAREN:
		return "("
	case RPAREN:
		return ")"
	case COMMA:
		return ","
	case MINUS:
		return "-"
	case DOT:
		return "."
	case AT:
		return "@"
	case EOF:
		return "EOF"
	case ILLEGAL:
		return "ILLEGAL"
	case WHITESPACE:
		return "WHITESPACE"
	default:
		return "UNKNOWN"
	}
}

// String returns a string representation of the token
func (t Token) String() string {
	if t.Value == "" {
		return fmt.Sprintf("%s", t.Type)
	}
	return fmt.Sprintf("%s(%s)", t.Type, t.Value)
}

// Keywords map for identifying keywords
var keywords = map[string]TokenType{
	"map":       MAP,
	"select":    SELECT,
	"pluck":     PLUCK,
	"sort":      SORT,
	"count":     COUNT,
	"sum":       SUM,
	"avg":       AVG,
	"min":       MIN,
	"max":       MAX,
	"group-by":  GROUP_BY,
	"take":      TAKE,
	"skip":      SKIP,
	"first":     FIRST,
	"last":      LAST,
	"union":     UNION,
	"diff":      DIFF,
	"intersect": INTERSECT,
	"and":       AND,
	"or":        OR,
	"not":       NOT,
	"true":      BOOLEAN,
	"false":     BOOLEAN,
}

// LookupIdent checks if an identifier is a keyword
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return FIELD
}

// IsKeyword checks if a token type is a keyword
func (tt TokenType) IsKeyword() bool {
	return tt >= MAP && tt <= NOT
}

// IsOperator checks if a token type is an operator
func (tt TokenType) IsOperator() bool {
	return tt >= PIPE && tt <= SUFFIX
}

// IsLiteral checks if a token type is a literal
func (tt TokenType) IsLiteral() bool {
	return tt >= FIELD && tt <= BOOLEAN
}
