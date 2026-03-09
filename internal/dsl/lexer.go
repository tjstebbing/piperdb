package dsl

import (
	"fmt"
	"strings"
)

// Lexer tokenizes DSL expressions
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
	line         int  // current line number
	column       int  // current column number
}

// NewLexer creates a new lexer instance
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar()
	return l
}

// readChar reads the next character and advances position
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // ASCII NUL represents EOF
	} else {
		l.ch = l.input[l.readPosition]
	}
	
	if l.ch == '\n' {
		l.line++
		l.column = 0
	} else {
		l.column++
	}
	
	l.position = l.readPosition
	l.readPosition++
}

// peekChar returns the next character without advancing position
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// NextToken scans the input and returns the next token
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case '|':
		tok = l.newToken(PIPE, string(l.ch))
	case ':':
		tok = l.newToken(COLON, string(l.ch))
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(LTE, string(ch)+string(l.ch))
		} else {
			tok = l.newToken(LT, string(l.ch))
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(GTE, string(ch)+string(l.ch))
		} else {
			tok = l.newToken(GT, string(l.ch))
		}
	case '=':
		tok = l.newToken(EQ, string(l.ch))
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(NEQ, string(ch)+string(l.ch))
		} else {
			tok = l.newToken(ILLEGAL, string(l.ch))
		}
	case '~':
		tok = l.newToken(MATCH, string(l.ch))
	case '^':
		tok = l.newToken(PREFIX, string(l.ch))
	case '$':
		tok = l.newToken(SUFFIX, string(l.ch))
	case '{':
		tok = l.newToken(LBRACE, string(l.ch))
	case '}':
		tok = l.newToken(RBRACE, string(l.ch))
	case '(':
		tok = l.newToken(LPAREN, string(l.ch))
	case ')':
		tok = l.newToken(RPAREN, string(l.ch))
	case ',':
		tok = l.newToken(COMMA, string(l.ch))
	case '-':
		tok = l.newToken(MINUS, string(l.ch))
	case '.':
		tok = l.newToken(DOT, string(l.ch))
	case '@':
		tok = l.newToken(AT, string(l.ch))
	case '"':
		tok.Type = STRING
		tok.Value = l.readString()
	case '\'':
		tok.Type = STRING
		tok.Value = l.readSingleQuotedString()
	case 0:
		tok = l.newToken(EOF, "")
	default:
		if isLetter(l.ch) {
			tok.Value = l.readIdentifier()
			tok.Type = LookupIdent(tok.Value)
			tok.Position = l.position - len(tok.Value)
			tok.Line = l.line
			tok.Column = l.column - len(tok.Value)
			return tok
		} else if isDigit(l.ch) {
			tok.Type = NUMBER
			tok.Value = l.readNumber()
			tok.Position = l.position - len(tok.Value)
			tok.Line = l.line
			tok.Column = l.column - len(tok.Value)
			return tok
		} else {
			tok = l.newToken(ILLEGAL, string(l.ch))
		}
	}

	l.readChar()
	return tok
}

// newToken creates a new token with position information
func (l *Lexer) newToken(tokenType TokenType, value string) Token {
	return Token{
		Type:     tokenType,
		Value:    value,
		Position: l.position,
		Line:     l.line,
		Column:   l.column,
	}
}

// readIdentifier reads an identifier (field name, keyword)
func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' || l.ch == '-' {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber reads a number (integer or float)
func (l *Lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	
	// Check for decimal point
	if l.ch == '.' && isDigit(l.peekChar()) {
		l.readChar() // consume '.'
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	
	return l.input[position:l.position]
}

// readString reads a double-quoted string
func (l *Lexer) readString() string {
	position := l.position + 1 // skip opening quote
	for {
		l.readChar()
		if l.ch == '"' || l.ch == 0 {
			break
		}
		// Handle escape sequences
		if l.ch == '\\' {
			l.readChar() // skip escaped character
		}
	}
	return l.input[position:l.position]
}

// readSingleQuotedString reads a single-quoted string
func (l *Lexer) readSingleQuotedString() string {
	position := l.position + 1 // skip opening quote
	for {
		l.readChar()
		if l.ch == '\'' || l.ch == 0 {
			break
		}
		// Handle escape sequences
		if l.ch == '\\' {
			l.readChar() // skip escaped character
		}
	}
	return l.input[position:l.position]
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// Helper functions

// isLetter checks if character is a letter
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

// isDigit checks if character is a digit
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// TokenizeAll returns all tokens from the input
func (l *Lexer) TokenizeAll() []Token {
	var tokens []Token
	
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == EOF {
			break
		}
	}
	
	return tokens
}

// Preview returns the next N tokens without consuming them
func (l *Lexer) Preview(n int) []Token {
	// Save current state
	savedInput := l.input
	savedPosition := l.position
	savedReadPosition := l.readPosition
	savedCh := l.ch
	savedLine := l.line
	savedColumn := l.column
	
	// Read N tokens
	var tokens []Token
	for i := 0; i < n; i++ {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == EOF {
			break
		}
	}
	
	// Restore state
	l.input = savedInput
	l.position = savedPosition
	l.readPosition = savedReadPosition
	l.ch = savedCh
	l.line = savedLine
	l.column = savedColumn
	
	return tokens
}

// ValidateExpression performs basic syntax validation
func (l *Lexer) ValidateExpression() error {
	tokens := l.TokenizeAll()
	
	// Basic validation rules
	if len(tokens) == 0 || (len(tokens) == 1 && tokens[0].Type == EOF) {
		return fmt.Errorf("empty expression")
	}
	
	// Check for illegal tokens
	for _, token := range tokens {
		if token.Type == ILLEGAL {
			return fmt.Errorf("illegal character '%s' at line %d, column %d", 
				token.Value, token.Line, token.Column)
		}
	}
	
	return nil
}

// String returns string representation of all tokens (for debugging)
func (l *Lexer) String() string {
	tokens := l.TokenizeAll()
	var parts []string
	
	for _, tok := range tokens {
		if tok.Type != EOF {
			parts = append(parts, tok.String())
		}
	}
	
	return "[" + strings.Join(parts, ", ") + "]"
}
