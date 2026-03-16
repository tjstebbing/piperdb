package dsl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tjstebbing/piperdb/internal/dsl"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []dsl.TokenType
	}{
		{
			name:     "Simple filter",
			input:    "@price<100",
			expected: []dsl.TokenType{dsl.AT, dsl.FIELD, dsl.LT, dsl.NUMBER},
		},
		{
			name:     "Pipe with sort",
			input:    "@category=electronics | sort -price",
			expected: []dsl.TokenType{dsl.AT, dsl.FIELD, dsl.EQ, dsl.FIELD, dsl.PIPE, dsl.SORT, dsl.MINUS, dsl.FIELD},
		},
		{
			name:     "Text search",
			input:    "\"hello world\"",
			expected: []dsl.TokenType{dsl.STRING},
		},
		{
			name:     "Map transform",
			input:    "map {name, price}",
			expected: []dsl.TokenType{dsl.MAP, dsl.LBRACE, dsl.FIELD, dsl.COMMA, dsl.FIELD, dsl.RBRACE},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := dsl.NewLexer(tt.input)
			tokens := lexer.TokenizeAll()

			// Remove EOF token for comparison
			var actualTypes []dsl.TokenType
			for _, token := range tokens {
				if token.Type != dsl.EOF {
					actualTypes = append(actualTypes, token.Type)
				}
			}

			assert.Equal(t, tt.expected, actualTypes)
		})
	}
}

func TestParser(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
	}{
		{
			name:      "Simple filter",
			input:     "@price<100",
			expectErr: false,
		},
		{
			name:      "Filter with sort",
			input:     "@category=electronics | sort -price",
			expectErr: false,
		},
		{
			name:      "Text search",
			input:     "\"AI tutorial\"",
			expectErr: false,
		},
		{
			name:      "Map transform",
			input:     "map {name, price}",
			expectErr: false,
		},
		{
			name:      "Select transform",
			input:     "select name price category",
			expectErr: false,
		},
		{
			name:      "Complex pipeline",
			input:     "@category=electronics | @price>100 | sort -price | take 10",
			expectErr: false,
		},
		{
			name:      "Count aggregation",
			input:     "@status=published | count",
			expectErr: false,
		},
		{
			name:      "Group by",
			input:     "group-by category",
			expectErr: false,
		},
		{
			name:      "Invalid syntax",
			input:     "@field | | sort",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipe, err := dsl.ParseExpression(tt.input)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, pipe)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, pipe)
				assert.Greater(t, len(pipe.Stages), 0)

				// Test that we can convert back to string
				str := pipe.String()
				assert.NotEmpty(t, str)

				t.Logf("Parsed: %s -> %s", tt.input, str)
			}
		})
	}
}

func TestFilterStageCreation(t *testing.T) {
	t.Run("Field filter", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("@price<100")
		require.NoError(t, err)
		require.Len(t, pipe.Stages, 1)

		filterStage, ok := pipe.Stages[0].(*dsl.FilterStage)
		require.True(t, ok)
		require.Len(t, filterStage.Conditions, 1)

		condition := filterStage.Conditions[0]
		assert.Equal(t, "price", condition.Path.Simple())
		assert.Equal(t, dsl.OpLessThan, condition.Operator)
		assert.Equal(t, int64(100), condition.Value)
		assert.False(t, condition.Negate)
	})

	t.Run("Text search", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("\"search term\"")
		require.NoError(t, err)
		require.Len(t, pipe.Stages, 1)

		filterStage, ok := pipe.Stages[0].(*dsl.FilterStage)
		require.True(t, ok)
		require.Len(t, filterStage.Conditions, 1)

		condition := filterStage.Conditions[0]
		assert.True(t, condition.Path.IsEmpty()) // Empty path for text search
		assert.Equal(t, dsl.OpContains, condition.Operator)
		assert.Equal(t, "search term", condition.Value)
	})
}

func TestTransformStageCreation(t *testing.T) {
	t.Run("Map transform", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("map {name, price: cost}")
		require.NoError(t, err)
		require.Len(t, pipe.Stages, 1)

		transformStage, ok := pipe.Stages[0].(*dsl.TransformStage)
		require.True(t, ok)
		assert.Equal(t, dsl.TransformMap, transformStage.TransformType)
		require.Len(t, transformStage.Fields, 2)

		// First field: name -> name
		assert.Equal(t, "name", transformStage.Fields[0].Source.Simple())
		assert.Equal(t, "name", transformStage.Fields[0].Target)

		// Second field: price -> cost
		assert.Equal(t, "price", transformStage.Fields[1].Source.Simple())
		assert.Equal(t, "cost", transformStage.Fields[1].Target)
	})

	t.Run("Select transform", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("select name price")
		require.NoError(t, err)
		require.Len(t, pipe.Stages, 1)

		transformStage, ok := pipe.Stages[0].(*dsl.TransformStage)
		require.True(t, ok)
		assert.Equal(t, dsl.TransformSelect, transformStage.TransformType)
		require.Len(t, transformStage.Fields, 2)

		assert.Equal(t, "name", transformStage.Fields[0].Source.Simple())
		assert.Equal(t, "price", transformStage.Fields[1].Source.Simple())
	})
}

func TestSortStageCreation(t *testing.T) {
	pipe, err := dsl.ParseExpression("sort name -price")
	require.NoError(t, err)
	require.Len(t, pipe.Stages, 1)

	sortStage, ok := pipe.Stages[0].(*dsl.SortStage)
	require.True(t, ok)
	require.Len(t, sortStage.Fields, 2)

	// First field: name (ascending)
	assert.Equal(t, "name", sortStage.Fields[0].Path.Simple())
	assert.False(t, sortStage.Fields[0].Descending)

	// Second field: price (descending)
	assert.Equal(t, "price", sortStage.Fields[1].Path.Simple())
	assert.True(t, sortStage.Fields[1].Descending)
}

func TestAggregateStageCreation(t *testing.T) {
	tests := []struct {
		input    string
		expected dsl.AggregateType
		field    string
	}{
		{"count", dsl.AggCount, ""},
		{"sum price", dsl.AggSum, "price"},
		{"avg rating", dsl.AggAvg, "rating"},
		{"min price", dsl.AggMin, "price"},
		{"max price", dsl.AggMax, "price"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			pipe, err := dsl.ParseExpression(tt.input)
			require.NoError(t, err)
			require.Len(t, pipe.Stages, 1)

			aggStage, ok := pipe.Stages[0].(*dsl.AggregateStage)
			require.True(t, ok)
			assert.Equal(t, tt.expected, aggStage.AggregateType)
			assert.Equal(t, tt.field, aggStage.Field.String())
		})
	}
}

func TestSliceStageCreation(t *testing.T) {
	tests := []struct {
		input    string
		expected dsl.SliceType
		amount   int64
	}{
		{"take 10", dsl.SliceTake, 10},
		{"skip 5", dsl.SliceSkip, 5},
		{"first", dsl.SliceFirst, 1},
		{"last", dsl.SliceLast, 1},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			pipe, err := dsl.ParseExpression(tt.input)
			require.NoError(t, err)
			require.Len(t, pipe.Stages, 1)

			sliceStage, ok := pipe.Stages[0].(*dsl.SliceStage)
			require.True(t, ok)
			assert.Equal(t, tt.expected, sliceStage.SliceType)
			assert.Equal(t, tt.amount, sliceStage.Amount)
		})
	}
}

func TestNestedFieldPaths(t *testing.T) {
	t.Run("Dot notation parsing", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("@user.profile.name=Alice")
		require.NoError(t, err)
		require.Len(t, pipe.Stages, 1)

		filterStage, ok := pipe.Stages[0].(*dsl.FilterStage)
		require.True(t, ok)
		condition := filterStage.Conditions[0]

		assert.Equal(t, "user.profile.name", condition.Path.String())
		assert.Len(t, condition.Path.Segments, 3)
		assert.Equal(t, dsl.SegmentField, condition.Path.Segments[0].Type)
		assert.Equal(t, "user", condition.Path.Segments[0].Name)
		assert.Equal(t, "profile", condition.Path.Segments[1].Name)
		assert.Equal(t, "name", condition.Path.Segments[2].Name)
	})

	t.Run("Array index parsing", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("@tags[0]=golang")
		require.NoError(t, err)

		filterStage := pipe.Stages[0].(*dsl.FilterStage)
		condition := filterStage.Conditions[0]

		assert.Equal(t, "tags[0]", condition.Path.String())
		assert.Len(t, condition.Path.Segments, 2)
		assert.Equal(t, dsl.SegmentField, condition.Path.Segments[0].Type)
		assert.Equal(t, dsl.SegmentIndex, condition.Path.Segments[1].Type)
		assert.Equal(t, 0, condition.Path.Segments[1].Index)
	})

	t.Run("Array wildcard parsing", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("@tags[]=golang")
		require.NoError(t, err)

		filterStage := pipe.Stages[0].(*dsl.FilterStage)
		condition := filterStage.Conditions[0]

		assert.Equal(t, "tags[]", condition.Path.String())
		assert.Len(t, condition.Path.Segments, 2)
		assert.Equal(t, dsl.SegmentWildcard, condition.Path.Segments[1].Type)
	})

	t.Run("Nested array field parsing", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("@items[].price>100")
		require.NoError(t, err)

		filterStage := pipe.Stages[0].(*dsl.FilterStage)
		condition := filterStage.Conditions[0]

		assert.Equal(t, "items[].price", condition.Path.String())
		assert.Len(t, condition.Path.Segments, 3)
		assert.Equal(t, dsl.SegmentField, condition.Path.Segments[0].Type)
		assert.Equal(t, dsl.SegmentWildcard, condition.Path.Segments[1].Type)
		assert.Equal(t, dsl.SegmentField, condition.Path.Segments[2].Type)
	})

	t.Run("Sort with nested path", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("sort -user.score")
		require.NoError(t, err)

		sortStage := pipe.Stages[0].(*dsl.SortStage)
		assert.Equal(t, "user.score", sortStage.Fields[0].Path.String())
		assert.True(t, sortStage.Fields[0].Descending)
	})

	t.Run("Select with nested paths", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("select user.name user.email")
		require.NoError(t, err)

		transformStage := pipe.Stages[0].(*dsl.TransformStage)
		assert.Equal(t, "user.name", transformStage.Fields[0].Source.String())
		assert.Equal(t, "user.email", transformStage.Fields[1].Source.String())
	})

	t.Run("Aggregate with nested path", func(t *testing.T) {
		pipe, err := dsl.ParseExpression("avg items[].price")
		require.NoError(t, err)

		aggStage := pipe.Stages[0].(*dsl.AggregateStage)
		assert.Equal(t, "items[].price", aggStage.Field.String())
	})
}

func TestComplexPipeline(t *testing.T) {
	input := "@category=electronics | @price>100 | sort -price | take 5 | select name price"
	
	pipe, err := dsl.ParseExpression(input)
	require.NoError(t, err)
	require.Len(t, pipe.Stages, 5)

	// Verify each stage type
	stages := []dsl.StageType{
		dsl.FilterStageType,
		dsl.FilterStageType,
		dsl.SortStageType,
		dsl.SliceStageType,
		dsl.TransformStageType,
	}

	for i, expectedType := range stages {
		assert.Equal(t, expectedType, pipe.Stages[i].Type(), "Stage %d type mismatch", i+1)
	}

	// Test string representation
	str := pipe.String()
	t.Logf("Pipeline string: %s", str)
	assert.NotEmpty(t, str)
}
