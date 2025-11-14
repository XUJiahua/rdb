package helper

import (
	"testing"
)

func TestGetFieldFromKey(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		fieldIndexes  []int
		separator     string
		expected      string
	}{
		{
			name:         "single field - first",
			key:          "f1:i00018:5efm9xucrqU:90221900:2024111214",
			fieldIndexes: []int{1},
			separator:    ":",
			expected:     "f1",
		},
		{
			name:         "single field - second",
			key:          "f1:i00018:5efm9xucrqU:90221900:2024111214",
			fieldIndexes: []int{2},
			separator:    ":",
			expected:     "i00018",
		},
		{
			name:         "multiple fields",
			key:          "f1:i00018:5efm9xucrqU:90221900:2024111214",
			fieldIndexes: []int{1, 2},
			separator:    ":",
			expected:     "f1:i00018",
		},
		{
			name:         "multiple fields non-sequential",
			key:          "f1:i00018:5efm9xucrqU:90221900:2024111214",
			fieldIndexes: []int{1, 3, 5},
			separator:    ":",
			expected:     "f1:5efm9xucrqU:2024111214",
		},
		{
			name:         "field index out of range",
			key:          "f1:i00018:abc",
			fieldIndexes: []int{5},
			separator:    ":",
			expected:     "",
		},
		{
			name:         "custom separator",
			key:          "f1-i00018-5efm9xucrqU",
			fieldIndexes: []int{1},
			separator:    "-",
			expected:     "f1",
		},
		{
			name:         "no separator in key",
			key:          "noseparator",
			fieldIndexes: []int{1},
			separator:    ":",
			expected:     "noseparator",
		},
		{
			name:         "no separator - field 2",
			key:          "noseparator",
			fieldIndexes: []int{2},
			separator:    ":",
			expected:     "",
		},
		{
			name:         "empty key",
			key:          "",
			fieldIndexes: []int{1},
			separator:    ":",
			expected:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFieldFromKey(tt.key, tt.fieldIndexes, tt.separator)
			if result != tt.expected {
				t.Errorf("getFieldFromKey(%q, %v, %q) = %q; want %q",
					tt.key, tt.fieldIndexes, tt.separator, result, tt.expected)
			}
		})
	}
}

func TestParseFieldIndexes(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    []int
		expectError bool
	}{
		{
			name:        "single field",
			input:       "1",
			expected:    []int{1},
			expectError: false,
		},
		{
			name:        "multiple fields",
			input:       "1,2,3",
			expected:    []int{1, 2, 3},
			expectError: false,
		},
		{
			name:        "fields with spaces",
			input:       "1, 2, 3",
			expected:    []int{1, 2, 3},
			expectError: false,
		},
		{
			name:        "empty string",
			input:       "",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "invalid number",
			input:       "1,abc,3",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "zero index",
			input:       "0",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "negative index",
			input:       "-1",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "mixed valid and invalid",
			input:       "1,0,3",
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseFieldIndexes(tt.input)
			if tt.expectError {
				if err == nil {
					t.Errorf("ParseFieldIndexes(%q) expected error, got nil", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("ParseFieldIndexes(%q) unexpected error: %v", tt.input, err)
				}
				if len(result) != len(tt.expected) {
					t.Errorf("ParseFieldIndexes(%q) = %v; want %v", tt.input, result, tt.expected)
				} else {
					for i := range result {
						if result[i] != tt.expected[i] {
							t.Errorf("ParseFieldIndexes(%q) = %v; want %v", tt.input, result, tt.expected)
							break
						}
					}
				}
			}
		})
	}
}
