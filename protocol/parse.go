package protocol

import (
	"fmt"
	"strconv"
)

// GetArrayLength returns the length of the given array.
func GetArrayLength(s string) (int, error) {
	if s[0] == '*' {
		i, err := strconv.Atoi(s[1:])
		if err != nil {
			return 0, fmt.Errorf("Atoi failed: %v", err)
		}

		return i, nil
	}

	return 0, fmt.Errorf("* not found: %v", s[0])
}

// GetBulkStringLength returns the length of the given bulk string.
func GetBulkStringLength(s string) (int, error) {
	if s[0] == '$' {
		i, err := strconv.Atoi(s[1:])
		if err != nil {
			return 0, fmt.Errorf("Atoi failed: %v", err)
		}

		return i, nil
	}

	return 0, fmt.Errorf("$ not found: %v", s[0])
}

// VerifyBulkStringLength verifies the length of the bulk string.
func VerifyBulkStringLength(s string, length int) error {
	if len(s) != length {
		return fmt.Errorf("Expected data length: %d Actual data length: %d", length, len(s))
	}

	return nil
}
