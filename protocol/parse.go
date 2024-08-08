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

// ToRespArray recieves an array of strings and returns a RESP Array
func ToRespArray(arr []string) string {
	respArr := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		respArr += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	}

	return respArr
}

// ToExecRespArray returns a RESP Array just for the EXEC command
func ToExecRespArray(arr []string) string {
	respArr := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		respArr += s
	}

	return respArr
}

// ToBulkString turns a regular string into a bulk string
func ToBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// ToSimpleError turns a string into a RESP simple error
func ToSimpleError(s string) string {
	return fmt.Sprintf("-%s\r\n", s)
}

// ExtractString Extracts the string response from RESP data
// func ExtractString(s string) (string, error) {
// 	firstByte := s[0]

// 	switch firstByte {
// 	case '+', '-', ':':
// 		return s[1 : len(s)-2], nil
// 	case '$':
// 		crlfIndex := strings.Index(s, "\r\n")

// 		return s[crlfIndex+2 : len(s)-2], nil

// 	default:
// 		return "", fmt.Errorf("can't extract string: %s", s)
// 	}
// }
