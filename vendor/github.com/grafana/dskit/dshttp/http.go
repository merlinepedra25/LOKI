package dshttp

import "strings"

// IsRequestBodyTooLarge returns true if the error contains "http: request body too large".
func IsRequestBodyTooLarge(err error) bool {
	return err != nil && strings.Contains(err.Error(), "http: request body too large")
}
