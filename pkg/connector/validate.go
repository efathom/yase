package connector

import (
	"fmt"
	"regexp"
)

// safeIdentifier matches valid SQL/SOQL identifiers: starts with letter/underscore,
// contains only alphanumerics and underscores, max 64 chars.
var safeIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

// safeSFIdentifier also allows Salesforce custom fields (e.g., "Knowledge__kav").
var safeSFIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}(__[a-zA-Z]{1,3})?$`)

// ValidateSQLIdentifier checks that a string is safe to use as a SQL table or column name.
func ValidateSQLIdentifier(name string) error {
	if !safeIdentifier.MatchString(name) {
		return fmt.Errorf("unsafe SQL identifier: %q", name)
	}
	return nil
}

// ValidateSFIdentifier checks that a string is safe to use as a Salesforce object or field name.
func ValidateSFIdentifier(name string) error {
	if !safeSFIdentifier.MatchString(name) {
		return fmt.Errorf("unsafe Salesforce identifier: %q", name)
	}
	return nil
}
