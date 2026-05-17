package management

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPermissionsForRole(t *testing.T) {
	tests := []struct {
		role          string
		expectedPerms []string
	}{
		{"SA", []string{"customers:write", "customers:read", "campaigns:write", "campaigns:read", "settings:write", "settings:read", "blacklist:write", "blacklist:read", "audit:read"}},
		{"admin", []string{"customers:write", "customers:read", "campaigns:write", "campaigns:read", "settings:write", "settings:read", "blacklist:write", "blacklist:read", "audit:read"}},
		{"SuperAdmin", []string{"customers:write", "customers:read", "campaigns:write", "campaigns:read", "settings:write", "settings:read", "blacklist:write", "blacklist:read", "audit:read"}},
		{"M", []string{"customers:write", "customers:read", "campaigns:write", "campaigns:read", "audit:read"}},
		{"manager", []string{"customers:write", "customers:read", "campaigns:write", "campaigns:read", "audit:read"}},
		{"C", []string{"campaigns:write", "campaigns:read", "customers:read"}},
		{"customer", []string{"campaigns:write", "campaigns:read", "customers:read"}},
		{"user", []string{"campaigns:write", "campaigns:read", "customers:read"}},
		{"G", []string{"campaigns:read"}},
		{"guest", []string{"campaigns:read"}},
		{"unknown", []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.role, func(t *testing.T) {
			perms := GetPermissionsForRole(tc.role)
			assert.ElementsMatch(t, tc.expectedPerms, perms)
		})
	}
}
