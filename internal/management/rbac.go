package management

import "strings"

var rolePermissions = map[string][]string{
	"SA": {
		"customers:write", "customers:read",
		"campaigns:write", "campaigns:read",
		"settings:write", "settings:read",
		"blacklist:write", "blacklist:read",
		"audit:read",
	},
	"M": {
		"customers:write", "customers:read",
		"campaigns:write", "campaigns:read",
		"audit:read",
	},
	"C": {
		"campaigns:write", "campaigns:read",
		"customers:read",
	},
	"G": {
		"campaigns:read",
	},
}

// GetPermissionsForRole maps high-level user roles into fine-grained capability matrices. Exposing explicit permissions to client applications decouples frontend presentation logic from static backend role definitions.
func GetPermissionsForRole(role string) []string {
	normalized := strings.ToUpper(strings.TrimSpace(role))
	switch normalized {
	case "SUPERADMIN", "ADMIN", "SA":
		normalized = "SA"
	case "MANAGER", "M":
		normalized = "M"
	case "CUSTOMER", "USER", "C":
		normalized = "C"
	case "GUEST", "G":
		normalized = "G"
	}

	perms, exists := rolePermissions[normalized]
	if !exists {
		return []string{}
	}
	return perms
}
