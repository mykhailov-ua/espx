package management

import (
	"fmt"
	"math"
)

const microUnitFactor = 1_000_000

// parseMoneyMicro resolves a monetary amount preferring explicit micro fields over legacy floats.
func parseMoneyMicro(micro *int64, legacy float64, hasLegacy bool, field string) (int64, error) {
	if micro != nil {
		if *micro < 0 {
			return 0, fmt.Errorf("invalid %s", field)
		}
		return *micro, nil
	}
	if hasLegacy {
		if legacy < 0 || math.IsNaN(legacy) || math.IsInf(legacy, 0) {
			return 0, fmt.Errorf("invalid %s", field)
		}
		return int64(math.Round(legacy * microUnitFactor)), nil
	}
	return 0, nil
}

// parseBudgetMicro resolves budget fields that may arrive as budget_micro or budget_limit dollars.
func parseBudgetMicro(micro *int64, legacy float64, hasLegacy bool) (int64, error) {
	if micro != nil {
		if *micro <= 0 {
			return 0, fmt.Errorf("budget must be positive")
		}
		return *micro, nil
	}
	if hasLegacy {
		if legacy <= 0 || math.IsNaN(legacy) || math.IsInf(legacy, 0) {
			return 0, fmt.Errorf("budget must be positive")
		}
		return int64(math.Round(legacy * microUnitFactor)), nil
	}
	return 0, fmt.Errorf("budget is required")
}

// optionalBudgetMicro resolves an optional override budget from micro or legacy fields.
func optionalBudgetMicro(micro *int64, legacy *float64) (*int64, error) {
	if micro != nil {
		if *micro <= 0 {
			return nil, fmt.Errorf("budget must be positive")
		}
		v := *micro
		return &v, nil
	}
	if legacy != nil {
		if *legacy <= 0 || math.IsNaN(*legacy) || math.IsInf(*legacy, 0) {
			return nil, fmt.Errorf("budget must be positive")
		}
		v := int64(math.Round(*legacy * microUnitFactor))
		return &v, nil
	}
	return nil, nil
}
