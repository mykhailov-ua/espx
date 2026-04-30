package ads

import "time"

var (
	MaxRetries  = 3
	InitialWait = 100 * time.Millisecond
	MaxWait     = 2 * time.Second
)
