// Package version provides build version information.
package version

// These variables are set at build time via ldflags.
//
//nolint:gochecknoglobals // Build-time injected variables
var (
	Version   = "0.6.0"
	Commit    = "unknown"
	BuildTime = "unknown"
)
