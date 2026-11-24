package geoarrow

// Refreshes the committed GeoArrow reference files under
// testdata/geoarrow-data (from geoarrow-data) that the conformance tests read.
// Files already present are skipped, so this is a no-op unless the pinned tag
// or file list in internal/fetchtestdata changes. Pass -types to fetch more
// geometry types and -force to re-download everything.
//
//go:generate go run ./internal/fetchtestdata -out testdata/geoarrow-data
