# geoarrow-go
GeoArrow Extension Types for https://github.com/apache/arrow-go

## Development

Conformance tests read the reference example files from
[geoarrow-data](https://github.com/geoarrow/geoarrow-data). They are committed
under `testdata/geoarrow-data` (see the README there for the source and
licensing), so `go test ./...` needs no setup.

To refresh or extend them, run `go generate ./...`, or
`go run ./internal/fetchtestdata -types point,polygon -force`; see
`internal/fetchtestdata` for the flags.
