# GeoArrow reference test data

Example files from [geoarrow/geoarrow-data](https://github.com/geoarrow/geoarrow-data)
at tag `v0.2.0`, read by `geoarrowdata_test.go`. Don't edit them by hand.

- `example_<type><dim>[_interleaved|_wkt|_wkb].arrows`: Arrow IPC streams for
  every geometry type (point, linestring, polygon, multipoint,
  multilinestring, multipolygon), dimension (XY, `-z`, `-m`, `-zm`) and
  encoding. Each has a `wkt` column (the source text) and a `geometry` column.
- `example_<type><dim>.tsv`: the same source WKT, one row per line after a
  header. A blank line is a null row.
- `natural-earth_countries-bounds_box.arrows`: country bounds as
  `geoarrow.box`, including boxes that cross the antimeridian. Natural Earth
  is public domain.

To refresh or extend these, change the tag or file list in
`internal/fetchtestdata` and run `go generate ./...` (add `-force` via
`go run ./internal/fetchtestdata -force` to re-download existing files).
