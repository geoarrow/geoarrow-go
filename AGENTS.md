This repository provides a go implementation of geoarrow. The geoarrow specification can be found here: https://github.com/geoarrow/geoarrow

## Go Arrow Ecosystem Interop

geoarrow-go has the following relationship with the wider go arrow/parquet/iceberg ecosystem. It is vital to maintain compatibility with these other libraries. If there is a breaking change, explicitly raise it for confirmation before changing it.

- geoarrow-go
    - Defines the geoarrow Arrow extension type and maps it to and from the Parquet GEOMETRY/GEOGRAPHY logical types, including CRS and edges
- arrow-go
    - Defines the Parquet GEOMETRY/GEOGRAPHY logical types
    - Provides the pqarrow hooks that let an extension type pick its Parquet logical type on write and read
    - Does not import geoarrow-go; contains some logic for calculating geospatial Parquet row group stats
- iceberg-go
    - Imports geoarrow-go and is one of geoarrow-go's key downstream users. It is advisible to check out iceberg-go and run its test suite using `replace` in the `go.mod` to verify any new geoarrow-go changes will not break anything.
    - Maps Iceberg GEOMETRY/GEOGRAPHY types and their CRS to the geoarrow Arrow extension type
    - Computes the Iceberg manifest bounds and null counts for geo columns from the Arrow data, not from Parquet stats

## Geoarrow Ecosystem Interop

When making changes to geoarrow-go, if there is a behavior that is ambiguous or undefined in the geoarrow specification it is best to defer to interoperability and follow other the practices of other mature implementations like https://github.com/apache/arrow-rs and https://github.com/geoarrow/geoarrow-rs as well as GDAL's geoarrow driver: https://gdal.org/en/stable/drivers/vector/arrow.html

## Testing Changes

- When changing anything related to Parquet write behavior (logical type selection, CRS, edges, metadata), also test the round trip for both directions: write Arrow to Parquet and read it back, and read Parquet and write it back. Confirm the extension type, CRS, and edges survive unchanged, or change in only the intended, documented ways. Cover edge cases like an unset / null CRS.
- All changes must pass the test suite and golangci-lint via precommit
