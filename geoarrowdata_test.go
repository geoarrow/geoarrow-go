package geoarrow_test

import (
	"bytes"
	stdjson "encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/geoarrow/geoarrow-go"
	"github.com/stretchr/testify/require"
)

// Conformance tests against the reference example files published in
// https://github.com/geoarrow/geoarrow-data, which other GeoArrow
// implementations (geoarrow-c, geoarrow-rs, ...) are tested against too.
//
// The files are committed under testdata/geoarrow-data; see the README there
// for their source and how to refresh them.

const geoarrowDataDir = "testdata/geoarrow-data"

// dataFile returns the path of a committed example file, failing if it is
// missing.
func dataFile(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(geoarrowDataDir, name)
	_, err := os.Stat(path)
	require.NoError(t, err, "missing reference file %s: see testdata/geoarrow-data/README.md", path)
	return path
}

// readArrowsColumn reads the "geometry" column (the files also carry the
// source WKT in a "wkt" column) of the single batch in an Arrow IPC stream
// file. The caller must Release the returned array.
func readArrowsColumn(t *testing.T, mem memory.Allocator, path string) arrow.Array {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }() // read-only; a close error cannot lose data

	rdr, err := ipc.NewReader(f, ipc.WithAllocator(mem))
	require.NoError(t, err)
	defer rdr.Release()

	require.True(t, rdr.Next(), "no record batches in %s: %v", path, rdr.Err())
	rec := rdr.RecordBatch()
	idx := rec.Schema().FieldIndices("geometry")
	require.Len(t, idx, 1, "no geometry column in %s", path)
	col := rec.Column(idx[0])
	col.Retain()
	require.False(t, rdr.Next(), "expected a single record batch in %s", path)
	return col
}

// readTSV returns the WKT rows of an example .tsv file. The first line is a
// header; a blank line is a null row.
func readTSV(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	require.Equal(t, "geometry", lines[0], "unexpected header in %s", path)
	return lines[1:]
}

// buildFromWKT builds an array of typ from WKT rows, mapping blank rows to
// null.
func buildFromWKT(t *testing.T, mem memory.Allocator, typ arrow.ExtensionType, rows []string) arrow.Array {
	t.Helper()
	b := typ.(array.CustomExtensionBuilder).NewBuilder(mem)
	defer b.Release()
	for _, row := range rows {
		if strings.TrimSpace(row) == "" {
			b.AppendNull()
			continue
		}
		require.NoError(t, b.AppendValueFromString(row), row)
	}
	return b.NewArray()
}

// examples is the geometry type x dimension grid the example files cover.
var examples = []struct {
	name string
	// newType returns the default type for a dimension and layout, to compare
	// the reference storage against ours.
	newType func(dim geoarrow.Dimension, interleaved bool) arrow.ExtensionType
}{
	{"linestring", func(d geoarrow.Dimension, il bool) arrow.ExtensionType {
		if il {
			return geoarrow.NewLineStringType(geoarrow.LineStringWithInterleaved(d))
		}
		return geoarrow.NewLineStringType(geoarrow.LineStringWithDimension(d))
	}},
	{"multipoint", func(d geoarrow.Dimension, il bool) arrow.ExtensionType {
		if il {
			return geoarrow.NewMultiPointType(geoarrow.MultiPointWithInterleaved(d))
		}
		return geoarrow.NewMultiPointType(geoarrow.MultiPointWithDimension(d))
	}},
	{"multilinestring", func(d geoarrow.Dimension, il bool) arrow.ExtensionType {
		if il {
			return geoarrow.NewMultiLineStringType(geoarrow.MultiLineStringWithInterleaved(d))
		}
		return geoarrow.NewMultiLineStringType(geoarrow.MultiLineStringWithDimension(d))
	}},
	{"multipolygon", func(d geoarrow.Dimension, il bool) arrow.ExtensionType {
		if il {
			return geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithInterleaved(d))
		}
		return geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithDimension(d))
	}},
}

var exampleDims = []struct {
	suffix string
	dim    geoarrow.Dimension
}{
	{"", geoarrow.XY},
	{"-z", geoarrow.XYZ},
	{"-m", geoarrow.XYM},
	{"-zm", geoarrow.XYZM},
}

func TestGeoArrowDataExamples(t *testing.T) {
	for _, ex := range examples {
		for _, d := range exampleDims {
			base := "example_" + ex.name + d.suffix
			t.Run(ex.name+d.suffix, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)

				wkt := readTSV(t, dataFile(t, base+".tsv"))
				require.NotEmpty(t, wkt)

				cols := map[bool]arrow.Array{}
				for _, interleaved := range []bool{false, true} {
					name := base + ".arrows"
					label := "struct"
					if interleaved {
						name = base + "_interleaved.arrows"
						label = "interleaved"
					}
					col := readArrowsColumn(t, mem, dataFile(t, name))
					defer col.Release()
					cols[interleaved] = col

					t.Run(label, func(t *testing.T) {
						typ, ok := col.DataType().(arrow.ExtensionType)
						require.True(t, ok, "reference column is %s, not an extension type: is %s registered?", col.DataType(), ex.name)
						want := ex.newType(d.dim, interleaved)
						require.Equal(t, want.ExtensionName(), typ.ExtensionName())

						// Our default storage matches the reference storage.
						require.True(t, arrow.TypeEqual(want.StorageType(), typ.StorageType()),
							"storage mismatch\n got: %s\nwant: %s", typ.StorageType(), want.StorageType())
						require.True(t, want.ExtensionEquals(typ))

						require.Equal(t, len(wkt), col.Len())

						// Parsing the reference WKT yields exactly the reference
						// Arrow data, which also covers null vs. EMPTY rows.
						built := buildFromWKT(t, mem, typ, wkt)
						defer built.Release()
						require.True(t, array.Equal(col, built))

						checkRoundTrips(t, mem, typ, col)
					})
				}

				// Struct and interleaved encode the same geometries.
				require.Equal(t, cols[false].Len(), cols[true].Len())
				for i := range cols[false].Len() {
					require.Equal(t, cols[false].ValueStr(i), cols[true].ValueStr(i), "row %d", i)
				}

				t.Run("wkt", func(t *testing.T) {
					// The reference WKT column parses to the same geometries as
					// the native column.
					col := readArrowsColumn(t, mem, dataFile(t, base+"_wkt.arrows"))
					defer col.Release()

					wkts, ok := col.(*geoarrow.WKTArray)
					require.True(t, ok, "unexpected wkt column type %s", col.DataType())
					rows := make([]string, wkts.Len())
					for i := range rows {
						if wkts.IsValid(i) {
							rows[i] = wkts.Value(i).String()
						}
					}
					native := cols[false]
					built := buildFromWKT(t, mem, native.DataType().(arrow.ExtensionType), rows)
					defer built.Release()
					require.True(t, array.Equal(native, built))
				})
			})
		}
	}
}

// wktExamples are all six native geometry types, whose _wkt example files are
// read as geoarrow.wkt.
var wktExamples = []struct {
	name string
	id   geoarrow.GeometryTypeID // XY type id; Z/M/ZM add 10/20/30
}{
	{"point", geoarrow.PointID},
	{"linestring", geoarrow.LineStringID},
	{"polygon", geoarrow.PolygonID},
	{"multipoint", geoarrow.MultiPointID},
	{"multilinestring", geoarrow.MultiLineStringID},
	{"multipolygon", geoarrow.MultiPolygonID},
}

func TestGeoArrowDataWKT(t *testing.T) {
	offset := map[geoarrow.Dimension]geoarrow.GeometryTypeID{
		geoarrow.XY: 0, geoarrow.XYZ: 10, geoarrow.XYM: 20, geoarrow.XYZM: 30,
	}
	for _, ex := range wktExamples {
		for _, d := range exampleDims {
			base := "example_" + ex.name + d.suffix
			t.Run(ex.name+d.suffix, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)

				tsv := readTSV(t, dataFile(t, base+".tsv"))
				col := readArrowsColumn(t, mem, dataFile(t, base+"_wkt.arrows"))
				defer col.Release()

				typ, ok := col.DataType().(*geoarrow.WKTType)
				require.True(t, ok, "reference column is %s: is geoarrow.wkt registered?", col.DataType())
				require.Equal(t, arrow.STRING, typ.StorageType().ID())
				wkts := col.(*geoarrow.WKTArray)
				require.Equal(t, len(tsv), wkts.Len())

				for i, row := range tsv {
					if row == "" {
						require.True(t, wkts.IsNull(i), "row %d", i)
						continue
					}
					require.True(t, wkts.IsValid(i), "row %d", i)
					v := wkts.Value(i)
					require.Equal(t, row, v.String(), "row %d", i)
					require.Equal(t, ex.id+offset[d.dim], v.GeometryType(), "row %d: %s", i, row)
					require.Equal(t, d.dim, v.Dimension(), "row %d: %s", i, row)
					require.Equal(t, strings.HasSuffix(row, "EMPTY"), v.IsEmpty(), "row %d: %s", i, row)
				}

				checkRoundTrips(t, mem, typ, col)
			})
		}
	}
}

func TestGeoArrowDataBox(t *testing.T) {
	for _, tc := range []struct {
		file string
		// crossing is the minimum number of boxes expected to wrap the
		// antimeridian (xmin > xmax).
		crossing int
	}{
		{"natural-earth_countries-bounds_box.arrows", 1},
	} {
		t.Run(tc.file, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			col := readArrowsColumn(t, mem, dataFile(t, tc.file))
			defer col.Release()

			typ, ok := col.DataType().(*geoarrow.BoxType)
			require.True(t, ok, "reference column is %s: is geoarrow.box registered?", col.DataType())
			require.True(t, arrow.TypeEqual(geoarrow.NewBoxType().StorageType(), typ.StorageType()),
				"storage mismatch: %s", typ.StorageType())
			require.NotEmpty(t, typ.Metadata().CRS, "reference data carries a CRS")
			require.Equal(t, geoarrow.CRSTypePROJJSON, typ.Metadata().CRSType)

			ba := col.(*geoarrow.BoxArray)
			require.NotZero(t, ba.Len())
			crossing := 0
			for i := range ba.Len() {
				if ba.IsNull(i) {
					continue
				}
				v := ba.Value(i)
				require.Equal(t, geoarrow.XY, v.Dimension())
				if v.CrossesAntimeridian() {
					crossing++
					continue
				}
				require.LessOrEqual(t, v.XMin(), v.XMax(), "row %d", i)
				require.LessOrEqual(t, v.YMin(), v.YMax(), "row %d", i)
			}
			require.GreaterOrEqual(t, crossing, tc.crossing)

			// The reference CRS is pretty-printed JSON, but Serialize compacts
			// it and Extension.Equal compares CRS bytes, so a type with such a
			// CRS is never equal to its own Deserialize(Serialize()). Compact it
			// up front so the round trips below test the box, not that.
			var crs bytes.Buffer
			require.NoError(t, stdjson.Compact(&crs, typ.Metadata().CRS))
			meta := typ.Metadata()
			meta.CRS = crs.Bytes()
			compact := geoarrow.NewBoxType(geoarrow.BoxWithStorage(typ.StorageType()), geoarrow.BoxWithMetadata(meta))
			arr := array.NewExtensionArrayWithStorage(compact, ba.Storage())
			defer arr.Release()

			checkRoundTrips(t, mem, compact, arr)
		})
	}
}
