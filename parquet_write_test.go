package geoarrow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/geoarrow/geoarrow-go"
	"github.com/stretchr/testify/require"
)

func TestE2EWriteArrowToParquetGeometry(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rec := newFiveRowWKBRecord(t, mem, geoarrow.NewWKBType())
	defer rec.Release()

	path := writeRecordToParquetFile(t, mem, rec, "geometry.parquet")
	// An omitted GeoArrow CRS is unknown. Parquet writes that as srid:0 since
	// an unset Parquet CRS means OGC:CRS84.
	assertParquetLogicalType(t, path, schema.GeometryLogicalType{Crs: "srid:0"})
}

func TestE2EWriteArrowToParquetGeography(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rec := newFiveRowWKBRecord(t, mem, geoarrow.NewWKBType(geoarrow.WKBWithMetadata(geoarrow.Metadata{
		Edges: geoarrow.EdgeSpherical,
	})))
	defer rec.Release()

	path := writeRecordToParquetFile(t, mem, rec, "geography.parquet")
	assertParquetLogicalType(t, path, schema.GeographyLogicalType{
		Crs:       "srid:0",
		Algorithm: schema.GeographyEdgeSpherical,
	})
}

func TestE2EWriteReadWKBWithProjJSONCRS(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// Test the pattern used by the iceberg v3 spec form that references a PROJJSON definition stored
	// outside the Parquet logical type instead of inlining the definition.
	const crs = "projjson:geometry_crs"
	meta := geoarrow.NewMetadata()
	meta.SetCRSString(crs)
	rec := newFiveRowWKBRecord(t, mem, geoarrow.NewWKBType(geoarrow.WKBWithMetadata(meta)))
	defer rec.Release()

	// First verify the write path stores the CRS on the Parquet GEOMETRY
	// logical type exactly as provided by the WKB extension metadata.
	path := writeRecordToParquetFile(t, mem, rec, "geometry_projjson.parquet")
	assertParquetLogicalType(t, path, schema.GeometryLogicalType{Crs: crs})

	// Then verify the read schema path reconstructs WKB from that Parquet
	// logical type and preserves the CRS reference.
	arrsc := readParquetArrowSchema(t, mem, path)
	require.Equal(t, 1, arrsc.NumFields())

	wkb, ok := arrsc.Field(0).Type.(*geoarrow.WKBType)
	require.True(t, ok)
	require.True(t, arrow.TypeEqual(arrow.BinaryTypes.Binary, wkb.StorageType()))
	require.Equal(t, crs, wkb.Metadata().ParquetCRS())
	require.Empty(t, wkb.Metadata().CRSType)
	require.JSONEq(t, `{"crs":"projjson:geometry_crs"}`, wkb.Serialize())
	require.True(t, schema.GeometryLogicalType{Crs: crs}.Equals(wkb.ParquetLogicalType()))
}

func TestE2EReadTableWKBWithProjJSONCRS(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// Test the pattern used by v3 iceberg geospatial spec which stores CRS as an opaque identifier string.
	// In this form, the PROJJSON definition lives in table metadata elsewhere.
	const crs = "projjson:geometry_crs"
	meta := geoarrow.NewMetadata()
	meta.SetCRSString(crs)
	rec := newFiveRowWKBRecord(t, mem, geoarrow.NewWKBType(geoarrow.WKBWithMetadata(meta)))
	defer rec.Release()

	// Use the low-level writer helper so the file relies on the Parquet logical
	// type instead of embedding the original Arrow extension schema.
	path := writeRecordToParquetFile(t, mem, rec, "geometry_projjson_read_table.parquet")
	in, err := os.Open(path)
	require.NoError(t, err)

	parquetReader, err := file.NewParquetReader(in)
	require.NoError(t, err)
	defer func() { require.NoError(t, parquetReader.Close()) }()

	// Without ARROW:schema metadata, pqarrow must reconstruct WKB from the
	// Parquet GEOMETRY logical type via ArrowTypeFromParquet.
	require.Nil(t, parquetReader.MetaData().KeyValueMetadata().FindValue("ARROW:schema"))

	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	tbl, err := arrowReader.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()

	// The schema read path should restore the GeoArrow extension type and keep
	// the CRS identifier unchanged rather than parsing it as inline PROJJSON.
	wkbType, ok := tbl.Schema().Field(0).Type.(*geoarrow.WKBType)
	require.True(t, ok)
	require.True(t, arrow.TypeEqual(arrow.BinaryTypes.Binary, wkbType.StorageType()))
	require.Equal(t, crs, wkbType.Metadata().ParquetCRS())
	require.Empty(t, wkbType.Metadata().CRSType)
	require.JSONEq(t, `{"crs":"projjson:geometry_crs"}`, wkbType.Serialize())

	// Reading data should produce a usable WKB extension array, not just a
	// binary array with the right schema metadata.
	wkbArr, ok := tbl.Column(0).Data().Chunk(0).(*geoarrow.WKBArray)
	require.True(t, ok)
	require.Equal(t, geoarrow.WKBBytes(testWKBPoint), wkbArr.Value(0))
}

func TestParquetLogicalTypeOnlyAppliesToWKB(t *testing.T) {
	_, ok := any(geoarrow.NewWKBType()).(pqarrow.ExtensionCustomParquetType)
	require.True(t, ok)

	_, ok = any(geoarrow.NewWKBType()).(pqarrow.ExtensionCustomArrowReadType)
	require.True(t, ok)

	_, ok = any(geoarrow.NewPointType()).(pqarrow.ExtensionCustomParquetType)
	require.False(t, ok)

	_, ok = any(geoarrow.NewPolygonType()).(pqarrow.ExtensionCustomParquetType)
	require.False(t, ok)
}

func TestFromParquetGeometryLogicalTypeToWKB(t *testing.T) {
	logical := schema.GeometryLogicalType{Crs: "EPSG:4326"}

	// Construct only a Parquet schema to isolate ArrowTypeFromParquet from file
	// IO and Arrow schema metadata.
	arrsc, err := pqarrow.FromParquet(newSingleColumnParquetSchema(t, "geometry", logical), nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, arrsc.NumFields())

	// A Parquet GEOMETRY byte-array column should become WKB with binary
	// storage, preserving the CRS string exactly as provided by Parquet.
	wkb, ok := arrsc.Field(0).Type.(*geoarrow.WKBType)
	require.True(t, ok)
	require.True(t, arrow.TypeEqual(arrow.BinaryTypes.Binary, wkb.StorageType()))
	require.Empty(t, wkb.Metadata().CRSType)
	require.Equal(t, geoarrow.EdgePlanar, wkb.Metadata().Edges)
	require.JSONEq(t, `{"crs":"EPSG:4326"}`, wkb.Serialize())
	// EPSG:4326 is canonicalized to the Parquet default (an unset CRS) when
	// written back, matching arrow-rs, Arrow C++ and GDAL.
	require.Empty(t, wkb.Metadata().ParquetCRS())
	require.True(t, schema.GeometryLogicalType{}.Equals(wkb.ParquetLogicalType()))
}

func TestFromParquetGeometryLogicalTypeWithProjJSONToWKB(t *testing.T) {
	crs := `{"type":"GeographicCRS","name":"WGS 84"}`
	logical := schema.GeometryLogicalType{Crs: crs}

	arrsc, err := pqarrow.FromParquet(newSingleColumnParquetSchema(t, "geometry", logical), nil, nil)
	require.NoError(t, err)

	wkb, ok := arrsc.Field(0).Type.(*geoarrow.WKBType)
	require.True(t, ok)
	require.Equal(t, crs, wkb.Metadata().ParquetCRS())
	require.Equal(t, geoarrow.CRSTypePROJJSON, wkb.Metadata().CRSType)
	require.JSONEq(t, `{"crs":{"type":"GeographicCRS","name":"WGS 84"},"crs_type":"projjson"}`, wkb.Serialize())
	require.True(t, logical.Equals(wkb.ParquetLogicalType()))
}

func TestFromParquetGeographyLogicalTypeToWKB(t *testing.T) {
	// Geography carries both CRS and edge interpolation metadata; both should
	// survive conversion into the GeoArrow WKB extension metadata.
	logical := schema.GeographyLogicalType{
		Crs:       "OGC:CRS84",
		Algorithm: schema.GeographyEdgeKarney,
	}

	// This uses schema conversion only, so the test specifically exercises the
	// ExtensionParquetLogicalType hook rather than Arrow schema restoration.
	arrsc, err := pqarrow.FromParquet(newSingleColumnParquetSchema(t, "geography", logical), nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, arrsc.NumFields())

	// The reconstructed WKB type should preserve the opaque CRS string and map
	// the Parquet geography algorithm onto GeoArrow edge interpolation.
	wkb, ok := arrsc.Field(0).Type.(*geoarrow.WKBType)
	require.True(t, ok)
	require.True(t, arrow.TypeEqual(arrow.BinaryTypes.Binary, wkb.StorageType()))
	require.Empty(t, wkb.Metadata().CRSType)
	require.Equal(t, geoarrow.EdgeKarney, wkb.Metadata().Edges)
	require.JSONEq(t, `{"crs":"OGC:CRS84","edges":"karney"}`, wkb.Serialize())
	// OGC:CRS84 is the Parquet default, so it is omitted when written back.
	require.Empty(t, wkb.Metadata().ParquetCRS())
	require.True(t, schema.GeographyLogicalType{Algorithm: schema.GeographyEdgeKarney}.Equals(wkb.ParquetLogicalType()))
}

func TestE2EWriteReadWKBWithSRIDCRS(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// GeoArrow stores SRIDs bare with crs_type "srid"; the Parquet spec
	// requires them to be written with a "srid:" prefix.
	rec := newFiveRowWKBRecord(t, mem, geoarrow.NewWKBType(geoarrow.WKBWithMetadata(geoarrow.Metadata{
		CRS:     json.RawMessage(`"4326"`),
		CRSType: geoarrow.CRSTypeSRID,
	})))
	defer rec.Release()

	path := writeRecordToParquetFile(t, mem, rec, "geometry_srid.parquet")
	assertParquetLogicalType(t, path, schema.GeometryLogicalType{Crs: "srid:4326"})

	// Reading keeps the Parquet SRID whole, as arrow-rs does, rather than
	// splitting it into a bare id plus crs_type "srid".
	arrsc := readParquetArrowSchema(t, mem, path)
	wkb, ok := arrsc.Field(0).Type.(*geoarrow.WKBType)
	require.True(t, ok)
	require.Empty(t, wkb.Metadata().CRSType)
	require.JSONEq(t, `{"crs":"srid:4326"}`, wkb.Serialize())
	require.Equal(t, "srid:4326", wkb.Metadata().ParquetCRS())
}

func TestE2EWriteWKBOmitsDefaultCRS(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rec := newFiveRowWKBRecord(t, mem, geoarrow.NewWKBType(geoarrow.WKBWithMetadata(geoarrow.Metadata{
		CRS:     json.RawMessage(`"OGC:CRS84"`),
		CRSType: geoarrow.CRSTypeAuthorityCode,
	})))
	defer rec.Release()

	path := writeRecordToParquetFile(t, mem, rec, "geometry_default_crs.parquet")
	assertParquetLogicalType(t, path, schema.GeometryLogicalType{})
}

func TestParquetCRS(t *testing.T) {
	tests := []struct {
		name string
		meta geoarrow.Metadata
		want string
	}{
		{"omitted is unknown", geoarrow.Metadata{}, "srid:0"},
		{"json null is unknown", geoarrow.Metadata{CRS: json.RawMessage(`null`)}, "srid:0"},
		{"json null with whitespace is unknown", geoarrow.Metadata{CRS: json.RawMessage(` null `)}, "srid:0"},
		{"srid unknown", geoarrow.Metadata{CRS: json.RawMessage(`"0"`), CRSType: geoarrow.CRSTypeSRID}, "srid:0"},
		{"epsg 4326 authority code", geoarrow.Metadata{CRS: json.RawMessage(`"EPSG:4326"`), CRSType: geoarrow.CRSTypeAuthorityCode}, ""},
		{"epsg 4326 untyped", geoarrow.Metadata{CRS: json.RawMessage(`"EPSG:4326"`)}, ""},
		{"srid", geoarrow.Metadata{CRS: json.RawMessage(`"4326"`), CRSType: geoarrow.CRSTypeSRID}, "srid:4326"},
		{"srid already prefixed", geoarrow.Metadata{CRS: json.RawMessage(`"srid:4326"`), CRSType: geoarrow.CRSTypeSRID}, "srid:4326"},
		{"srid string without type", geoarrow.Metadata{CRS: json.RawMessage(`"srid:4326"`)}, "srid:4326"},
		{"srid:0 string is unknown", geoarrow.Metadata{CRS: json.RawMessage(`"srid:0"`)}, "srid:0"},
		{"SRID:0 string is written as-is", geoarrow.Metadata{CRS: json.RawMessage(`"SRID:0"`)}, "SRID:0"},
		{"default authority code", geoarrow.Metadata{CRS: json.RawMessage(`"OGC:CRS84"`), CRSType: geoarrow.CRSTypeAuthorityCode}, ""},
		{"default untyped", geoarrow.Metadata{CRS: json.RawMessage(`"OGC:CRS84"`)}, ""},
		{"authority code", geoarrow.Metadata{CRS: json.RawMessage(`"EPSG:3857"`), CRSType: geoarrow.CRSTypeAuthorityCode}, "EPSG:3857"},
		{"projjson reference", geoarrow.Metadata{CRS: json.RawMessage(`"projjson:geometry_crs"`)}, "projjson:geometry_crs"},
		{
			"projjson crs84",
			geoarrow.Metadata{CRS: json.RawMessage(`{"name":"WGS 84","id":{"authority":"OGC","code":"CRS84"}}`), CRSType: geoarrow.CRSTypePROJJSON},
			"",
		},
		{
			"projjson epsg 4326",
			geoarrow.Metadata{CRS: json.RawMessage(`{"name":"WGS 84","id":{"authority":"EPSG","code":4326}}`), CRSType: geoarrow.CRSTypePROJJSON},
			"",
		},
		{
			"projjson epsg 4326 string code",
			geoarrow.Metadata{CRS: json.RawMessage(`{"name":"WGS 84","id":{"authority":"EPSG","code":"4326"}}`)},
			"",
		},
		{
			"projjson other",
			geoarrow.Metadata{CRS: json.RawMessage(`{"name":"Pseudo-Mercator","id":{"authority":"EPSG","code":3857}}`), CRSType: geoarrow.CRSTypePROJJSON},
			`{"name":"Pseudo-Mercator","id":{"authority":"EPSG","code":3857}}`,
		},
		{
			"projjson without id",
			geoarrow.Metadata{CRS: json.RawMessage(`{"name":"WGS 84"}`), CRSType: geoarrow.CRSTypePROJJSON},
			`{"name":"WGS 84"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.meta.ParquetCRS())
		})
	}
}

func TestSetCRSStringKeepsSRIDWhole(t *testing.T) {
	meta := geoarrow.NewMetadata()
	meta.SetCRSString("srid:3857")
	require.Empty(t, meta.CRSType)
	require.JSONEq(t, `"srid:3857"`, string(meta.CRS))
	require.Equal(t, "srid:3857", meta.ParquetCRS())
}

// TestSetCRSStringFromParquet covers the Parquet to GeoArrow CRS mapping, which
// follows arrow-rs: an unset Parquet CRS is OGC:CRS84, and srid:0 is unknown.
func TestSetCRSStringFromParquet(t *testing.T) {
	tests := []struct {
		name    string
		parquet string
		wantCRS string // empty means the GeoArrow CRS is omitted
	}{
		{"unset is OGC:CRS84", "", `"OGC:CRS84"`},
		{"explicit OGC:CRS84", "OGC:CRS84", `"OGC:CRS84"`},
		{"srid:0 is unknown", "srid:0", ""},
		{"SRID:0 is not treated as unknown", "SRID:0", `"SRID:0"`},
		{"other srid", "srid:3857", `"srid:3857"`},
		{"srid:00 is not unknown", "srid:00", `"srid:00"`},
		{"other authority code", "EPSG:3857", `"EPSG:3857"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := geoarrow.NewMetadata()
			meta.SetCRSString(tt.parquet)
			if tt.wantCRS == "" {
				require.Empty(t, meta.CRS)
				require.Empty(t, meta.CRSType)
				return
			}
			require.JSONEq(t, tt.wantCRS, string(meta.CRS))
			require.Empty(t, meta.CRSType)
		})
	}
}

// TestParquetCRSRoundTrips checks the cases from the arrow-rs conformance
// table survive a GeoArrow -> Parquet -> GeoArrow round trip.
func TestParquetCRSRoundTrips(t *testing.T) {
	roundTrip := func(meta geoarrow.Metadata) geoarrow.Metadata {
		logical := geoarrow.NewWKBType(geoarrow.WKBWithMetadata(meta)).ParquetLogicalType()
		arrsc, err := pqarrow.FromParquet(newSingleColumnParquetSchema(t, "geometry", logical), nil, nil)
		require.NoError(t, err)
		wkb, ok := arrsc.Field(0).Type.(*geoarrow.WKBType)
		require.True(t, ok)
		return wkb.Metadata()
	}

	t.Run("unknown stays unknown", func(t *testing.T) {
		require.Empty(t, roundTrip(geoarrow.Metadata{}).CRS)
	})
	t.Run("OGC:CRS84 stays OGC:CRS84", func(t *testing.T) {
		got := roundTrip(geoarrow.Metadata{CRS: json.RawMessage(`"OGC:CRS84"`), CRSType: geoarrow.CRSTypeAuthorityCode})
		require.JSONEq(t, `"OGC:CRS84"`, string(got.CRS))
	})
	t.Run("EPSG:4326 canonicalizes to OGC:CRS84", func(t *testing.T) {
		got := roundTrip(geoarrow.Metadata{CRS: json.RawMessage(`"EPSG:4326"`), CRSType: geoarrow.CRSTypeAuthorityCode})
		require.JSONEq(t, `"OGC:CRS84"`, string(got.CRS))
	})
	t.Run("EPSG:4326 PROJJSON canonicalizes to OGC:CRS84", func(t *testing.T) {
		got := roundTrip(geoarrow.Metadata{
			CRS:     json.RawMessage(`{"name":"WGS 84","id":{"authority":"EPSG","code":4326}}`),
			CRSType: geoarrow.CRSTypePROJJSON,
		})
		require.JSONEq(t, `"OGC:CRS84"`, string(got.CRS))
	})
	t.Run("bare srid reads back whole", func(t *testing.T) {
		got := roundTrip(geoarrow.Metadata{CRS: json.RawMessage(`"3857"`), CRSType: geoarrow.CRSTypeSRID})
		require.JSONEq(t, `"srid:3857"`, string(got.CRS))
		require.Empty(t, got.CRSType)
	})
	t.Run("whole srid is stable", func(t *testing.T) {
		got := roundTrip(geoarrow.Metadata{CRS: json.RawMessage(`"srid:3857"`)})
		require.JSONEq(t, `"srid:3857"`, string(got.CRS))
		require.Empty(t, got.CRSType)
	})
	t.Run("uppercase SRID:0 is not treated as unknown", func(t *testing.T) {
		got := roundTrip(geoarrow.Metadata{CRS: json.RawMessage(`"SRID:0"`)})
		require.JSONEq(t, `"SRID:0"`, string(got.CRS))
	})
}

// TestParquetCRSFromDeserializedNullCRS checks that an explicit JSON null CRS
// is treated like an omitted one (unknown), as arrow-rs does, and is therefore
// written as srid:0 instead of the Parquet default OGC:CRS84.
func TestParquetCRSFromDeserializedNullCRS(t *testing.T) {
	tests := []struct {
		name string
		data string
		want schema.LogicalType
	}{
		{"geometry omitted", `{}`, schema.GeometryLogicalType{Crs: "srid:0"}},
		{"geometry null", `{"crs":null}`, schema.GeometryLogicalType{Crs: "srid:0"}},
		{"geography omitted", `{"edges":"spherical"}`, schema.GeographyLogicalType{Crs: "srid:0", Algorithm: schema.GeographyEdgeSpherical}},
		{"geography null", `{"crs":null,"edges":"spherical"}`, schema.GeographyLogicalType{Crs: "srid:0", Algorithm: schema.GeographyEdgeSpherical}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt, err := geoarrow.NewWKBType().Deserialize(arrow.BinaryTypes.Binary, tt.data)
			require.NoError(t, err)
			wkb, ok := dt.(*geoarrow.WKBType)
			require.True(t, ok)

			require.Equal(t, "srid:0", wkb.Metadata().ParquetCRS())
			require.True(t, tt.want.Equals(wkb.ParquetLogicalType()),
				"want %s, got %s", tt.want, wkb.ParquetLogicalType())
		})
	}
}

// TestE2EParquetCRSWithoutStoredArrowSchema writes and reads real Parquet
// files with no ARROW:schema metadata, so the GeoArrow type must be rebuilt
// from the Parquet GEOMETRY/GEOGRAPHY logical type alone. This is the path a
// downstream reader (e.g. iceberg-go) takes for files written by other tools.
func TestE2EParquetCRSWithoutStoredArrowSchema(t *testing.T) {
	const projjsonEPSG4326 = `{"type":"GeographicCRS","name":"WGS 84","id":{"authority":"EPSG","code":4326}}`
	raw := func(s string) json.RawMessage { return json.RawMessage(s) }

	crsCases := []struct {
		name string
		meta geoarrow.Metadata // GeoArrow metadata before writing (edges added per case)
		// parquetCRS is the CRS stored on the Parquet logical type.
		parquetCRS string
		// readCRS and readCRSType are the GeoArrow CRS read back; empty readCRS
		// means the CRS is omitted (unknown).
		readCRS     string
		readCRSType geoarrow.CRSType
	}{
		{"unknown", geoarrow.Metadata{}, "srid:0", "", ""},
		{"OGC:CRS84", geoarrow.Metadata{CRS: raw(`"OGC:CRS84"`), CRSType: geoarrow.CRSTypeAuthorityCode}, "", `"OGC:CRS84"`, ""},
		{"EPSG:4326", geoarrow.Metadata{CRS: raw(`"EPSG:4326"`), CRSType: geoarrow.CRSTypeAuthorityCode}, "", `"OGC:CRS84"`, ""},
		{"EPSG:4326 PROJJSON", geoarrow.Metadata{CRS: raw(projjsonEPSG4326), CRSType: geoarrow.CRSTypePROJJSON}, "", `"OGC:CRS84"`, ""},
		{"other authority code", geoarrow.Metadata{CRS: raw(`"EPSG:3857"`), CRSType: geoarrow.CRSTypeAuthorityCode}, "EPSG:3857", `"EPSG:3857"`, ""},
		{"srid", geoarrow.Metadata{CRS: raw(`"3857"`), CRSType: geoarrow.CRSTypeSRID}, "srid:3857", `"srid:3857"`, ""},
		{"uppercase SRID:0 is not treated as unknown", geoarrow.Metadata{CRS: raw(`"SRID:0"`)}, "SRID:0", `"SRID:0"`, ""},
		{"projjson reference", geoarrow.Metadata{CRS: raw(`"projjson:geo.crs"`)}, "projjson:geo.crs", `"projjson:geo.crs"`, ""},
	}

	edgeCases := []struct {
		name  string
		edges geoarrow.EdgeInterpolation
		algo  schema.GeographyEdgeInterpolationAlgorithm
	}{
		{"geometry", geoarrow.EdgePlanar, ""},
		{"geography spherical", geoarrow.EdgeSpherical, schema.GeographyEdgeSpherical},
		{"geography karney", geoarrow.EdgeKarney, schema.GeographyEdgeKarney},
	}

	for _, crsCase := range crsCases {
		for _, edgeCase := range edgeCases {
			t.Run(crsCase.name+"/"+edgeCase.name, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)

				meta := crsCase.meta
				meta.Edges = edgeCase.edges
				typ := geoarrow.NewWKBType(geoarrow.WKBWithMetadata(meta))

				// Three rows including a null, to confirm values survive.
				builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
				builder.Append(geoarrow.WKBBytes(testWKBPoint))
				builder.AppendNull()
				builder.Append(geoarrow.WKBBytes(testWKBPoint))
				arr := builder.NewArray()
				builder.Release()
				defer arr.Release()

				sc := arrow.NewSchema([]arrow.Field{{Name: "geom", Type: typ, Nullable: true}}, nil)
				rec := array.NewRecordBatch(sc, []arrow.Array{arr}, int64(arr.Len()))
				defer rec.Release()

				// Deliberately no pqarrow.WithStoreSchema().
				var buf bytes.Buffer
				writer, err := pqarrow.NewFileWriter(sc, &buf,
					parquet.NewWriterProperties(parquet.WithAllocator(mem)),
					pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem)))
				require.NoError(t, err)
				require.NoError(t, writer.Write(rec))
				require.NoError(t, writer.Close())

				rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
				require.NoError(t, err)
				defer func() { require.NoError(t, rdr.Close()) }()
				require.Nil(t, rdr.MetaData().KeyValueMetadata().FindValue("ARROW:schema"))

				// The Parquet logical type carries the converted CRS.
				var wantLogical schema.LogicalType = schema.GeometryLogicalType{Crs: crsCase.parquetCRS}
				if edgeCase.algo != "" {
					wantLogical = schema.GeographyLogicalType{Crs: crsCase.parquetCRS, Algorithm: edgeCase.algo}
				}
				gotLogical := rdr.MetaData().Schema.Column(0).LogicalType()
				require.True(t, wantLogical.Equals(gotLogical), "want %s, got %s", wantLogical, gotLogical)

				arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
				require.NoError(t, err)
				tbl, err := arrowRdr.ReadTable(context.Background())
				require.NoError(t, err)
				defer tbl.Release()

				// The GeoArrow type is rebuilt from the logical type alone.
				wkb, ok := tbl.Schema().Field(0).Type.(*geoarrow.WKBType)
				require.True(t, ok)
				got := wkb.Metadata()
				require.Equal(t, edgeCase.edges, got.Edges)
				require.Equal(t, crsCase.readCRSType, got.CRSType)
				if crsCase.readCRS == "" {
					require.Empty(t, got.CRS)
				} else {
					require.JSONEq(t, crsCase.readCRS, string(got.CRS))
				}

				// Values and nulls are intact.
				wkbArr, ok := tbl.Column(0).Data().Chunk(0).(*geoarrow.WKBArray)
				require.True(t, ok)
				require.Equal(t, 3, wkbArr.Len())
				require.Equal(t, geoarrow.WKBBytes(testWKBPoint), wkbArr.Value(0))
				require.True(t, wkbArr.IsNull(1))
				require.Equal(t, geoarrow.WKBBytes(testWKBPoint), wkbArr.Value(2))
			})
		}
	}
}

func TestFromParquetGeographyLogicalTypeRejectsUnknownEdgeAlgorithm(t *testing.T) {
	_, err := geoarrow.NewWKBType().ArrowTypeFromParquet(schema.GeographyLogicalType{
		Algorithm: schema.GeographyEdgeInterpolationAlgorithm("bad"),
	}, arrow.BinaryTypes.Binary)
	require.Error(t, err)
}

func newFiveRowWKBRecord(t *testing.T, mem memory.Allocator, typ *geoarrow.WKBType) arrow.RecordBatch {
	t.Helper()
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	for range 5 {
		builder.Append(geoarrow.WKBBytes(testWKBPoint))
	}

	arr := builder.NewArray()
	defer arr.Release()

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "geometry", Type: typ, Nullable: false},
	}, nil)

	return array.NewRecordBatch(sc, []arrow.Array{arr}, int64(arr.Len()))
}

// newSingleColumnParquetSchema returns a Parquet schema with a single column for
// testing purposes.
func newSingleColumnParquetSchema(t *testing.T, name string, logical schema.LogicalType) *schema.Schema {
	t.Helper()

	node := schema.Must(schema.NewPrimitiveNodeLogical(name, parquet.Repetitions.Optional,
		logical, parquet.Types.ByteArray, -1, -1))
	return schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{node}, -1)))
}

func writeRecordToParquetFile(t *testing.T, mem memory.Allocator, rec arrow.RecordBatch, name string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	out, err := os.Create(path)
	require.NoError(t, err)

	props := parquet.NewWriterProperties(parquet.WithAllocator(mem))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))
	parquetSchema, err := pqarrow.ToParquet(rec.Schema(), props, arrowProps)
	require.NoError(t, err)

	writer, err := file.NewParquetWriterWithError(
		out,
		parquetSchema.Root(),
		file.WithWriterProps(props),
	)
	require.NoError(t, err)

	rowGroupWriter, err := writer.AppendRowGroupChecked()
	require.NoError(t, err)

	columnWriter, err := rowGroupWriter.NextColumn()
	require.NoError(t, err)

	writeContext := pqarrow.NewArrowWriteContext(context.Background(), &arrowProps)
	storage := rec.Column(0).(array.ExtensionArray).Storage()
	require.NoError(t, pqarrow.WriteArrowToColumn(writeContext, columnWriter, storage, nil, nil, false))
	require.NoError(t, columnWriter.Close())
	require.NoError(t, rowGroupWriter.Close())
	require.NoError(t, writer.Close())

	return path
}

func assertParquetLogicalType(t *testing.T, path string, want schema.LogicalType) {
	t.Helper()

	in, err := os.Open(path)
	require.NoError(t, err)

	parquetReader, err := file.NewParquetReader(in)
	require.NoError(t, err)
	defer func() { require.NoError(t, parquetReader.Close()) }()

	require.EqualValues(t, 5, parquetReader.MetaData().NumRows)
	require.Equal(t, 1, parquetReader.MetaData().Schema.NumColumns())
	require.True(t, want.Equals(parquetReader.MetaData().Schema.Column(0).LogicalType()))
}

// readParquetArrowSchema is a helper function for reading the Arrow schema from a test Parquet file
func readParquetArrowSchema(t *testing.T, mem memory.Allocator, path string) *arrow.Schema {
	t.Helper()

	in, err := os.Open(path)
	require.NoError(t, err)

	parquetReader, err := file.NewParquetReader(in)
	require.NoError(t, err)
	defer func() { require.NoError(t, parquetReader.Close()) }()

	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	arrsc, err := arrowReader.Schema()
	require.NoError(t, err)
	return arrsc
}
