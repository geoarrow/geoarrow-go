package geoarrow_test

import (
	"context"
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
	assertParquetLogicalType(t, path, schema.GeometryLogicalType{})
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
	require.Equal(t, geoarrow.CRSTypeSRID, wkb.Metadata().CRSType)
	require.JSONEq(t, `{"crs":"projjson:geometry_crs","crs_type":"srid"}`, wkb.Serialize())
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
	defer in.Close()

	parquetReader, err := file.NewParquetReader(in)
	require.NoError(t, err)
	defer parquetReader.Close()

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
	require.Equal(t, geoarrow.CRSTypeSRID, wkbType.Metadata().CRSType)
	require.JSONEq(t, `{"crs":"projjson:geometry_crs","crs_type":"srid"}`, wkbType.Serialize())

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
	require.Equal(t, "EPSG:4326", wkb.Metadata().ParquetCRS())
	require.Equal(t, geoarrow.CRSTypeSRID, wkb.Metadata().CRSType)
	require.Equal(t, geoarrow.EdgePlanar, wkb.Metadata().Edges)
	require.JSONEq(t, `{"crs":"EPSG:4326","crs_type":"srid"}`, wkb.Serialize())
	// The reconstructed WKB type should write back to an equivalent Parquet
	// logical type.
	require.True(t, logical.Equals(wkb.ParquetLogicalType()))
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
	require.Equal(t, "OGC:CRS84", wkb.Metadata().ParquetCRS())
	require.Equal(t, geoarrow.CRSTypeSRID, wkb.Metadata().CRSType)
	require.Equal(t, geoarrow.EdgeKarney, wkb.Metadata().Edges)
	require.JSONEq(t, `{"crs":"OGC:CRS84","crs_type":"srid","edges":"karney"}`, wkb.Serialize())
	require.True(t, logical.Equals(wkb.ParquetLogicalType()))
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

func writeRecordToParquetFile(t *testing.T, mem memory.Allocator, rec arrow.Record, name string) string {
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
	defer in.Close()

	parquetReader, err := file.NewParquetReader(in)
	require.NoError(t, err)
	defer parquetReader.Close()

	require.EqualValues(t, 5, parquetReader.MetaData().NumRows)
	require.Equal(t, 1, parquetReader.MetaData().Schema.NumColumns())
	require.True(t, want.Equals(parquetReader.MetaData().Schema.Column(0).LogicalType()))
}

// readParquetArrowSchema is a helper function for reading the Arrow schema from a test Parquet file
func readParquetArrowSchema(t *testing.T, mem memory.Allocator, path string) *arrow.Schema {
	t.Helper()

	in, err := os.Open(path)
	require.NoError(t, err)
	defer in.Close()

	parquetReader, err := file.NewParquetReader(in)
	require.NoError(t, err)
	defer parquetReader.Close()

	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)

	arrsc, err := arrowReader.Schema()
	require.NoError(t, err)
	return arrsc
}
