package geoarrow_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/geoarrow/geoarrow-go"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// A simple square: (0,0) → (10,0) → (10,10) → (0,10) → (0,0)
var testSquare = geoarrow.NewPolygonValue(geoarrow.XY, [][]float64{
	{0, 0, 10, 0, 10, 10, 0, 10, 0, 0},
})

// A square with a hole
var testSquareWithHole = geoarrow.NewPolygonValue(geoarrow.XY, [][]float64{
	{0, 0, 10, 0, 10, 10, 0, 10, 0, 0},
	{2, 2, 8, 2, 8, 8, 2, 8, 2, 2},
})

func TestPolygonTypeBasics(t *testing.T) {
	typ := geoarrow.NewPolygonType()

	require.Equal(t, "geoarrow.polygon", typ.ExtensionName())
	require.True(t, typ.ExtensionEquals(typ))
	require.Equal(t, arrow.LIST, typ.StorageType().ID())

	// Check nested structure: List<List<Struct>>
	outerList := typ.StorageType().(*arrow.ListType)
	innerList := outerList.ElemField().Type.(*arrow.ListType)
	coordStruct := innerList.ElemField().Type.(*arrow.StructType)

	require.Equal(t, 2, coordStruct.NumFields())
	require.Equal(t, "x", coordStruct.Field(0).Name)
	require.Equal(t, "y", coordStruct.Field(1).Name)

	// Equal types
	typ2 := geoarrow.NewPolygonType()
	require.True(t, typ.ExtensionEquals(typ2))
}

func TestPolygonSerializationRoundTrip(t *testing.T) {
	typ := geoarrow.NewPolygonType(geoarrow.PolygonWithMetadata(geoarrow.Metadata{
		Edges: geoarrow.EdgeSpherical,
	}))

	serialized := typ.Serialize()
	deserialized, err := typ.Deserialize(typ.StorageType(), serialized)
	require.NoError(t, err)
	require.True(t, typ.ExtensionEquals(deserialized))
}

func TestPolygonValueBasics(t *testing.T) {
	t.Run("simple_square", func(t *testing.T) {
		require.Equal(t, 1, testSquare.NumRings())
		require.Equal(t, 5, testSquare.NumVertices(0))
		require.Equal(t, geoarrow.XY, testSquare.Dimension())
		require.Equal(t, geoarrow.PolygonID, testSquare.GeometryType())
		require.False(t, testSquare.IsEmpty())
		require.Contains(t, testSquare.String(), "POLYGON(")
	})

	t.Run("square_with_hole", func(t *testing.T) {
		require.Equal(t, 2, testSquareWithHole.NumRings())
		require.Equal(t, 5, testSquareWithHole.NumVertices(0))
		require.Equal(t, 5, testSquareWithHole.NumVertices(1))
		require.Contains(t, testSquareWithHole.String(), "), (")
	})

	t.Run("empty", func(t *testing.T) {
		empty := geoarrow.PolygonValue{}
		require.True(t, empty.IsEmpty())
		require.Equal(t, "POLYGON EMPTY", empty.String())
	})
}

func TestPolygonBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder.Release()

	builder.Append(testSquare)
	builder.AppendNull()
	builder.Append(testSquareWithHole)

	arr := builder.NewArray()
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	polyArr := arr.(*geoarrow.PolygonArray)

	// First polygon: simple square
	v0 := polyArr.Value(0)
	require.Equal(t, 1, v0.NumRings())
	require.Equal(t, 5, v0.NumVertices(0))
	ring0 := v0.Ring(0)
	require.Equal(t, 0.0, ring0[0]) // x of first vertex
	require.Equal(t, 0.0, ring0[1]) // y of first vertex

	// Second: null
	require.True(t, arr.IsNull(1))

	// Third: square with hole
	v2 := polyArr.Value(2)
	require.Equal(t, 2, v2.NumRings())
	require.Equal(t, 5, v2.NumVertices(0))
	require.Equal(t, 5, v2.NumVertices(1))
}

func TestPolygonAppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder.Release()

	values := []geoarrow.PolygonValue{testSquare, testSquareWithHole, testSquare}
	valid := []bool{true, false, true}
	builder.AppendValues(values, valid)

	arr := builder.NewArray()
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())
	require.True(t, arr.IsNull(1))
}

func TestPolygonCreateFromStorage(t *testing.T) {
	typ := geoarrow.NewPolygonType()

	// Build storage manually: List<rings: List<vertices: Struct<x,y>>>
	mem := memory.DefaultAllocator
	outerList := typ.StorageType().(*arrow.ListType)
	outerListBuilder := array.NewListBuilderWithField(mem, outerList.ElemField())
	defer outerListBuilder.Release()

	innerListBuilder := outerListBuilder.ValueBuilder().(*array.ListBuilder)
	structBuilder := innerListBuilder.ValueBuilder().(*array.StructBuilder)
	xBuilder := structBuilder.FieldBuilder(0).(*array.Float64Builder)
	yBuilder := structBuilder.FieldBuilder(1).(*array.Float64Builder)

	// Polygon 0: triangle
	outerListBuilder.Append(true)
	innerListBuilder.Append(true)
	for _, xy := range [][2]float64{{0, 0}, {1, 0}, {0, 1}, {0, 0}} {
		structBuilder.Append(true)
		xBuilder.Append(xy[0])
		yBuilder.Append(xy[1])
	}

	// Polygon 1: null
	outerListBuilder.AppendNull()

	storage := outerListBuilder.NewArray()
	defer storage.Release()

	arr := array.NewExtensionArrayWithStorage(typ, storage)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, 1, arr.NullN())

	polyArr, ok := arr.(*geoarrow.PolygonArray)
	require.True(t, ok)

	v0 := polyArr.Value(0)
	require.Equal(t, 1, v0.NumRings())
	require.Equal(t, 4, v0.NumVertices(0))
}

func TestPolygonStringRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder.Release()

	builder.Append(testSquare)
	builder.AppendNull()
	builder.Append(testSquareWithHole)

	arr := builder.NewArray()
	defer arr.Release()

	// Rebuild from ValueStr
	builder2 := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder2.Release()

	for i := 0; i < arr.Len(); i++ {
		require.NoError(t, builder2.AppendValueFromString(arr.ValueStr(i)))
	}

	arr2 := builder2.NewArray()
	defer arr2.Release()

	require.True(t, array.Equal(arr, arr2))
}

func TestPolygonJSONRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder.Release()

	builder.Append(testSquare)
	builder.AppendNull()
	builder.Append(testSquareWithHole)

	arr := builder.NewArray()
	defer arr.Release()

	// Marshal
	jsonData, err := json.Marshal(arr)
	require.NoError(t, err)

	// Unmarshal via FromJSON
	arr2, _, err := array.FromJSON(mem, typ, bytes.NewReader(jsonData))
	require.NoError(t, err)
	defer arr2.Release()

	require.True(t, array.Equal(arr, arr2))
}

func TestPolygonIPCRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)

	builder.Append(testSquare)
	builder.AppendNull()
	builder.Append(testSquareWithHole)

	arr := builder.NewArray()
	defer arr.Release()
	builder.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "geom", Type: typ, Nullable: true}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{arr}, -1)
	defer batch.Release()

	var buf bytes.Buffer
	wr := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	require.NoError(t, wr.Write(batch))
	require.NoError(t, wr.Close())

	rdr, err := ipc.NewReader(&buf)
	require.NoError(t, err)
	written, err := rdr.Read()
	require.NoError(t, err)
	written.Retain()
	defer written.Release()
	rdr.Release()

	require.True(t, batch.Schema().Equal(written.Schema()))
	require.True(t, array.RecordEqual(batch, written))
}

func TestPolygonRecordBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geom", Type: typ},
	}, nil)
	recBuilder := array.NewRecordBuilder(mem, schema)
	defer recBuilder.Release()

	polyBuilder := recBuilder.Field(0).(*geoarrow.PolygonBuilder)
	polyBuilder.Append(testSquare)
	polyBuilder.AppendNull()

	record := recBuilder.NewRecordBatch()
	defer record.Release()

	require.Equal(t, int64(2), record.NumRows())
}

func TestPolygonRegistration(t *testing.T) {
	extType := arrow.GetExtensionType("geoarrow.polygon")
	require.NotNil(t, extType)
	require.Equal(t, "geoarrow.polygon", extType.ExtensionName())
}

func TestPolygonValuesMethod(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder.Release()

	builder.Append(testSquare)
	builder.Append(testSquareWithHole)

	arr := builder.NewArray()
	defer arr.Release()

	polyArr := arr.(*geoarrow.PolygonArray)
	values := polyArr.Values()
	require.Equal(t, 2, len(values))
	require.Equal(t, 1, values[0].NumRings())
	require.Equal(t, 2, values[1].NumRings())
}

func TestPolygonMarshalJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder.Release()

	builder.Append(testSquare)
	builder.AppendNull()

	arr := builder.NewArray()
	defer arr.Release()

	polyArr := arr.(*geoarrow.PolygonArray)
	b, err := polyArr.MarshalJSON()
	require.NoError(t, err)

	jsonStr := string(b)
	require.Contains(t, jsonStr, "null")
	require.Contains(t, jsonStr, "[0,0]")
	require.Contains(t, jsonStr, "[10,10]")
}
