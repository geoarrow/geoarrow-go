package geoarrowgeom_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/geoarrow/geoarrow-go"
	geoarrowgeom "github.com/geoarrow/geoarrow-go/gogeom"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestPointsToGeom(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPointType()
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValue(1.5, 2.5))
	builder.AppendNull()
	builder.Append(geoarrow.NewPointValue(3.0, 4.0))

	arr := builder.NewArray()
	defer arr.Release()

	pointArr := arr.(*geoarrow.PointArray)
	points := geoarrowgeom.PointsToGeom(pointArr)

	require.Equal(t, 3, len(points))

	// First point
	require.NotNil(t, points[0])
	require.Equal(t, 1.5, points[0].X())
	require.Equal(t, 2.5, points[0].Y())
	require.Equal(t, geom.XY, points[0].Layout())

	// Null
	require.Nil(t, points[1])

	// Third point
	require.NotNil(t, points[2])
	require.Equal(t, 3.0, points[2].X())
	require.Equal(t, 4.0, points[2].Y())
}

func TestPointsToGeomXYZ(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	xyzStorage := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "z", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
	typ := geoarrow.NewPointType(geoarrow.PointWithStorage(xyzStorage))
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	builder.Append(geoarrow.NewPointValueZ(1.0, 2.0, 3.0))

	arr := builder.NewArray()
	defer arr.Release()

	points := geoarrowgeom.PointsToGeom(arr.(*geoarrow.PointArray))
	require.Equal(t, 1, len(points))
	require.Equal(t, geom.XYZ, points[0].Layout())
	require.Equal(t, 3.0, points[0].Z())
}

func TestPointsFromGeom(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	points := []*geom.Point{
		geom.NewPointFlat(geom.XY, []float64{1.5, 2.5}),
		nil,
		geom.NewPointFlat(geom.XY, []float64{3.0, 4.0}),
	}

	typ := geoarrow.NewPointType()
	arr := geoarrowgeom.PointsFromGeom(mem, points, typ)
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	pointArr := arr.(*geoarrow.PointArray)
	v0 := pointArr.Value(0)
	require.Equal(t, 1.5, v0.X())
	require.Equal(t, 2.5, v0.Y())

	require.True(t, arr.IsNull(1))

	v2 := pointArr.Value(2)
	require.Equal(t, 3.0, v2.X())
	require.Equal(t, 4.0, v2.Y())
}

func TestPointsRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	original := []*geom.Point{
		geom.NewPointFlat(geom.XY, []float64{1.0, 2.0}),
		nil,
		geom.NewPointFlat(geom.XY, []float64{3.0, 4.0}),
		geom.NewPointFlat(geom.XY, []float64{5.0, 6.0}),
	}

	typ := geoarrow.NewPointType()
	arr := geoarrowgeom.PointsFromGeom(mem, original, typ)
	defer arr.Release()

	roundTripped := geoarrowgeom.PointsToGeom(arr.(*geoarrow.PointArray))

	require.Equal(t, len(original), len(roundTripped))
	for i, orig := range original {
		if orig == nil {
			require.Nil(t, roundTripped[i])
		} else {
			require.NotNil(t, roundTripped[i])
			require.InDelta(t, orig.X(), roundTripped[i].X(), 1e-10)
			require.InDelta(t, orig.Y(), roundTripped[i].Y(), 1e-10)
		}
	}
}

func TestPolygonsToGeom(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewPolygonType()
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder.Release()

	// Square
	builder.Append(geoarrow.NewPolygonValue(geoarrow.XY, [][]float64{
		{0, 0, 10, 0, 10, 10, 0, 10, 0, 0},
	}))
	builder.AppendNull()
	// Square with hole
	builder.Append(geoarrow.NewPolygonValue(geoarrow.XY, [][]float64{
		{0, 0, 10, 0, 10, 10, 0, 10, 0, 0},
		{2, 2, 8, 2, 8, 8, 2, 8, 2, 2},
	}))

	arr := builder.NewArray()
	defer arr.Release()

	polyArr := arr.(*geoarrow.PolygonArray)
	polygons := geoarrowgeom.PolygonsToGeom(polyArr)

	require.Equal(t, 3, len(polygons))

	// First polygon: simple square
	require.NotNil(t, polygons[0])
	require.Equal(t, geom.XY, polygons[0].Layout())
	require.Equal(t, 1, polygons[0].NumLinearRings())
	ring := polygons[0].LinearRing(0)
	require.Equal(t, 5, ring.NumCoords())

	// Null
	require.Nil(t, polygons[1])

	// Third: square with hole
	require.NotNil(t, polygons[2])
	require.Equal(t, 2, polygons[2].NumLinearRings())
	outerRing := polygons[2].LinearRing(0)
	require.Equal(t, 5, outerRing.NumCoords())
	innerRing := polygons[2].LinearRing(1)
	require.Equal(t, 5, innerRing.NumCoords())
}

func TestPolygonsFromGeom(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	polygons := []*geom.Polygon{
		geom.NewPolygonFlat(geom.XY,
			[]float64{0, 0, 10, 0, 10, 10, 0, 10, 0, 0},
			[]int{10}),
		nil,
		geom.NewPolygonFlat(geom.XY,
			[]float64{0, 0, 10, 0, 10, 10, 0, 10, 0, 0, 2, 2, 8, 2, 8, 8, 2, 8, 2, 2},
			[]int{10, 20}),
	}

	typ := geoarrow.NewPolygonType()
	arr := geoarrowgeom.PolygonsFromGeom(mem, polygons, typ)
	defer arr.Release()

	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	polyArr := arr.(*geoarrow.PolygonArray)

	v0 := polyArr.Value(0)
	require.Equal(t, 1, v0.NumRings())
	require.Equal(t, 5, v0.NumVertices(0))

	require.True(t, arr.IsNull(1))

	v2 := polyArr.Value(2)
	require.Equal(t, 2, v2.NumRings())
}

func TestPolygonsRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	original := []*geom.Polygon{
		geom.NewPolygonFlat(geom.XY,
			[]float64{0, 0, 10, 0, 10, 10, 0, 10, 0, 0},
			[]int{10}),
		nil,
		geom.NewPolygonFlat(geom.XY,
			[]float64{0, 0, 10, 0, 10, 10, 0, 10, 0, 0, 2, 2, 8, 2, 8, 8, 2, 8, 2, 2},
			[]int{10, 20}),
	}

	typ := geoarrow.NewPolygonType()
	arr := geoarrowgeom.PolygonsFromGeom(mem, original, typ)
	defer arr.Release()

	roundTripped := geoarrowgeom.PolygonsToGeom(arr.(*geoarrow.PolygonArray))

	require.Equal(t, len(original), len(roundTripped))
	for i, orig := range original {
		if orig == nil {
			require.Nil(t, roundTripped[i])
		} else {
			require.NotNil(t, roundTripped[i])
			require.Equal(t, orig.NumLinearRings(), roundTripped[i].NumLinearRings())
			require.InDeltaSlice(t, orig.FlatCoords(), roundTripped[i].FlatCoords(), 1e-10)
		}
	}
}

func TestPointsIPCRoundTripWithConversion(t *testing.T) {
	// Verify the full flow: go-geom → geoarrow → Arrow IPC → geoarrow → go-geom
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	original := []*geom.Point{
		geom.NewPointFlat(geom.XY, []float64{-122.4194, 37.7749}),
		geom.NewPointFlat(geom.XY, []float64{-73.9857, 40.7484}),
	}

	typ := geoarrow.NewPointType()
	arr := geoarrowgeom.PointsFromGeom(mem, original, typ)
	defer arr.Release()

	// Write to IPC and read back
	schema := arrow.NewSchema([]arrow.Field{{Name: "location", Type: typ, Nullable: true}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{arr}, -1)
	defer batch.Release()

	var buf []byte
	{
		var b = new(bytes.Buffer)
		wr := ipc.NewWriter(b, ipc.WithSchema(schema))
		require.NoError(t, wr.Write(batch))
		require.NoError(t, wr.Close())
		buf = b.Bytes()
	}

	rdr, err := ipc.NewReader(bytes.NewReader(buf))
	require.NoError(t, err)
	readBatch, err := rdr.Read()
	require.NoError(t, err)
	readBatch.Retain()
	defer readBatch.Release()
	rdr.Release()

	// Convert back to go-geom
	readArr := readBatch.Column(0).(*geoarrow.PointArray)
	roundTripped := geoarrowgeom.PointsToGeom(readArr)

	require.Equal(t, 2, len(roundTripped))
	require.InDelta(t, -122.4194, roundTripped[0].X(), 1e-10)
	require.InDelta(t, 37.7749, roundTripped[0].Y(), 1e-10)
	require.InDelta(t, -73.9857, roundTripped[1].X(), 1e-10)
	require.InDelta(t, 40.7484, roundTripped[1].Y(), 1e-10)
}
