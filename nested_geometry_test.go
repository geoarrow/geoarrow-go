package geoarrow_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/geoarrow/geoarrow-go"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// Tests for geoarrow.linestring, geoarrow.multipoint, geoarrow.multilinestring
// and geoarrow.multipolygon. They share the same List-of-coord storage shape,
// so the round-trip checks are shared and the per-type tests only differ in how
// values are built and what accessors they exercise.

var allDims = []geoarrow.Dimension{geoarrow.XY, geoarrow.XYZ, geoarrow.XYM, geoarrow.XYZM}

// flatCoords builds a flat interleaved slice for dim from (x, y) pairs. Extra
// ordinates (z and/or m) are derived from x so every vertex is distinct.
func flatCoords(dim geoarrow.Dimension, xy ...[2]float64) []float64 {
	out := make([]float64, 0, len(xy)*dim.NDim())
	for _, p := range xy {
		out = append(out, p[0], p[1])
		for k := 2; k < dim.NDim(); k++ {
			out = append(out, p[0]*10+float64(k))
		}
	}
	return out
}

type layout struct {
	name       string
	interleave bool
}

var layouts = []layout{{"struct", false}, {"interleaved", true}}

// checkRoundTrips asserts that arr survives each serialization path that the
// geoarrow extension types support: WKT strings (ValueStr/AppendValueFromString),
// JSON, Arrow IPC, and type metadata (Serialize/Deserialize).
func checkRoundTrips(t *testing.T, mem memory.Allocator, typ arrow.ExtensionType, arr arrow.Array) {
	t.Helper()

	t.Run("wkt", func(t *testing.T) {
		builder := typ.(array.CustomExtensionBuilder).NewBuilder(mem)
		defer builder.Release()
		for i := range arr.Len() {
			require.NoError(t, builder.AppendValueFromString(arr.ValueStr(i)), arr.ValueStr(i))
		}
		got := builder.NewArray()
		defer got.Release()
		require.True(t, array.Equal(arr, got))
	})

	t.Run("json", func(t *testing.T) {
		data, err := json.Marshal(arr)
		require.NoError(t, err)
		got, _, err := array.FromJSON(mem, typ, bytes.NewReader(data))
		require.NoError(t, err)
		defer got.Release()
		require.True(t, array.Equal(arr, got))
	})

	t.Run("ipc", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "geom", Type: typ, Nullable: true}}, nil)
		batch := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(arr.Len()))
		defer batch.Release()

		var buf bytes.Buffer
		wr := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
		require.NoError(t, wr.Write(batch))
		require.NoError(t, wr.Close())

		rdr, err := ipc.NewReader(&buf, ipc.WithAllocator(mem))
		require.NoError(t, err)
		defer rdr.Release()
		require.True(t, rdr.Next())
		got := rdr.RecordBatch()
		require.True(t, schema.Equal(got.Schema()), "schema did not round trip: %s", got.Schema())
		require.True(t, array.RecordEqual(batch, got))
	})

	t.Run("metadata", func(t *testing.T) {
		got, err := typ.Deserialize(typ.StorageType(), typ.Serialize())
		require.NoError(t, err)
		require.True(t, typ.ExtensionEquals(got))
	})
}

func TestLineStringValue(t *testing.T) {
	for _, dim := range allDims {
		t.Run(dim.String(), func(t *testing.T) {
			coords := flatCoords(dim, [2]float64{0, 0}, [2]float64{1, 1}, [2]float64{2, 0})
			v := geoarrow.NewLineStringValue(dim, coords)

			require.Equal(t, 3, v.NumVertices())
			require.Equal(t, dim, v.Dimension())
			require.Equal(t, coords, v.Coords())
			require.Equal(t, coords[dim.NDim():2*dim.NDim()], v.Vertex(1))
			require.False(t, v.IsEmpty())
			require.Contains(t, v.String(), "LINESTRING")
		})
	}

	require.Equal(t, geoarrow.LineStringID, geoarrow.NewLineStringValue(geoarrow.XY, nil).GeometryType())
	require.Equal(t, geoarrow.LineStringZID, geoarrow.NewLineStringValue(geoarrow.XYZ, nil).GeometryType())
	require.Equal(t, geoarrow.LineStringMID, geoarrow.NewLineStringValue(geoarrow.XYM, nil).GeometryType())
	require.Equal(t, geoarrow.LineStringZMID, geoarrow.NewLineStringValue(geoarrow.XYZM, nil).GeometryType())

	empty := geoarrow.LineStringValue{}
	require.True(t, empty.IsEmpty())
	require.Equal(t, "LINESTRING EMPTY", empty.String())
}

func TestLineStringArray(t *testing.T) {
	for _, dim := range allDims {
		for _, l := range layouts {
			t.Run(dim.String()+"/"+l.name, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)

				typ := geoarrow.NewLineStringType(geoarrow.LineStringWithDimension(dim))
				if l.interleave {
					typ = geoarrow.NewLineStringType(geoarrow.LineStringWithInterleaved(dim))
				}
				line := geoarrow.NewLineStringValue(dim, flatCoords(dim, [2]float64{30, 10}, [2]float64{10, 30}, [2]float64{40, 40}))

				b := typ.NewBuilder(mem).(*geoarrow.LineStringBuilder)
				defer b.Release()
				b.Append(line)
				b.AppendNull()
				b.Append(geoarrow.NewLineStringValue(dim, nil)) // empty, distinct from null
				b.Append(geoarrow.NewLineStringValue(dim, flatCoords(dim, [2]float64{1, 2}, [2]float64{3, 4})))

				arr := b.NewArray()
				defer arr.Release()
				require.Equal(t, 4, arr.Len())
				require.Equal(t, 1, arr.NullN())

				la := arr.(*geoarrow.LineStringArray)
				require.Equal(t, line.Coords(), la.Value(0).Coords())
				require.Equal(t, dim, la.Value(0).Dimension())
				require.True(t, arr.IsNull(1))
				require.True(t, arr.IsValid(2))
				require.True(t, la.Value(2).IsEmpty())
				require.Equal(t, 2, la.Value(3).NumVertices())
				require.Len(t, la.Values(), 4)

				checkRoundTrips(t, mem, typ, arr)
			})
		}
	}
}

func TestMultiPointValue(t *testing.T) {
	for _, dim := range allDims {
		t.Run(dim.String(), func(t *testing.T) {
			coords := flatCoords(dim, [2]float64{0, 0}, [2]float64{5, 6})
			v := geoarrow.NewMultiPointValue(dim, coords)

			require.Equal(t, 2, v.NumPoints())
			require.Equal(t, dim, v.Dimension())
			require.Equal(t, coords, v.Coords())
			p := v.Point(1)
			require.Equal(t, 5.0, p.X())
			require.Equal(t, 6.0, p.Y())
			require.Equal(t, dim, p.Dimension())
			require.Contains(t, v.String(), "MULTIPOINT")
		})
	}

	require.Equal(t, geoarrow.MultiPointID, geoarrow.NewMultiPointValue(geoarrow.XY, nil).GeometryType())
	require.Equal(t, geoarrow.MultiPointZID, geoarrow.NewMultiPointValue(geoarrow.XYZ, nil).GeometryType())
	require.Equal(t, geoarrow.MultiPointMID, geoarrow.NewMultiPointValue(geoarrow.XYM, nil).GeometryType())
	require.Equal(t, geoarrow.MultiPointZMID, geoarrow.NewMultiPointValue(geoarrow.XYZM, nil).GeometryType())

	empty := geoarrow.MultiPointValue{}
	require.True(t, empty.IsEmpty())
	require.Equal(t, "MULTIPOINT EMPTY", empty.String())
}

func TestMultiPointArray(t *testing.T) {
	for _, dim := range allDims {
		for _, l := range layouts {
			t.Run(dim.String()+"/"+l.name, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)

				typ := geoarrow.NewMultiPointType(geoarrow.MultiPointWithDimension(dim))
				if l.interleave {
					typ = geoarrow.NewMultiPointType(geoarrow.MultiPointWithInterleaved(dim))
				}
				mp := geoarrow.NewMultiPointValue(dim, flatCoords(dim, [2]float64{10, 40}, [2]float64{40, 30}, [2]float64{20, 20}))

				b := typ.NewBuilder(mem).(*geoarrow.MultiPointBuilder)
				defer b.Release()
				b.AppendValues([]geoarrow.MultiPointValue{mp, {}, geoarrow.NewMultiPointValue(dim, flatCoords(dim, [2]float64{1, 1}))}, []bool{true, false, true})

				arr := b.NewArray()
				defer arr.Release()
				require.Equal(t, 3, arr.Len())
				require.Equal(t, 1, arr.NullN())

				ma := arr.(*geoarrow.MultiPointArray)
				require.Equal(t, mp.Coords(), ma.Value(0).Coords())
				require.Equal(t, 1, ma.Value(2).NumPoints())

				checkRoundTrips(t, mem, typ, arr)
			})
		}
	}
}

func TestMultiLineStringValue(t *testing.T) {
	for _, dim := range allDims {
		t.Run(dim.String(), func(t *testing.T) {
			a := flatCoords(dim, [2]float64{0, 0}, [2]float64{1, 1})
			b := flatCoords(dim, [2]float64{2, 2}, [2]float64{3, 3}, [2]float64{4, 4})
			v := geoarrow.NewMultiLineStringValue(dim, [][]float64{a, b})

			require.Equal(t, 2, v.NumLineStrings())
			require.Equal(t, dim, v.Dimension())
			require.Equal(t, 3, v.LineString(1).NumVertices())
			require.Equal(t, a, v.LineString(0).Coords())
			require.Contains(t, v.String(), "MULTILINESTRING")
		})
	}

	require.Equal(t, geoarrow.MultiLineStringID, geoarrow.NewMultiLineStringValue(geoarrow.XY, nil).GeometryType())
	require.Equal(t, geoarrow.MultiLineStringZID, geoarrow.NewMultiLineStringValue(geoarrow.XYZ, nil).GeometryType())
	require.Equal(t, geoarrow.MultiLineStringMID, geoarrow.NewMultiLineStringValue(geoarrow.XYM, nil).GeometryType())
	require.Equal(t, geoarrow.MultiLineStringZMID, geoarrow.NewMultiLineStringValue(geoarrow.XYZM, nil).GeometryType())

	empty := geoarrow.MultiLineStringValue{}
	require.True(t, empty.IsEmpty())
	require.Equal(t, "MULTILINESTRING EMPTY", empty.String())
}

func TestMultiLineStringArray(t *testing.T) {
	for _, dim := range allDims {
		for _, l := range layouts {
			t.Run(dim.String()+"/"+l.name, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)

				typ := geoarrow.NewMultiLineStringType(geoarrow.MultiLineStringWithDimension(dim))
				if l.interleave {
					typ = geoarrow.NewMultiLineStringType(geoarrow.MultiLineStringWithInterleaved(dim))
				}
				one := geoarrow.NewMultiLineStringValue(dim, [][]float64{
					flatCoords(dim, [2]float64{30, 10}, [2]float64{10, 30}, [2]float64{40, 40}),
				})
				two := geoarrow.NewMultiLineStringValue(dim, [][]float64{
					flatCoords(dim, [2]float64{10, 10}, [2]float64{20, 20}),
					flatCoords(dim, [2]float64{40, 40}, [2]float64{30, 30}, [2]float64{40, 20}),
				})

				b := typ.NewBuilder(mem).(*geoarrow.MultiLineStringBuilder)
				defer b.Release()
				b.Append(one)
				b.AppendNull()
				b.Append(two)
				b.Append(geoarrow.NewMultiLineStringValue(dim, nil))

				arr := b.NewArray()
				defer arr.Release()
				require.Equal(t, 4, arr.Len())
				require.Equal(t, 1, arr.NullN())

				ma := arr.(*geoarrow.MultiLineStringArray)
				require.Equal(t, 1, ma.Value(0).NumLineStrings())
				require.Equal(t, 2, ma.Value(2).NumLineStrings())
				require.Equal(t, 3, ma.Value(2).LineString(1).NumVertices())
				require.Equal(t, two.LineString(1).Coords(), ma.Value(2).LineString(1).Coords())
				require.True(t, ma.Value(3).IsEmpty())

				checkRoundTrips(t, mem, typ, arr)
			})
		}
	}
}

func TestMultiPolygonValue(t *testing.T) {
	for _, dim := range allDims {
		t.Run(dim.String(), func(t *testing.T) {
			shell := flatCoords(dim, [2]float64{0, 0}, [2]float64{10, 0}, [2]float64{0, 10}, [2]float64{0, 0})
			hole := flatCoords(dim, [2]float64{1, 1}, [2]float64{2, 1}, [2]float64{1, 2}, [2]float64{1, 1})
			other := flatCoords(dim, [2]float64{20, 20}, [2]float64{30, 20}, [2]float64{20, 30}, [2]float64{20, 20})
			v := geoarrow.NewMultiPolygonValue(dim, [][][]float64{{shell, hole}, {other}})

			require.Equal(t, 2, v.NumPolygons())
			require.Equal(t, dim, v.Dimension())
			require.Equal(t, 2, v.Polygon(0).NumRings())
			require.Equal(t, 4, v.Polygon(0).NumVertices(1))
			require.Equal(t, other, v.Polygon(1).Ring(0))
			require.Contains(t, v.String(), "MULTIPOLYGON")
		})
	}

	require.Equal(t, geoarrow.MultiPolygonID, geoarrow.NewMultiPolygonValue(geoarrow.XY, nil).GeometryType())
	require.Equal(t, geoarrow.MultiPolygonZID, geoarrow.NewMultiPolygonValue(geoarrow.XYZ, nil).GeometryType())
	require.Equal(t, geoarrow.MultiPolygonMID, geoarrow.NewMultiPolygonValue(geoarrow.XYM, nil).GeometryType())
	require.Equal(t, geoarrow.MultiPolygonZMID, geoarrow.NewMultiPolygonValue(geoarrow.XYZM, nil).GeometryType())

	empty := geoarrow.MultiPolygonValue{}
	require.True(t, empty.IsEmpty())
	require.Equal(t, "MULTIPOLYGON EMPTY", empty.String())
}

func TestMultiPolygonArray(t *testing.T) {
	for _, dim := range allDims {
		for _, l := range layouts {
			t.Run(dim.String()+"/"+l.name, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)

				typ := geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithDimension(dim))
				if l.interleave {
					typ = geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithInterleaved(dim))
				}
				tri := func(x float64) []float64 {
					return flatCoords(dim, [2]float64{x, 0}, [2]float64{x + 1, 0}, [2]float64{x, 1}, [2]float64{x, 0})
				}
				one := geoarrow.NewMultiPolygonValue(dim, [][][]float64{{tri(0)}})
				two := geoarrow.NewMultiPolygonValue(dim, [][][]float64{{tri(10), tri(11)}, {tri(20)}})

				b := typ.NewBuilder(mem).(*geoarrow.MultiPolygonBuilder)
				defer b.Release()
				b.Append(one)
				b.AppendNull()
				b.Append(two)
				b.Append(geoarrow.NewMultiPolygonValue(dim, nil))
				b.Append(one)

				arr := b.NewArray()
				defer arr.Release()
				require.Equal(t, 5, arr.Len())
				require.Equal(t, 1, arr.NullN())

				ma := arr.(*geoarrow.MultiPolygonArray)
				require.Equal(t, 1, ma.Value(0).NumPolygons())
				got := ma.Value(2)
				require.Equal(t, 2, got.NumPolygons())
				require.Equal(t, 2, got.Polygon(0).NumRings())
				require.Equal(t, tri(11), got.Polygon(0).Ring(1))
				require.Equal(t, tri(20), got.Polygon(1).Ring(0))
				require.True(t, ma.Value(3).IsEmpty())
				require.Equal(t, tri(0), ma.Value(4).Polygon(0).Ring(0))

				checkRoundTrips(t, mem, typ, arr)
			})
		}
	}
}

// Slicing an array must not leak values from outside the slice: offsets in the
// child arrays stay absolute.
func TestNestedGeometrySlice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewMultiPolygonType()
	b := typ.NewBuilder(mem).(*geoarrow.MultiPolygonBuilder)
	defer b.Release()
	for i := range 4 {
		x := float64(i * 10)
		b.Append(geoarrow.NewMultiPolygonValue(geoarrow.XY, [][][]float64{
			{{x, 0, x + 1, 0, x, 1, x, 0}},
		}))
	}
	arr := b.NewArray()
	defer arr.Release()

	sliced := array.NewSlice(arr, 2, 4).(*geoarrow.MultiPolygonArray)
	defer sliced.Release()
	require.Equal(t, 2, sliced.Len())
	require.Equal(t, 20.0, sliced.Value(0).Polygon(0).Ring(0)[0])
	require.Equal(t, 30.0, sliced.Value(1).Polygon(0).Ring(0)[0])
}

func TestNestedGeometryRegistration(t *testing.T) {
	for _, name := range []string{
		geoarrow.ExtensionNameLineString,
		geoarrow.ExtensionNameMultiPoint,
		geoarrow.ExtensionNameMultiLineString,
		geoarrow.ExtensionNameMultiPolygon,
	} {
		t.Run(name, func(t *testing.T) {
			typ := arrow.GetExtensionType(name)
			require.NotNil(t, typ)
			require.Equal(t, name, typ.ExtensionName())
		})
	}
}

func TestNestedGeometryTypeBasics(t *testing.T) {
	// Storage shape and field names are mandated by the GeoArrow spec.
	cases := []struct {
		name  string
		typ   arrow.ExtensionType
		names []string
	}{
		{"geoarrow.linestring", geoarrow.NewLineStringType(), []string{"vertices"}},
		{"geoarrow.multipoint", geoarrow.NewMultiPointType(), []string{"points"}},
		{"geoarrow.multilinestring", geoarrow.NewMultiLineStringType(), []string{"linestrings", "vertices"}},
		{"geoarrow.multipolygon", geoarrow.NewMultiPolygonType(), []string{"polygons", "rings", "vertices"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.name, tc.typ.ExtensionName())
			require.True(t, tc.typ.ExtensionEquals(tc.typ))

			var cur = tc.typ.StorageType()
			for _, want := range tc.names {
				lt, ok := cur.(*arrow.ListType)
				require.True(t, ok, "expected list, got %s", cur)
				require.Equal(t, want, lt.ElemField().Name)
				require.False(t, lt.ElemField().Nullable)
				cur = lt.Elem()
			}
			coord, ok := cur.(*arrow.StructType)
			require.True(t, ok)
			require.Equal(t, 2, coord.NumFields())
			require.Equal(t, "x", coord.Field(0).Name)
			require.Equal(t, "y", coord.Field(1).Name)
		})
	}

	// Types are not interchangeable even when the storage depth matches.
	require.False(t, geoarrow.NewLineStringType().ExtensionEquals(geoarrow.NewMultiPointType()))
	require.False(t, geoarrow.NewMultiPointType().ExtensionEquals(geoarrow.NewLineStringType()))
	require.False(t, geoarrow.NewPolygonType().ExtensionEquals(geoarrow.NewMultiLineStringType()))
	require.False(t, geoarrow.NewMultiLineStringType().ExtensionEquals(geoarrow.NewPolygonType()))

	// Metadata participates in equality.
	spherical := geoarrow.Metadata{Edges: geoarrow.EdgeSpherical}
	require.False(t, geoarrow.NewLineStringType().ExtensionEquals(
		geoarrow.NewLineStringType(geoarrow.LineStringWithMetadata(spherical))))
	require.True(t, geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithMetadata(spherical)).ExtensionEquals(
		geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithMetadata(spherical))))
}

func TestNestedGeometryDeserializeRejectsBadStorage(t *testing.T) {
	list := func(name string, elem arrow.DataType) arrow.DataType {
		return arrow.ListOfField(arrow.Field{Name: name, Type: elem})
	}
	cases := []struct {
		name string
		typ  arrow.ExtensionType
		deep int // number of List layers the type requires
	}{
		{"linestring", geoarrow.NewLineStringType(), 1},
		{"multipoint", geoarrow.NewMultiPointType(), 1},
		{"multilinestring", geoarrow.NewMultiLineStringType(), 2},
		{"multipolygon", geoarrow.NewMultiPolygonType(), 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Not a list at all.
			_, err := tc.typ.Deserialize(arrow.PrimitiveTypes.Float64, "{}")
			require.Error(t, err)

			// Right number of lists, but the leaf is not a coord.
			bad := arrow.DataType(arrow.PrimitiveTypes.Float64)
			for range tc.deep {
				bad = list("x", bad)
			}
			_, err = tc.typ.Deserialize(bad, "{}")
			require.Error(t, err)

			// One list too few: the leaf is a list, not a coord.
			short := arrow.DataType(coordStruct())
			for range tc.deep - 1 {
				short = list("x", short)
			}
			_, err = tc.typ.Deserialize(short, "{}")
			require.Error(t, err)

			// Malformed metadata.
			_, err = tc.typ.Deserialize(tc.typ.StorageType(), "not json")
			require.Error(t, err)
		})
	}
}

func coordStruct() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	)
}

func TestNestedGeometryWKTParsing(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	t.Run("multipoint accepts both member syntaxes", func(t *testing.T) {
		typ := geoarrow.NewMultiPointType()
		b := typ.NewBuilder(mem).(*geoarrow.MultiPointBuilder)
		defer b.Release()
		require.NoError(t, b.AppendValueFromString("MULTIPOINT ((10 40), (40 30))"))
		require.NoError(t, b.AppendValueFromString("MULTIPOINT (10 40, 40 30)"))
		require.NoError(t, b.AppendValueFromString("multipoint(10 40,40 30)"))
		arr := b.NewArray()
		defer arr.Release()
		ma := arr.(*geoarrow.MultiPointArray)
		for i := range 3 {
			require.Equal(t, []float64{10, 40, 40, 30}, ma.Value(i).Coords(), i)
		}
	})

	t.Run("empty forms", func(t *testing.T) {
		for _, tc := range []struct {
			wkt  string
			typ  arrow.ExtensionType
			dim  geoarrow.Dimension
			want geoarrow.GeometryTypeID
		}{
			{"LINESTRING EMPTY", geoarrow.NewLineStringType(), geoarrow.XY, geoarrow.LineStringID},
			{"MULTIPOINT EMPTY", geoarrow.NewMultiPointType(), geoarrow.XY, geoarrow.MultiPointID},
			{"MULTILINESTRING EMPTY", geoarrow.NewMultiLineStringType(), geoarrow.XY, geoarrow.MultiLineStringID},
			{"MULTIPOLYGON EMPTY", geoarrow.NewMultiPolygonType(), geoarrow.XY, geoarrow.MultiPolygonID},
			{"LINESTRING Z EMPTY", geoarrow.NewLineStringType(geoarrow.LineStringWithDimension(geoarrow.XYZ)), geoarrow.XYZ, geoarrow.LineStringZID},
			{"MULTIPOLYGON ZM EMPTY", geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithDimension(geoarrow.XYZM)), geoarrow.XYZM, geoarrow.MultiPolygonZMID},
		} {
			t.Run(tc.wkt, func(t *testing.T) {
				b := tc.typ.(array.CustomExtensionBuilder).NewBuilder(mem)
				defer b.Release()
				require.NoError(t, b.AppendValueFromString(tc.wkt))
				arr := b.NewArray()
				defer arr.Release()
				require.Equal(t, 1, arr.Len())
				require.Equal(t, 0, arr.NullN())
				// The empty value adopts the storage's dimension.
				type geom interface {
					Dimension() geoarrow.Dimension
					GeometryType() geoarrow.GeometryTypeID
					IsEmpty() bool
				}
				var g geom
				switch a := arr.(type) {
				case *geoarrow.LineStringArray:
					g = a.Value(0)
				case *geoarrow.MultiPointArray:
					g = a.Value(0)
				case *geoarrow.MultiLineStringArray:
					g = a.Value(0)
				case *geoarrow.MultiPolygonArray:
					g = a.Value(0)
				}
				require.True(t, g.IsEmpty())
				require.Equal(t, tc.dim, g.Dimension())
				require.Equal(t, tc.want, g.GeometryType())
			})
		}
	})

	t.Run("empty members survive a string round trip", func(t *testing.T) {
		typ := geoarrow.NewMultiPolygonType()
		b := typ.NewBuilder(mem).(*geoarrow.MultiPolygonBuilder)
		defer b.Release()
		require.NoError(t, b.AppendValueFromString("MULTIPOLYGON (EMPTY, ((0 0, 1 0, 0 1, 0 0)))"))
		arr := b.NewArray()
		defer arr.Release()
		got := arr.(*geoarrow.MultiPolygonArray).Value(0)
		require.Equal(t, 2, got.NumPolygons())
		require.True(t, got.Polygon(0).IsEmpty())
		require.Equal(t, "MULTIPOLYGON(EMPTY, ((0.000000 0.000000, 1.000000 0.000000, 0.000000 1.000000, 0.000000 0.000000)))", got.String())

		b2 := typ.NewBuilder(mem).(*geoarrow.MultiPolygonBuilder)
		defer b2.Release()
		require.NoError(t, b2.AppendValueFromString(got.String()))
		arr2 := b2.NewArray()
		defer arr2.Release()
		require.True(t, array.Equal(arr, arr2))

		ml := geoarrow.NewMultiLineStringType()
		mb := ml.NewBuilder(mem).(*geoarrow.MultiLineStringBuilder)
		defer mb.Release()
		require.NoError(t, mb.AppendValueFromString("MULTILINESTRING (EMPTY, (0 0, 1 1))"))
		marr := mb.NewArray()
		defer marr.Release()
		require.Equal(t, 2, marr.(*geoarrow.MultiLineStringArray).Value(0).NumLineStrings())
	})

	t.Run("errors", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			typ  arrow.ExtensionType
			wkt  string
		}{
			{"wrong keyword", geoarrow.NewLineStringType(), "POINT (1 2)"},
			{"keyword prefix only", geoarrow.NewLineStringType(), "LINESTRINGZ (1 2, 3 4)"},
			{"missing parens", geoarrow.NewLineStringType(), "LINESTRING 1 2, 3 4"},
			{"bad number", geoarrow.NewLineStringType(), "LINESTRING (a b, 3 4)"},
			{"ragged stride", geoarrow.NewLineStringType(), "LINESTRING (1 2, 3 4 5)"},
			{"z into xy storage", geoarrow.NewLineStringType(), "LINESTRING Z (1 2 3, 4 5 6)"},
			{"xy into z storage", geoarrow.NewMultiPointType(geoarrow.MultiPointWithDimension(geoarrow.XYZ)), "MULTIPOINT ((1 2))"},
			{"multipoint empty member", geoarrow.NewMultiPointType(), "MULTIPOINT (EMPTY, (1 2))"},
			{"multipoint multi-vertex member", geoarrow.NewMultiPointType(), "MULTIPOINT ((1 2, 3 4))"},
			{"multilinestring unbalanced", geoarrow.NewMultiLineStringType(), "MULTILINESTRING ((1 2, 3 4)"},
			{"multilinestring ragged members", geoarrow.NewMultiLineStringType(), "MULTILINESTRING ((1 2, 3 4), (1 2 3, 4 5 6))"},
			{"multilinestring unwrapped member", geoarrow.NewMultiLineStringType(), "MULTILINESTRING (1 2, 3 4)"},
			{"multipolygon too shallow", geoarrow.NewMultiPolygonType(), "MULTIPOLYGON ((0 0, 1 0, 0 1, 0 0))"},
			{"multipolygon stray comma", geoarrow.NewMultiPolygonType(), "MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)),)"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				b := tc.typ.(array.CustomExtensionBuilder).NewBuilder(mem)
				defer b.Release()
				require.Error(t, b.AppendValueFromString(tc.wkt))
				// A failed parse must not leave a partial value behind.
				require.Equal(t, 0, b.Len())
			})
		}
	})
}

func TestNestedGeometryJSONShape(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	build := func(typ arrow.ExtensionType, fn func(b array.Builder)) string {
		b := typ.(array.CustomExtensionBuilder).NewBuilder(mem)
		defer b.Release()
		fn(b)
		arr := b.NewArray()
		defer arr.Release()
		data, err := json.Marshal(arr)
		require.NoError(t, err)
		return string(data)
	}

	require.JSONEq(t, `[[[0,0],[1,1]], null]`, build(geoarrow.NewLineStringType(), func(b array.Builder) {
		lb := b.(*geoarrow.LineStringBuilder)
		lb.Append(geoarrow.NewLineStringValue(geoarrow.XY, []float64{0, 0, 1, 1}))
		lb.AppendNull()
	}))
	require.JSONEq(t, `[[[0,0],[1,1]]]`, build(geoarrow.NewMultiPointType(), func(b array.Builder) {
		b.(*geoarrow.MultiPointBuilder).Append(geoarrow.NewMultiPointValue(geoarrow.XY, []float64{0, 0, 1, 1}))
	}))
	require.JSONEq(t, `[[[[0,0],[1,1]],[[2,2],[3,3]]]]`, build(geoarrow.NewMultiLineStringType(), func(b array.Builder) {
		b.(*geoarrow.MultiLineStringBuilder).Append(geoarrow.NewMultiLineStringValue(geoarrow.XY, [][]float64{{0, 0, 1, 1}, {2, 2, 3, 3}}))
	}))
	require.JSONEq(t, `[[[[[0,0],[1,0],[0,1],[0,0]]]]]`, build(geoarrow.NewMultiPolygonType(), func(b array.Builder) {
		b.(*geoarrow.MultiPolygonBuilder).Append(geoarrow.NewMultiPolygonValue(geoarrow.XY, [][][]float64{{{0, 0, 1, 0, 0, 1, 0, 0}}}))
	}))
}

// nestedFactories builds each new nested type for a dimension and layout.
var nestedFactories = []struct {
	name string
	make func(dim geoarrow.Dimension, interleaved bool) arrow.ExtensionType
	// wrongDim appends a value of dimension dim to builder b.
	appendValue func(b array.Builder, dim geoarrow.Dimension)
	appendEmpty func(b array.Builder, dim geoarrow.Dimension)
}{
	{
		"linestring",
		func(d geoarrow.Dimension, il bool) arrow.ExtensionType {
			if il {
				return geoarrow.NewLineStringType(geoarrow.LineStringWithInterleaved(d))
			}
			return geoarrow.NewLineStringType(geoarrow.LineStringWithDimension(d))
		},
		func(b array.Builder, d geoarrow.Dimension) {
			b.(*geoarrow.LineStringBuilder).Append(geoarrow.NewLineStringValue(d, flatCoords(d, [2]float64{1, 2})))
		},
		func(b array.Builder, d geoarrow.Dimension) {
			b.(*geoarrow.LineStringBuilder).Append(geoarrow.NewLineStringValue(d, nil))
		},
	},
	{
		"multipoint",
		func(d geoarrow.Dimension, il bool) arrow.ExtensionType {
			if il {
				return geoarrow.NewMultiPointType(geoarrow.MultiPointWithInterleaved(d))
			}
			return geoarrow.NewMultiPointType(geoarrow.MultiPointWithDimension(d))
		},
		func(b array.Builder, d geoarrow.Dimension) {
			b.(*geoarrow.MultiPointBuilder).Append(geoarrow.NewMultiPointValue(d, flatCoords(d, [2]float64{1, 2})))
		},
		func(b array.Builder, d geoarrow.Dimension) {
			b.(*geoarrow.MultiPointBuilder).Append(geoarrow.NewMultiPointValue(d, nil))
		},
	},
	{
		"multilinestring",
		func(d geoarrow.Dimension, il bool) arrow.ExtensionType {
			if il {
				return geoarrow.NewMultiLineStringType(geoarrow.MultiLineStringWithInterleaved(d))
			}
			return geoarrow.NewMultiLineStringType(geoarrow.MultiLineStringWithDimension(d))
		},
		func(b array.Builder, d geoarrow.Dimension) {
			b.(*geoarrow.MultiLineStringBuilder).Append(geoarrow.NewMultiLineStringValue(d, [][]float64{flatCoords(d, [2]float64{1, 2})}))
		},
		func(b array.Builder, d geoarrow.Dimension) {
			b.(*geoarrow.MultiLineStringBuilder).Append(geoarrow.NewMultiLineStringValue(d, nil))
		},
	},
	{
		"multipolygon",
		func(d geoarrow.Dimension, il bool) arrow.ExtensionType {
			if il {
				return geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithInterleaved(d))
			}
			return geoarrow.NewMultiPolygonType(geoarrow.MultiPolygonWithDimension(d))
		},
		func(b array.Builder, d geoarrow.Dimension) {
			b.(*geoarrow.MultiPolygonBuilder).Append(geoarrow.NewMultiPolygonValue(d, [][][]float64{{flatCoords(d, [2]float64{1, 2})}}))
		},
		func(b array.Builder, d geoarrow.Dimension) {
			b.(*geoarrow.MultiPolygonBuilder).Append(geoarrow.NewMultiPolygonValue(d, nil))
		},
	},
}

// Types with different coordinate dimension or layout are different types, but
// field names and nullability do not matter: the spec requires readers to
// accept other names.
func TestNestedGeometryExtensionEquals(t *testing.T) {
	for _, f := range nestedFactories {
		t.Run(f.name, func(t *testing.T) {
			for _, a := range allDims {
				for _, aIL := range []bool{false, true} {
					for _, b := range allDims {
						for _, bIL := range []bool{false, true} {
							want := a == b && aIL == bIL
							got := f.make(a, aIL).ExtensionEquals(f.make(b, bIL))
							require.Equal(t, want, got, "%s interleaved=%v vs %s interleaved=%v", a, aIL, b, bIL)
						}
					}
				}
			}
		})
	}

	t.Run("names and nullability are ignored", func(t *testing.T) {
		// Same shape as the default linestring, but with other field names
		// and nullable fields.
		f64 := arrow.PrimitiveTypes.Float64
		other := arrow.ListOfField(arrow.Field{Name: "pts", Nullable: true, Type: arrow.StructOf(
			arrow.Field{Name: "x", Type: f64, Nullable: true},
			arrow.Field{Name: "y", Type: f64, Nullable: true},
		)})
		got, err := geoarrow.NewLineStringType().Deserialize(other, "{}")
		require.NoError(t, err)
		require.True(t, geoarrow.NewLineStringType().ExtensionEquals(got))
		require.True(t, got.ExtensionEquals(geoarrow.NewLineStringType()))
	})
}

func TestNestedGeometryAppendWrongDimensionPanics(t *testing.T) {
	for _, f := range nestedFactories {
		for _, l := range layouts {
			t.Run(f.name+"/"+l.name, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)

				typ := f.make(geoarrow.XY, l.interleave)
				b := typ.(array.CustomExtensionBuilder).NewBuilder(mem)
				defer b.Release()

				// A value of another dimension is rejected with a clear message,
				// and nothing is written.
				require.PanicsWithValue(t,
					"geoarrow."+f.name+": value has dimension XYZ, builder expects XY",
					func() { f.appendValue(b, geoarrow.XYZ) })
				require.Equal(t, 0, b.Len())

				// An empty value carries no coordinates, so any dimension fits.
				require.NotPanics(t, func() { f.appendEmpty(b, geoarrow.XYZM) })
				f.appendValue(b, geoarrow.XY)
				arr := b.NewArray()
				defer arr.Release()
				require.Equal(t, 2, arr.Len())
				require.NoError(t, array.ValidateFull(arr))
			})
		}
	}
}
