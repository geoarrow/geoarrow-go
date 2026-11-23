package geoarrow_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/geoarrow/geoarrow-go"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestWKTStringHeader(t *testing.T) {
	for _, tc := range []struct {
		wkt   string
		id    geoarrow.GeometryTypeID
		dim   geoarrow.Dimension
		empty bool
	}{
		{"POINT (1 2)", geoarrow.PointID, geoarrow.XY, false},
		{"POINT Z (1 2 3)", geoarrow.PointZID, geoarrow.XYZ, false},
		{"POINT M (1 2 3)", geoarrow.PointMID, geoarrow.XYM, false},
		{"POINT ZM (1 2 3 4)", geoarrow.PointZMID, geoarrow.XYZM, false},
		{"POINTZ (1 2 3)", geoarrow.PointZID, geoarrow.XYZ, false},
		{"LINESTRINGZM(1 2 3 4, 5 6 7 8)", geoarrow.LineStringZMID, geoarrow.XYZM, false},
		{"point m (1 2 3)", geoarrow.PointMID, geoarrow.XYM, false},
		{"  POLYGON ((0 0, 1 0, 0 1, 0 0))  ", geoarrow.PolygonID, geoarrow.XY, false},
		// No marker: dimension is inferred from the first vertex (3 means Z).
		{"LINESTRING (1 2 3, 4 5 6)", geoarrow.LineStringZID, geoarrow.XYZ, false},
		{"POINT (-1.5 -2.5 -3.5 -4.5)", geoarrow.PointZMID, geoarrow.XYZM, false},
		{"MULTIPOINT ((1 2), (3 4))", geoarrow.MultiPointID, geoarrow.XY, false},
		{"MULTILINESTRING Z ((1 2 3, 4 5 6))", geoarrow.MultiLineStringZID, geoarrow.XYZ, false},
		{"MULTIPOLYGON M (((0 0 1, 1 0 1, 0 1 1, 0 0 1)))", geoarrow.MultiPolygonMID, geoarrow.XYM, false},
		{"POINT EMPTY", geoarrow.PointID, geoarrow.XY, true},
		{"POLYGON Z EMPTY", geoarrow.PolygonZID, geoarrow.XYZ, true},
		{"multipolygon zm empty", geoarrow.MultiPolygonZMID, geoarrow.XYZM, true},
		// GEOMETRYCOLLECTION has no GeometryTypeID.
		{"GEOMETRYCOLLECTION (POINT (1 2))", 0, geoarrow.XY, false},
		{"GEOMETRYCOLLECTION Z EMPTY", 0, geoarrow.XYZ, true},
		{"not wkt", 0, geoarrow.XY, false},
		{"", 0, geoarrow.XY, true},
	} {
		t.Run(tc.wkt, func(t *testing.T) {
			v := geoarrow.WKTString(tc.wkt)
			require.Equal(t, tc.id, v.GeometryType())
			require.Equal(t, tc.dim, v.Dimension())
			require.Equal(t, tc.empty, v.IsEmpty())
			require.Equal(t, tc.wkt, v.String())
		})
	}
}

func TestWKTArray(t *testing.T) {
	storages := []struct {
		name string
		opt  func() *geoarrow.WKTType
	}{
		{"utf8", func() *geoarrow.WKTType { return geoarrow.NewWKTType(geoarrow.WKTWithStringStorage()) }},
		{"large_utf8", func() *geoarrow.WKTType { return geoarrow.NewWKTType(geoarrow.WKTWithLargeStringStorage()) }},
		{"utf8_view", func() *geoarrow.WKTType { return geoarrow.NewWKTType(geoarrow.WKTWithStringViewStorage()) }},
	}
	for _, s := range storages {
		t.Run(s.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			typ := s.opt()
			b := typ.NewBuilder(mem).(*geoarrow.WKTBuilder)
			defer b.Release()
			b.Append("POINT (1 2)")
			b.AppendNull()
			b.Append("POLYGON Z EMPTY")
			require.NoError(t, b.AppendValueFromString("LINESTRING (0 0, 1 1)"))
			b.AppendValues([]geoarrow.WKTString{"MULTIPOINT ((1 2))", "ignored"}, []bool{true, false})

			arr := b.NewArray()
			defer arr.Release()
			require.Equal(t, 6, arr.Len())
			require.Equal(t, 2, arr.NullN())

			wa := arr.(*geoarrow.WKTArray)
			require.Equal(t, geoarrow.WKTString("POINT (1 2)"), wa.Value(0))
			require.True(t, arr.IsNull(1))
			require.Equal(t, geoarrow.WKTString(""), wa.Value(1))
			require.Equal(t, geoarrow.PolygonZID, wa.Value(2).GeometryType())
			require.Equal(t, geoarrow.WKTString("LINESTRING (0 0, 1 1)"), wa.Value(3))
			require.Equal(t, "(null)", wa.ValueStr(1))
			require.Equal(t, "POINT (1 2)", wa.ValueStr(0))
			require.Len(t, wa.Values(), 6)

			checkRoundTrips(t, mem, typ, arr)
		})
	}
}

func TestWKTType(t *testing.T) {
	typ := geoarrow.NewWKTType()
	require.Equal(t, "geoarrow.wkt", typ.ExtensionName())
	require.Equal(t, arrow.LARGE_STRING, typ.StorageType().ID())
	require.True(t, typ.ExtensionEquals(geoarrow.NewWKTType()))
	require.False(t, typ.ExtensionEquals(geoarrow.NewWKTType(geoarrow.WKTWithStringStorage())))
	require.False(t, typ.ExtensionEquals(geoarrow.NewWKBType()))
	require.False(t, typ.ExtensionEquals(geoarrow.NewWKTType(geoarrow.WKTWithMetadata(geoarrow.Metadata{Edges: geoarrow.EdgeSpherical}))))

	registered := arrow.GetExtensionType("geoarrow.wkt")
	require.NotNil(t, registered)
	require.Equal(t, "geoarrow.wkt", registered.ExtensionName())

	// Spec-allowed storage round-trips through Deserialize; others do not.
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, arrow.BinaryTypes.StringView} {
		got, err := typ.Deserialize(dt, "{}")
		require.NoError(t, err, dt.String())
		require.Equal(t, dt.ID(), got.StorageType().ID())
	}
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.Binary, arrow.PrimitiveTypes.Float64, arrow.ListOf(arrow.BinaryTypes.String)} {
		_, err := typ.Deserialize(dt, "{}")
		require.Error(t, err, dt.String())
	}
	_, err := typ.Deserialize(arrow.BinaryTypes.String, "not json")
	require.Error(t, err)
}

func TestWKTJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewWKTType()
	arr, _, err := array.FromJSON(mem, typ, bytes.NewReader([]byte(`["POINT (1 2)", null, "POLYGON EMPTY"]`)))
	require.NoError(t, err)
	defer arr.Release()
	require.Equal(t, 3, arr.Len())
	require.Equal(t, 1, arr.NullN())

	out, err := json.Marshal(arr)
	require.NoError(t, err)
	require.JSONEq(t, `["POINT (1 2)", null, "POLYGON EMPTY"]`, string(out))

	_, _, err = array.FromJSON(mem, typ, bytes.NewReader([]byte(`[123]`)))
	require.Error(t, err)
}

func TestBoxValue(t *testing.T) {
	t.Run("xy", func(t *testing.T) {
		v := geoarrow.NewBoxValue(1, 2, 3, 4)
		require.Equal(t, geoarrow.XY, v.Dimension())
		require.Equal(t, []float64{1, 2}, v.Min())
		require.Equal(t, []float64{3, 4}, v.Max())
		require.Equal(t, 1.0, v.XMin())
		require.Equal(t, 2.0, v.YMin())
		require.Equal(t, 3.0, v.XMax())
		require.Equal(t, 4.0, v.YMax())
		require.True(t, math.IsNaN(v.ZMin()))
		require.True(t, math.IsNaN(v.MMax()))
		require.False(t, v.IsEmpty())
		require.False(t, v.CrossesAntimeridian())
		require.Equal(t, geoarrow.GeometryTypeID(0), v.GeometryType())
		require.Equal(t, "BOX(1 2, 3 4)", v.String())
	})

	t.Run("xyz", func(t *testing.T) {
		v := geoarrow.NewBoxValueZ(1, 2, 3, 4, 5, 6)
		require.Equal(t, geoarrow.XYZ, v.Dimension())
		require.Equal(t, 3.0, v.ZMin())
		require.Equal(t, 6.0, v.ZMax())
		require.True(t, math.IsNaN(v.MMin()))
		require.Equal(t, "BOX Z(1 2 3, 4 5 6)", v.String())
	})

	t.Run("xym", func(t *testing.T) {
		v := geoarrow.NewBoxValueM(1, 2, 3, 4, 5, 6)
		require.Equal(t, geoarrow.XYM, v.Dimension())
		require.Equal(t, 3.0, v.MMin())
		require.Equal(t, 6.0, v.MMax())
		require.True(t, math.IsNaN(v.ZMin()))
		require.Equal(t, "BOX M(1 2 3, 4 5 6)", v.String())
	})

	t.Run("xyzm", func(t *testing.T) {
		v := geoarrow.NewBoxValueZM(1, 2, 3, 4, 5, 6, 7, 8)
		require.Equal(t, geoarrow.XYZM, v.Dimension())
		require.Equal(t, 3.0, v.ZMin())
		require.Equal(t, 4.0, v.MMin())
		require.Equal(t, 7.0, v.ZMax())
		require.Equal(t, 8.0, v.MMax())
		require.Equal(t, "BOX ZM(1 2 3 4, 5 6 7 8)", v.String())
	})

	t.Run("antimeridian", func(t *testing.T) {
		v := geoarrow.NewBoxValue(170, -10, -170, 10)
		require.True(t, v.CrossesAntimeridian())
		require.False(t, v.IsEmpty())
	})

	t.Run("empty", func(t *testing.T) {
		v := geoarrow.NewEmptyBoxValue(geoarrow.XYZ)
		require.True(t, v.IsEmpty())
		require.False(t, v.CrossesAntimeridian(), "an empty X range is not an antimeridian crossing")
		require.Equal(t, "BOX Z EMPTY", v.String())

		// One empty range makes the whole box empty.
		partial := geoarrow.NewBoxValue(0, math.Inf(1), 1, math.Inf(-1))
		require.True(t, partial.IsEmpty())

		// The zero value (what a null slot reads as) is safe to inspect.
		var zero geoarrow.BoxValue
		require.True(t, zero.IsEmpty())
		require.True(t, math.IsNaN(zero.XMin()))
		require.False(t, zero.CrossesAntimeridian())
	})

	t.Run("lossless string", func(t *testing.T) {
		v := geoarrow.NewBoxValue(-122.123456789012, 0.1, 1e-9, 12345678.5)
		require.Equal(t, "BOX(-122.123456789012 0.1, 0.000000001 12345678.5)", v.String())
	})
}

func TestBoxType(t *testing.T) {
	typ := geoarrow.NewBoxType()
	require.Equal(t, "geoarrow.box", typ.ExtensionName())
	require.True(t, typ.ExtensionEquals(geoarrow.NewBoxType()))
	require.False(t, typ.ExtensionEquals(geoarrow.NewBoxType(geoarrow.BoxWithDimension(geoarrow.XYZ))))
	require.False(t, typ.ExtensionEquals(geoarrow.NewPointType()))
	require.False(t, typ.ExtensionEquals(geoarrow.NewBoxType(geoarrow.BoxWithMetadata(geoarrow.Metadata{Edges: geoarrow.EdgeSpherical}))))

	registered := arrow.GetExtensionType("geoarrow.box")
	require.NotNil(t, registered)
	require.Equal(t, "geoarrow.box", registered.ExtensionName())

	// Field names and order are mandated by the GeoArrow spec.
	for _, tc := range []struct {
		dim   geoarrow.Dimension
		names []string
	}{
		{geoarrow.XY, []string{"xmin", "ymin", "xmax", "ymax"}},
		{geoarrow.XYZ, []string{"xmin", "ymin", "zmin", "xmax", "ymax", "zmax"}},
		{geoarrow.XYM, []string{"xmin", "ymin", "mmin", "xmax", "ymax", "mmax"}},
		{geoarrow.XYZM, []string{"xmin", "ymin", "zmin", "mmin", "xmax", "ymax", "zmax", "mmax"}},
	} {
		t.Run(tc.dim.String(), func(t *testing.T) {
			st := geoarrow.NewBoxType(geoarrow.BoxWithDimension(tc.dim)).StorageType().(*arrow.StructType)
			require.Equal(t, len(tc.names), st.NumFields())
			for i, name := range tc.names {
				require.Equal(t, name, st.Field(i).Name)
				require.Equal(t, arrow.FLOAT64, st.Field(i).Type.ID())
				require.False(t, st.Field(i).Nullable)
			}
		})
	}
}

func TestBoxDeserializeRejectsBadStorage(t *testing.T) {
	typ := geoarrow.NewBoxType()
	f64 := arrow.PrimitiveTypes.Float64
	field := func(name string, dt arrow.DataType) arrow.Field { return arrow.Field{Name: name, Type: dt} }

	for name, dt := range map[string]arrow.DataType{
		"not a struct":    f64,
		"point layout":    arrow.StructOf(field("x", f64), field("y", f64)),
		"wrong names":     arrow.StructOf(field("x", f64), field("y", f64), field("xx", f64), field("yy", f64)),
		"max before min":  arrow.StructOf(field("xmax", f64), field("ymax", f64), field("xmin", f64), field("ymin", f64)),
		"wrong order xy":  arrow.StructOf(field("ymin", f64), field("xmin", f64), field("xmax", f64), field("ymax", f64)),
		"zm in xym slot":  arrow.StructOf(field("xmin", f64), field("ymin", f64), field("zmin", f64), field("mmin", f64), field("xmax", f64), field("ymax", f64)),
		"wrong elem type": arrow.StructOf(field("xmin", arrow.PrimitiveTypes.Float32), field("ymin", f64), field("xmax", f64), field("ymax", f64)),
		"five fields":     arrow.StructOf(field("xmin", f64), field("ymin", f64), field("zmin", f64), field("xmax", f64), field("ymax", f64)),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := typ.Deserialize(dt, "{}")
			require.Error(t, err)
		})
	}

	_, err := typ.Deserialize(typ.StorageType(), "not json")
	require.Error(t, err)

	// Nullable fields are tolerated even though the spec asks for non-nullable.
	got, err := typ.Deserialize(arrow.StructOf(
		arrow.Field{Name: "xmin", Type: f64, Nullable: true}, arrow.Field{Name: "ymin", Type: f64, Nullable: true},
		arrow.Field{Name: "xmax", Type: f64, Nullable: true}, arrow.Field{Name: "ymax", Type: f64, Nullable: true},
	), "{}")
	require.NoError(t, err)
	require.IsType(t, &geoarrow.BoxType{}, got)
}

func TestBoxArray(t *testing.T) {
	for _, dim := range allDims {
		t.Run(dim.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			typ := geoarrow.NewBoxType(geoarrow.BoxWithDimension(dim))
			n := dim.NDim()
			lo, hi, wrap := make([]float64, n), make([]float64, n), make([]float64, n)
			for i := range n {
				lo[i], hi[i], wrap[i] = float64(i), float64(i+10), float64(i+10)
			}
			normal := geoarrow.NewBoxValueFromBounds(dim, lo, hi)
			antimeridian := geoarrow.NewBoxValueFromBounds(dim, wrap, lo) // xmin > xmax

			b := typ.NewBuilder(mem).(*geoarrow.BoxBuilder)
			defer b.Release()
			b.Append(normal)
			b.AppendNull()
			b.Append(geoarrow.NewEmptyBoxValue(dim))
			b.Append(antimeridian)
			b.Append(geoarrow.BoxValue{}) // zero value: written as an empty box

			arr := b.NewArray()
			defer arr.Release()
			require.Equal(t, 5, arr.Len())
			require.Equal(t, 1, arr.NullN())

			ba := arr.(*geoarrow.BoxArray)
			require.Equal(t, dim, ba.Value(0).Dimension())
			require.Equal(t, lo, ba.Value(0).Min())
			require.Equal(t, hi, ba.Value(0).Max())
			require.True(t, arr.IsNull(1))
			require.True(t, arr.IsValid(2))
			require.True(t, ba.Value(2).IsEmpty())
			require.True(t, ba.Value(3).CrossesAntimeridian())
			require.True(t, arr.IsValid(4), "zero value is an empty box, not a null")
			require.True(t, ba.Value(4).IsEmpty())
			require.Len(t, ba.Values(), 5)

			checkRoundTrips(t, mem, typ, arr)
		})
	}
}

func TestBoxSlice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	b := geoarrow.NewBoxType().NewBuilder(mem).(*geoarrow.BoxBuilder)
	defer b.Release()
	for i := range 4 {
		f := float64(i)
		b.Append(geoarrow.NewBoxValue(f, f, f+1, f+1))
	}
	arr := b.NewArray()
	defer arr.Release()

	sliced := array.NewSlice(arr, 2, 4).(*geoarrow.BoxArray)
	defer sliced.Release()
	require.Equal(t, 2.0, sliced.Value(0).XMin())
	require.Equal(t, 4.0, sliced.Value(1).XMax())
}

func TestBoxAppendWrongDimensionPanics(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	b := geoarrow.NewBoxType().NewBuilder(mem).(*geoarrow.BoxBuilder)
	defer b.Release()
	require.PanicsWithValue(t, "geoarrow.box: value has 3 dimensions, builder expects 2", func() {
		b.Append(geoarrow.NewBoxValueZ(0, 0, 0, 1, 1, 1))
	})
}

func TestBoxWKT(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	parse := func(typ *geoarrow.BoxType, s string) (geoarrow.BoxValue, error) {
		b := typ.NewBuilder(mem).(*geoarrow.BoxBuilder)
		defer b.Release()
		if err := b.AppendValueFromString(s); err != nil {
			require.Equal(t, 0, b.Len(), "failed parse left a partial value")
			return geoarrow.BoxValue{}, err
		}
		arr := b.NewArray()
		defer arr.Release()
		return arr.(*geoarrow.BoxArray).Value(0), nil
	}

	v, err := parse(geoarrow.NewBoxType(), " box (1 2,3.5 4) ")
	require.NoError(t, err)
	require.Equal(t, []float64{1, 2}, v.Min())
	require.Equal(t, []float64{3.5, 4}, v.Max())

	v, err = parse(geoarrow.NewBoxType(geoarrow.BoxWithDimension(geoarrow.XYM)), "BOX M (1 2 3, 4 5 6)")
	require.NoError(t, err)
	require.Equal(t, geoarrow.XYM, v.Dimension())
	require.Equal(t, 6.0, v.MMax())

	// Empty ranges are written as +Inf / -Inf and parse back.
	v, err = parse(geoarrow.NewBoxType(), "BOX(+Inf +Inf, -Inf -Inf)")
	require.NoError(t, err)
	require.True(t, v.IsEmpty())

	// "BOX EMPTY" takes the storage dimension.
	v, err = parse(geoarrow.NewBoxType(geoarrow.BoxWithDimension(geoarrow.XYZM)), "BOX EMPTY")
	require.NoError(t, err)
	require.True(t, v.IsEmpty())
	require.Equal(t, geoarrow.XYZM, v.Dimension())

	for name, s := range map[string]string{
		"wrong keyword":     "POLYGON ((0 0, 1 1))",
		"one corner":        "BOX (1 2)",
		"three corners":     "BOX (1 2, 3 4, 5 6)",
		"ragged corners":    "BOX (1 2, 3 4 5)",
		"bad number":        "BOX (a b, c d)",
		"missing parens":    "BOX 1 2, 3 4",
		"z into xy storage": "BOX Z (1 2 3, 4 5 6)",
		"keyword prefix":    "BOXZ (1 2, 3 4)",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parse(geoarrow.NewBoxType(), s)
			require.Error(t, err)
		})
	}
}

func TestBoxJSON(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typ := geoarrow.NewBoxType()
	b := typ.NewBuilder(mem).(*geoarrow.BoxBuilder)
	defer b.Release()
	b.Append(geoarrow.NewBoxValue(0, 1, 2, 3))
	b.AppendNull()
	b.Append(geoarrow.NewEmptyBoxValue(geoarrow.XY))
	arr := b.NewArray()
	defer arr.Release()

	data, err := json.Marshal(arr)
	require.NoError(t, err)
	// Mins then maxes, GeoJSON bbox order. Infinities are strings since JSON
	// numbers cannot express them.
	require.JSONEq(t, `[[0,1,2,3], null, ["+Inf","+Inf","-Inf","-Inf"]]`, string(data))

	for name, in := range map[string]string{
		"too few":     `[[0,1,2]]`,
		"too many":    `[[0,1,2,3,4]]`,
		"not a list":  `[1]`,
		"bad element": `[[0,1,2,true]]`,
		"bad string":  `[[0,1,2,"nope"]]`,
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := array.FromJSON(mem, typ, bytes.NewReader([]byte(in)))
			require.Error(t, err)
		})
	}
}

func TestNewBoxValueFromBoundsValidates(t *testing.T) {
	require.Panics(t, func() { geoarrow.NewBoxValueFromBounds(geoarrow.XYZ, []float64{0, 0}, []float64{1, 1}) })
	require.Panics(t, func() { geoarrow.NewBoxValueFromBounds(geoarrow.XY, []float64{0, 0}, []float64{1, 1, 1}) })
	v := geoarrow.NewBoxValueFromBounds(geoarrow.XYM, []float64{0, 1, 2}, []float64{3, 4, 5})
	require.Equal(t, 2.0, v.MMin())
	require.Equal(t, 5.0, v.MMax())
}
