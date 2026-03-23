package geoarrowgeom_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	_ "github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	geoarrow "github.com/geoarrow/geoarrow-go"
	geoarrowgeom "github.com/geoarrow/geoarrow-go/gogeom"
	"github.com/twpayne/go-geom"
)

const benchN = 10_000

func buildPointArray(mem memory.Allocator, n int) *geoarrow.PointArray {
	typ := geoarrow.NewPointType()
	b := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer b.Release()
	for i := range n {
		b.Append(geoarrow.NewPointValue(float64(i), float64(i*2)))
	}
	arr := b.NewArray()
	return arr.(*geoarrow.PointArray)
}

func buildGeomPoints(n int) []*geom.Point {
	pts := make([]*geom.Point, n)
	for i := range n {
		pts[i] = geom.NewPointFlat(geom.XY, []float64{float64(i), float64(i * 2)})
	}
	return pts
}

// A simple polygon: square with 5 vertices (closed ring)
var squareCoords = [][]float64{{0, 0, 10, 0, 10, 10, 0, 10, 0, 0}}

func buildPolygonArray(mem memory.Allocator, n int) *geoarrow.PolygonArray {
	typ := geoarrow.NewPolygonType()
	b := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer b.Release()
	val := geoarrow.NewPolygonValue(geoarrow.XY, squareCoords)
	for range n {
		b.Append(val)
	}
	arr := b.NewArray()
	return arr.(*geoarrow.PolygonArray)
}

func buildGeomPolygons(n int) []*geom.Polygon {
	polys := make([]*geom.Polygon, n)
	for i := range n {
		polys[i] = geom.NewPolygonFlat(geom.XY,
			[]float64{0, 0, 10, 0, 10, 10, 0, 10, 0, 0},
			[]int{10})
	}
	return polys
}

// --- Point benchmarks ---

func BenchmarkPointsToGeom(b *testing.B) {
	mem := memory.DefaultAllocator
	arr := buildPointArray(mem, benchN)
	defer arr.Release()
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		pts := geoarrowgeom.PointsToGeom(arr)
		_ = pts
	}
}

func BenchmarkPointsFromGeom(b *testing.B) {
	mem := memory.DefaultAllocator
	pts := buildGeomPoints(benchN)
	typ := geoarrow.NewPointType()
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		arr := geoarrowgeom.PointsFromGeom(mem, pts, typ)
		arr.Release()
	}
}

// --- Polygon benchmarks ---

func BenchmarkPolygonsToGeom(b *testing.B) {
	mem := memory.DefaultAllocator
	arr := buildPolygonArray(mem, benchN)
	defer arr.Release()
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		polys := geoarrowgeom.PolygonsToGeom(arr)
		_ = polys
	}
}

func BenchmarkPolygonsFromGeom(b *testing.B) {
	mem := memory.DefaultAllocator
	polys := buildGeomPolygons(benchN)
	typ := geoarrow.NewPolygonType()
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		arr := geoarrowgeom.PolygonsFromGeom(mem, polys, typ)
		arr.Release()
	}
}

// --- Per-element allocation tests ---

func TestPointsToGeomAllocs(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := buildPointArray(mem, 1000)
	defer arr.Release()

	avg := testing.AllocsPerRun(10, func() {
		pts := geoarrowgeom.PointsToGeom(arr)
		_ = pts
	})

	// Expect: 1 result slice + 1 fields slice + 2N (Point struct + flatCoords per point)
	// Should be exactly 2 allocs per point — no per-field allocs
	perPoint := avg / 1000
	t.Logf("PointsToGeom: %.0f allocs for 1000 points (%.2f per point)", avg, perPoint)
	if perPoint > 2.1 {
		t.Errorf("too many allocations per point: %.2f (expected ~2.0)", perPoint)
	}
}

func TestPointsFromGeomAllocs(t *testing.T) {
	mem := memory.DefaultAllocator
	pts := buildGeomPoints(1000)
	typ := geoarrow.NewPointType()

	avg := testing.AllocsPerRun(10, func() {
		arr := geoarrowgeom.PointsFromGeom(mem, pts, typ)
		arr.Release()
	})

	// Expect: only builder internals — no per-row allocs
	perPoint := avg / 1000
	t.Logf("PointsFromGeom: %.0f allocs for 1000 points (%.2f per point)", avg, perPoint)
	if perPoint > 0.1 {
		t.Errorf("too many allocations per point: %.2f (expected ~0)", perPoint)
	}
}

func TestPolygonsToGeomAllocs(t *testing.T) {
	mem := memory.DefaultAllocator
	arr := buildPolygonArray(mem, 1000)
	defer arr.Release()

	avg := testing.AllocsPerRun(10, func() {
		polys := geoarrowgeom.PolygonsToGeom(arr)
		_ = polys
	})

	// Expect per polygon: Polygon struct + flatCoords + ends + go-geom internals
	// With pre-allocated flatCoords, should be ~4-5 allocs per polygon
	perPoly := avg / 1000
	t.Logf("PolygonsToGeom: %.0f allocs for 1000 polygons (%.2f per polygon)", avg, perPoly)
	if perPoly > 7.1 {
		t.Errorf("too many allocations per polygon: %.2f (expected <=7)", perPoly)
	}
}

func TestPolygonsFromGeomAllocs(t *testing.T) {
	mem := memory.DefaultAllocator
	polys := buildGeomPolygons(1000)
	typ := geoarrow.NewPolygonType()

	avg := testing.AllocsPerRun(10, func() {
		arr := geoarrowgeom.PolygonsFromGeom(mem, polys, typ)
		arr.Release()
	})

	// Expect: only builder internals — no per-row allocs
	perPoly := avg / 1000
	t.Logf("PolygonsFromGeom: %.0f allocs for 1000 polygons (%.2f per polygon)", avg, perPoly)
	if perPoly > 0.2 {
		t.Errorf("too many allocations per polygon: %.2f (expected ~0)", perPoly)
	}
}

// --- Allocation comparison: native vs WKB ---

func BenchmarkPointsToGeom_VsWKB(b *testing.B) {
	// Native geoarrow point conversion
	b.Run("native", func(b *testing.B) {
		mem := memory.DefaultAllocator
		arr := buildPointArray(mem, benchN)
		defer arr.Release()
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			pts := geoarrowgeom.PointsToGeom(arr)
			_ = pts
		}
	})

	// For comparison: reading via PointValue intermediary
	b.Run("via_PointValue", func(b *testing.B) {
		mem := memory.DefaultAllocator
		arr := buildPointArray(mem, benchN)
		defer arr.Release()
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			n := arr.Len()
			pts := make([]*geom.Point, n)
			for i := range n {
				if arr.IsNull(i) {
					continue
				}
				v := arr.Value(i)
				pts[i] = geom.NewPointFlat(geom.XY, []float64{v.X(), v.Y()})
			}
			_ = pts
		}
	})
}

func BenchmarkPolygonsFromGeom_StorageAccess(b *testing.B) {
	// Verify we're getting expected throughput with direct storage access
	mem := memory.DefaultAllocator

	// Build polygons with more vertices to stress test
	bigPolys := make([]*geom.Polygon, 1000)
	// 100-vertex polygon
	coords := make([]float64, 200)
	for i := range 100 {
		coords[i*2] = float64(i)
		coords[i*2+1] = float64(i * 2)
	}
	for i := range bigPolys {
		bigPolys[i] = geom.NewPolygonFlat(geom.XY, coords, []int{200})
	}

	typ := geoarrow.NewPolygonType()

	b.Run("from_geom", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			arr := geoarrowgeom.PolygonsFromGeom(mem, bigPolys, typ)
			arr.Release()
		}
	})

	// Build the array once for to_geom benchmark
	arr := geoarrowgeom.PolygonsFromGeom(mem, bigPolys, typ)
	defer arr.Release()
	polyArr := arr.(*geoarrow.PolygonArray)

	b.Run("to_geom", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			polys := geoarrowgeom.PolygonsToGeom(polyArr)
			_ = polys
		}
	})
}

// Verify that PointsToGeom allocates exactly 1 []float64 per point (for flatCoords)
// and not per-field.
func BenchmarkPointsToGeom_XYZ(b *testing.B) {
	mem := memory.DefaultAllocator
	xyzStorage := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "z", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	)
	typ := geoarrow.NewPointType(geoarrow.PointWithStorage(xyzStorage))
	bldr := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer bldr.Release()
	for i := range benchN {
		bldr.Append(geoarrow.NewPointValueZ(float64(i), float64(i*2), float64(i*3)))
	}
	arr := bldr.NewArray()
	defer arr.Release()

	pointArr := arr.(*geoarrow.PointArray)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		pts := geoarrowgeom.PointsToGeom(pointArr)
		_ = pts
	}
}
