// Package geoarrowgeom provides zero-copy conversions between GeoArrow
// arrays (github.com/apache/arrow-go) and go-geom types (github.com/twpayne/go-geom).
package geoarrowgeom

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	geoarrow "github.com/geoarrow/geoarrow-go"
	"github.com/twpayne/go-geom"
)

// dimensionToLayout converts a geoarrow CoordinateDimension to a go-geom Layout.
func dimensionToLayout(dim geoarrow.Dimension) geom.Layout {
	switch dim {
	case geoarrow.XY:
		return geom.XY
	case geoarrow.XYZ:
		return geom.XYZ
	case geoarrow.XYM:
		return geom.XYM
	case geoarrow.XYZM:
		return geom.XYZM
	default:
		return geom.XY
	}
}

// PointsToGeom converts a geoarrow PointArray to a slice of go-geom Points.
// Null entries produce nil pointers. The conversion reads directly from the
// underlying Arrow storage arrays without intermediate copies.
// Supports both struct (separated) and fixed-size list (interleaved) coordinate layouts.
func PointsToGeom(arr *geoarrow.PointArray) []*geom.Point {
	n := arr.Len()
	dim := geoarrow.DimensionFromStorage(arr.ExtensionType().StorageType())
	layout := dimensionToLayout(dim)
	stride := layout.Stride()

	result := make([]*geom.Point, n)

	switch storage := arr.Storage().(type) {
	case *array.Struct:
		fields := make([]*array.Float64, storage.NumField())
		for i := range fields {
			fields[i] = storage.Field(i).(*array.Float64)
		}
		for i := range n {
			if arr.IsNull(i) {
				continue
			}
			flatCoords := make([]float64, stride)
			for f := range fields {
				flatCoords[f] = fields[f].Value(i)
			}
			result[i] = geom.NewPointFlat(layout, flatCoords)
		}
	case *array.FixedSizeList:
		coordArr := storage.ListValues().(*array.Float64)
		for i := range n {
			if arr.IsNull(i) {
				continue
			}
			start, _ := storage.ValueOffsets(i)
			flatCoords := make([]float64, stride)
			for j := range stride {
				flatCoords[j] = coordArr.Value(int(start) + j)
			}
			result[i] = geom.NewPointFlat(layout, flatCoords)
		}
	}

	return result
}

// PointsFromGeom converts a slice of go-geom Points to a geoarrow PointArray.
// Nil entries become null values. The caller owns the returned array and must
// call Release() when done.
// Supports both struct (separated) and fixed-size list (interleaved) coordinate layouts.
func PointsFromGeom(mem memory.Allocator, points []*geom.Point, typ *geoarrow.PointType) arrow.Array {
	builder := typ.NewBuilder(mem).(*geoarrow.PointBuilder)
	defer builder.Release()

	dim := geoarrow.DimensionFromStorage(typ.StorageType())
	stride := dim.NDim()
	storageBuilder := builder.StorageBuilder()

	switch sb := storageBuilder.(type) {
	case *array.StructBuilder:
		fieldBuilders := make([]*array.Float64Builder, stride)
		for i := range fieldBuilders {
			fieldBuilders[i] = sb.FieldBuilder(i).(*array.Float64Builder)
		}
		for _, pt := range points {
			if pt == nil {
				sb.AppendNull()
				continue
			}
			sb.Append(true)
			fc := pt.FlatCoords()
			for f := range fieldBuilders {
				if f < len(fc) {
					fieldBuilders[f].Append(fc[f])
				} else {
					fieldBuilders[f].Append(0)
				}
			}
		}
	case *array.FixedSizeListBuilder:
		vb := sb.ValueBuilder().(*array.Float64Builder)
		for _, pt := range points {
			if pt == nil {
				sb.AppendNull()
				continue
			}
			sb.Append(true)
			fc := pt.FlatCoords()
			for f := range stride {
				if f < len(fc) {
					vb.Append(fc[f])
				} else {
					vb.Append(0)
				}
			}
		}
	}

	return builder.NewArray()
}

// PolygonsToGeom converts a geoarrow PolygonArray to a slice of go-geom Polygons.
// Null entries produce nil pointers. The conversion reads directly from the
// underlying nested Arrow list/struct storage without intermediate copies.
func PolygonsToGeom(arr *geoarrow.PolygonArray) []*geom.Polygon {
	outerList := arr.Storage().(*array.List)
	innerList := outerList.ListValues().(*array.List)
	structArr := innerList.ListValues().(*array.Struct)

	// Determine layout from coordinate struct
	polyType := arr.ExtensionType().(*geoarrow.PolygonType)
	outerListType := polyType.StorageType().(*arrow.ListType)
	innerListType := outerListType.ElemField().Type.(*arrow.ListType)
	coordStruct := innerListType.ElemField().Type.(*arrow.StructType)

	nFields := coordStruct.NumFields()
	dim := geoarrow.DimensionFromStructType(coordStruct)
	layout := dimensionToLayout(dim)

	// Pre-extract typed field arrays
	fields := make([]*array.Float64, nFields)
	for i := range nFields {
		fields[i] = structArr.Field(i).(*array.Float64)
	}

	n := arr.Len()
	result := make([]*geom.Polygon, n)
	for i := range n {
		if arr.IsNull(i) {
			continue
		}

		ringStart, ringEnd := outerList.ValueOffsets(i)
		nRings := int(ringEnd - ringStart)

		// Compute total vertex count across all rings to pre-allocate
		totalVerts := 0
		for r := range nRings {
			vs, ve := innerList.ValueOffsets(int(ringStart) + r)
			totalVerts += int(ve - vs)
		}

		flatCoords := make([]float64, 0, totalVerts*nFields)
		ends := make([]int, nRings)

		for r := range nRings {
			vertStart, vertEnd := innerList.ValueOffsets(int(ringStart) + r)
			nVerts := int(vertEnd - vertStart)

			for v := range nVerts {
				idx := int(vertStart) + v
				for f := range nFields {
					flatCoords = append(flatCoords, fields[f].Value(idx))
				}
			}
			ends[r] = len(flatCoords)
		}

		result[i] = geom.NewPolygonFlat(layout, flatCoords, ends)
	}
	return result
}

// PolygonsFromGeom converts a slice of go-geom Polygons to a geoarrow PolygonArray.
// Nil entries become null values. The caller owns the returned array and must
// call Release() when done.
func PolygonsFromGeom(mem memory.Allocator, polygons []*geom.Polygon, typ *geoarrow.PolygonType) arrow.Array {
	builder := typ.NewBuilder(mem).(*geoarrow.PolygonBuilder)
	defer builder.Release()

	// Get builders for direct storage access
	outerListBuilder := builder.StorageBuilder().(*array.ListBuilder)
	innerListBuilder := outerListBuilder.ValueBuilder().(*array.ListBuilder)
	structBuilder := innerListBuilder.ValueBuilder().(*array.StructBuilder)

	outerListType := typ.StorageType().(*arrow.ListType)
	innerListType := outerListType.ElemField().Type.(*arrow.ListType)
	coordStruct := innerListType.ElemField().Type.(*arrow.StructType)
	nFields := coordStruct.NumFields()

	fieldBuilders := make([]*array.Float64Builder, nFields)
	for i := range nFields {
		fieldBuilders[i] = structBuilder.FieldBuilder(i).(*array.Float64Builder)
	}

	for _, poly := range polygons {
		if poly == nil {
			outerListBuilder.AppendNull()
			continue
		}

		outerListBuilder.Append(true)
		fc := poly.FlatCoords()
		ends := poly.Ends()
		stride := poly.Stride()

		prevEnd := 0
		for _, end := range ends {
			innerListBuilder.Append(true)
			for vi := prevEnd; vi < end; vi += stride {
				structBuilder.Append(true)
				for f := range nFields {
					if f < stride {
						fieldBuilders[f].Append(fc[vi+f])
					} else {
						fieldBuilders[f].Append(0)
					}
				}
			}
			prevEnd = end
		}
	}

	return builder.NewArray()
}
