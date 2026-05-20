package geoarrow

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// coordStorage returns the Arrow storage type for a single GeoArrow
// coordinate, in either separated (Struct<x,y[,z][,m]>) or interleaved
// (FixedSizeList<float64>) form, per the GeoArrow specification.
func coordStorage(dim Dimension, interleaved bool) arrow.DataType {
	if interleaved {
		return interleavedStorage(dim)
	}
	return coordStructStorage(dim)
}

// coordStructStorage returns the separated struct storage for one coordinate.
func coordStructStorage(dim Dimension) arrow.DataType {
	fields := make([]arrow.Field, dim.NDim())
	fields[0] = arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: false}
	fields[1] = arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false}
	switch dim {
	case XYZ:
		fields[2] = arrow.Field{Name: "z", Type: arrow.PrimitiveTypes.Float64, Nullable: false}
	case XYM:
		fields[2] = arrow.Field{Name: "m", Type: arrow.PrimitiveTypes.Float64, Nullable: false}
	case XYZM:
		fields[2] = arrow.Field{Name: "z", Type: arrow.PrimitiveTypes.Float64, Nullable: false}
		fields[3] = arrow.Field{Name: "m", Type: arrow.PrimitiveTypes.Float64, Nullable: false}
	}
	return arrow.StructOf(fields...)
}

// listOfNamed returns a List<elem> where the element field is named.
// GeoArrow requires specific inner field names (e.g. "vertices", "rings",
// "points", "polygons") to disambiguate nested storage.
func listOfNamed(name string, elem arrow.DataType) arrow.DataType {
	return arrow.ListOfField(arrow.Field{Name: name, Type: elem, Nullable: false})
}

// checkCoordStorage validates that dt is a valid GeoArrow coord storage type
// (separated struct or interleaved FSL) and returns its dimension.
func checkCoordStorage(dt arrow.DataType) (Dimension, error) {
	switch t := dt.(type) {
	case *arrow.StructType:
		if err := checkCoordStructFields(t); err != nil {
			return 0, err
		}
		return DimensionFromStructType(t), nil
	case *arrow.FixedSizeListType:
		if err := checkCoordInterleaved(t); err != nil {
			return 0, err
		}
		return DimensionFromInterleavedType(t), nil
	default:
		return 0, fmt.Errorf("unsupported storage type: %s", dt)
	}
}

// readCoordAt reads one coordinate at index i from a coord array
// (either *array.Struct or *array.FixedSizeList) into dst. dst must be
// pre-sized to the dimension's stride.
func readCoordAt(coordArr arrow.Array, i int, dst []float64) {
	switch a := coordArr.(type) {
	case *array.Struct:
		for f := range a.NumField() {
			dst[f] = a.Field(f).(*array.Float64).Value(i)
		}
	case *array.FixedSizeList:
		stride := int(a.DataType().(*arrow.FixedSizeListType).Len())
		vals := a.ListValues().(*array.Float64)
		start, _ := a.ValueOffsets(i)
		base := int(start)
		for f := range stride {
			dst[f] = vals.Value(base + f)
		}
	default:
		panic(fmt.Sprintf("readCoordAt: unsupported coord array type %T", coordArr))
	}
}

// readCoordsRange reads a contiguous range [start, end) of coordinates from a
// coord array into a flat interleaved slice of length (end-start)*stride.
func readCoordsRange(coordArr arrow.Array, start, end, stride int) []float64 {
	n := end - start
	out := make([]float64, n*stride)
	for i := range n {
		readCoordAt(coordArr, start+i, out[i*stride:(i+1)*stride])
	}
	return out
}

// appendCoord appends one coordinate (length == stride) to a coord builder
// (either *array.StructBuilder or *array.FixedSizeListBuilder).
func appendCoord(b array.Builder, coord []float64) {
	switch bb := b.(type) {
	case *array.StructBuilder:
		bb.Append(true)
		for i, c := range coord {
			bb.FieldBuilder(i).(*array.Float64Builder).Append(c)
		}
	case *array.FixedSizeListBuilder:
		bb.Append(true)
		bb.ValueBuilder().(*array.Float64Builder).AppendValues(coord, nil)
	default:
		panic(fmt.Sprintf("appendCoord: unsupported coord builder type %T", b))
	}
}

// appendCoords appends a flat interleaved coord slice to a coord builder.
func appendCoords(b array.Builder, coords []float64, stride int) {
	n := len(coords) / stride
	for i := range n {
		appendCoord(b, coords[i*stride:(i+1)*stride])
	}
}
