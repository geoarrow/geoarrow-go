package geoarrow

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// coordStructStorage returns the GeoArrow separated struct storage
// (Struct<x, y[, z][, m]>) for a coordinate of the given dimension.
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

// interleavedFieldName returns the GeoArrow field name for an interleaved
// coordinate list, used to disambiguate XYZ vs XYM at length 3.
func interleavedFieldName(dim Dimension) string {
	switch dim {
	case XYZ:
		return "xyz"
	case XYM:
		return "xym"
	case XYZM:
		return "xyzm"
	default:
		return "xy"
	}
}

// interleavedStorage returns the GeoArrow interleaved coord storage
// (FixedSizeList<float64>[n_dim]) for the given dimension.
func interleavedStorage(dim Dimension) arrow.DataType {
	return arrow.FixedSizeListOfField(int32(dim.NDim()), arrow.Field{
		Name: interleavedFieldName(dim), Type: arrow.PrimitiveTypes.Float64, Nullable: false,
	})
}

// listOfNamed returns a List<elem> where the element field carries the given
// name. GeoArrow requires specific inner field names (e.g. "vertices",
// "rings") to disambiguate nested storage.
func listOfNamed(name string, elem arrow.DataType) arrow.DataType {
	return arrow.ListOfField(arrow.Field{Name: name, Type: elem, Nullable: false})
}

// nestedListStorage wraps elem in len(names) layers of List, naming each
// layer's element field. names[0] is the outermost layer. Use for polygon
// (["rings","vertices"]) and multipolygon (["polygons","rings","vertices"]).
func nestedListStorage(elem arrow.DataType, names ...string) arrow.DataType {
	for i := len(names) - 1; i >= 0; i-- {
		elem = listOfNamed(names[i], elem)
	}
	return elem
}

// unwrapNestedLists peels depth layers of List off storage and returns the
// innermost element type. Errors if any layer is not a List.
func unwrapNestedLists(storage arrow.DataType, depth int) (arrow.DataType, error) {
	cur := storage
	for i := range depth {
		lt, ok := cur.(*arrow.ListType)
		if !ok {
			return nil, fmt.Errorf("expected List at depth %d, got %s", i, cur)
		}
		cur = lt.ElemField().Type
	}
	return cur, nil
}

// DimensionFromStructType determines dim from a separated coord struct.
// Exported for use by downstream geometry types (LineString, MultiPolygon,
// etc.) and by tools that need to introspect existing Arrow schemas.
func DimensionFromStructType(st *arrow.StructType) Dimension {
	switch st.NumFields() {
	case 2:
		return XY
	case 3:
		if st.Field(2).Name == "z" {
			return XYZ
		}
		return XYM
	case 4:
		return XYZM
	default:
		return XY
	}
}

// DimensionFromInterleavedType determines dim from an interleaved coord FSL.
// At length 3 the field name disambiguates XYZ ("xyz") vs XYM ("xym").
// Exported for the same reasons as DimensionFromStructType.
func DimensionFromInterleavedType(fsl *arrow.FixedSizeListType) Dimension {
	switch fsl.Len() {
	case 2:
		return XY
	case 3:
		if fsl.ElemField().Name == "xym" {
			return XYM
		}
		return XYZ
	case 4:
		return XYZM
	default:
		return XY
	}
}

// DimensionFromStorage determines dim from any supported coord storage
// (separated Struct or interleaved FSL). Exported as the umbrella helper
// used by downstream geometry types when inspecting nested storage.
func DimensionFromStorage(dt arrow.DataType) Dimension {
	switch st := dt.(type) {
	case *arrow.StructType:
		return DimensionFromStructType(st)
	case *arrow.FixedSizeListType:
		return DimensionFromInterleavedType(st)
	default:
		return XY
	}
}

func checkCoordStructFields(coord *arrow.StructType) error {
	dim := DimensionFromStructType(coord)
	if coord.NumFields() != dim.NDim() {
		return fmt.Errorf("storage struct has %d fields but expected %d for dimension %d", coord.NumFields(), dim.NDim(), dim)
	}
	names := make(map[string]bool, coord.NumFields())
	for i := range coord.NumFields() {
		names[coord.Field(i).Name] = true
	}
	if !names["x"] || !names["y"] {
		return fmt.Errorf("storage struct must have 'x' and 'y' fields")
	}
	switch dim {
	case XYZ:
		if !names["z"] {
			return fmt.Errorf("storage struct must have 'z' field for XYZ dimension")
		}
	case XYM:
		if !names["m"] {
			return fmt.Errorf("storage struct must have 'm' field for XYM dimension")
		}
	case XYZM:
		if !names["z"] || !names["m"] {
			return fmt.Errorf("storage struct must have 'z' and 'm' fields for XYZM dimension")
		}
	}
	return nil
}

func checkCoordInterleaved(coord *arrow.FixedSizeListType) error {
	if coord.Elem().ID() != arrow.PrimitiveTypes.Float64.ID() {
		return fmt.Errorf("interleaved storage must have float64 element type")
	}
	if coord.Len() < 2 || coord.Len() > 4 {
		return fmt.Errorf("interleaved storage must have length between 2 and 4 for valid dimensions")
	}
	return nil
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

// readCoordAt reads one coord at index i from a coord array (Struct or FSL)
// into dst. len(dst) determines how many values to read.
func readCoordAt(coordArr arrow.Array, i int, dst []float64) {
	switch a := coordArr.(type) {
	case *array.Struct:
		for f := range len(dst) {
			dst[f] = a.Field(f).(*array.Float64).Value(i)
		}
	case *array.FixedSizeList:
		vals := a.ListValues().(*array.Float64)
		start, _ := a.ValueOffsets(i)
		base := int(start)
		for f := range len(dst) {
			dst[f] = vals.Value(base + f)
		}
	default:
		// Unreachable when callers have validated storage via
		// checkCoordStorage during type construction / Deserialize.
		panic(fmt.Sprintf("readCoordAt: unsupported coord array type %T", coordArr))
	}
}

// readCoordsRange reads coords [start, end) from a coord array into a flat
// interleaved []float64 of length (end-start)*stride. The type assertion is
// hoisted out of the per-coord loop so large reads stay tight.
func readCoordsRange(coordArr arrow.Array, start, end, stride int) []float64 {
	n := end - start
	out := make([]float64, n*stride)
	switch a := coordArr.(type) {
	case *array.Struct:
		fields := make([]*array.Float64, stride)
		for f := range stride {
			fields[f] = a.Field(f).(*array.Float64)
		}
		for i := range n {
			row := start + i
			base := i * stride
			for f := range stride {
				out[base+f] = fields[f].Value(row)
			}
		}
	case *array.FixedSizeList:
		vals := a.ListValues().(*array.Float64).Float64Values()
		baseStart, _ := a.ValueOffsets(start)
		copy(out, vals[int(baseStart):int(baseStart)+n*stride])
	default:
		// Unreachable when callers have validated storage via
		// checkCoordStorage during type construction / Deserialize.
		panic(fmt.Sprintf("readCoordsRange: unsupported coord array type %T", coordArr))
	}
	return out
}

// appendCoord appends one coord (len(coord) == stride) to a coord builder.
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
		// Unreachable: builder type is determined by the validated storage.
		panic(fmt.Sprintf("appendCoord: unsupported coord builder type %T", b))
	}
}

// appendCoords appends a flat interleaved slice of n*stride floats to a
// coord builder, hoisting the builder type assertion above the inner loop.
func appendCoords(b array.Builder, coords []float64, stride int) {
	n := len(coords) / stride
	switch bb := b.(type) {
	case *array.StructBuilder:
		fields := make([]*array.Float64Builder, stride)
		for f := range stride {
			fields[f] = bb.FieldBuilder(f).(*array.Float64Builder)
		}
		for i := range n {
			bb.Append(true)
			base := i * stride
			for f := range stride {
				fields[f].Append(coords[base+f])
			}
		}
	case *array.FixedSizeListBuilder:
		inner := bb.ValueBuilder().(*array.Float64Builder)
		for i := range n {
			bb.Append(true)
			inner.AppendValues(coords[i*stride:(i+1)*stride], nil)
		}
	default:
		// Unreachable: builder type is determined by the validated storage.
		panic(fmt.Sprintf("appendCoords: unsupported coord builder type %T", b))
	}
}
