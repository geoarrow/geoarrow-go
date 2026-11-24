package geoarrow

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BoxValue is an axis-aligned rectangle (or box, with Z and/or M ranges). It
// holds a [min, max] range per dimension, in x, y[, z][, m] order.
//
// Following the GeoArrow spec, xmin > xmax is valid and means the box wraps
// the antimeridian (it contains x >= xmin OR x <= xmax); see
// CrossesAntimeridian. A range with min = +Inf and max = -Inf is empty. The
// returned slices alias the value's storage and must not be modified.
type BoxValue struct {
	min []float64
	max []float64
	dim Dimension
}

// NewBoxValue returns an XY box.
func NewBoxValue(xmin, ymin, xmax, ymax float64) BoxValue {
	return BoxValue{min: []float64{xmin, ymin}, max: []float64{xmax, ymax}, dim: XY}
}

// NewBoxValueZ returns an XYZ box.
func NewBoxValueZ(xmin, ymin, zmin, xmax, ymax, zmax float64) BoxValue {
	return BoxValue{min: []float64{xmin, ymin, zmin}, max: []float64{xmax, ymax, zmax}, dim: XYZ}
}

// NewBoxValueM returns an XYM box.
func NewBoxValueM(xmin, ymin, mmin, xmax, ymax, mmax float64) BoxValue {
	return BoxValue{min: []float64{xmin, ymin, mmin}, max: []float64{xmax, ymax, mmax}, dim: XYM}
}

// NewBoxValueZM returns an XYZM box.
func NewBoxValueZM(xmin, ymin, zmin, mmin, xmax, ymax, zmax, mmax float64) BoxValue {
	return BoxValue{
		min: []float64{xmin, ymin, zmin, mmin},
		max: []float64{xmax, ymax, zmax, mmax},
		dim: XYZM,
	}
}

// NewBoxValueFromBounds returns a box of the given dimension from its minimum
// and maximum corners, each in x, y[, z][, m] order. It panics if either
// slice's length is not dim.NDim().
func NewBoxValueFromBounds(dim Dimension, min, max []float64) BoxValue {
	if len(min) != dim.NDim() || len(max) != dim.NDim() {
		panic(fmt.Sprintf("geoarrow.box: %s box needs %d values per corner, got %d and %d", dim, dim.NDim(), len(min), len(max)))
	}
	return BoxValue{min: min, max: max, dim: dim}
}

// NewEmptyBoxValue returns an empty box of the given dimension (every range
// is min=+Inf, max=-Inf).
func NewEmptyBoxValue(dim Dimension) BoxValue {
	n := dim.NDim()
	lo, hi := make([]float64, n), make([]float64, n)
	for i := range n {
		lo[i], hi[i] = math.Inf(1), math.Inf(-1)
	}
	return BoxValue{min: lo, max: hi, dim: dim}
}

// Min returns the minimum of each dimension in x, y[, z][, m] order.
func (v BoxValue) Min() []float64 { return v.min }

// Max returns the maximum of each dimension in x, y[, z][, m] order.
func (v BoxValue) Max() []float64 { return v.max }

// bound returns the value at index i of s, or NaN if absent.
func bound(s []float64, i int) float64 {
	if i < 0 || i >= len(s) {
		return math.NaN()
	}
	return s[i]
}

// zIndex and mIndex return the position of the Z or M ordinate in a coord, or
// -1 if the dimension has none.
func (v BoxValue) zIndex() int {
	if v.dim == XYZ || v.dim == XYZM {
		return 2
	}
	return -1
}

func (v BoxValue) mIndex() int {
	switch v.dim {
	case XYM:
		return 2
	case XYZM:
		return 3
	}
	return -1
}

// XMin returns the minimum X, or NaN for a null value.
func (v BoxValue) XMin() float64 { return bound(v.min, 0) }

// YMin returns the minimum Y, or NaN for a null value.
func (v BoxValue) YMin() float64 { return bound(v.min, 1) }

// ZMin returns the minimum Z, or NaN if the box has no Z dimension.
func (v BoxValue) ZMin() float64 { return bound(v.min, v.zIndex()) }

// MMin returns the minimum M, or NaN if the box has no M dimension.
func (v BoxValue) MMin() float64 { return bound(v.min, v.mIndex()) }

// XMax returns the maximum X, or NaN for a null value.
func (v BoxValue) XMax() float64 { return bound(v.max, 0) }

// YMax returns the maximum Y, or NaN for a null value.
func (v BoxValue) YMax() float64 { return bound(v.max, 1) }

// ZMax returns the maximum Z, or NaN if the box has no Z dimension.
func (v BoxValue) ZMax() float64 { return bound(v.max, v.zIndex()) }

// MMax returns the maximum M, or NaN if the box has no M dimension.
func (v BoxValue) MMax() float64 { return bound(v.max, v.mIndex()) }

// CrossesAntimeridian reports whether the X range wraps the antimeridian
// (xmin > xmax) rather than being empty or an ordinary interval.
func (v BoxValue) CrossesAntimeridian() bool {
	if len(v.min) == 0 {
		return false
	}
	return v.min[0] > v.max[0] && !(math.IsInf(v.min[0], 1) && math.IsInf(v.max[0], -1))
}

// Dimension returns the dimension of v.
func (v BoxValue) Dimension() Dimension { return v.dim }

// GeometryType returns 0: a box is not one of the GeoArrow geometry types and
// has no GeometryTypeID.
func (v BoxValue) GeometryType() GeometryTypeID { return 0 }

// IsEmpty reports whether v has no values or any dimension's range is empty
// (min=+Inf, max=-Inf).
func (v BoxValue) IsEmpty() bool {
	if len(v.min) == 0 {
		return true
	}
	for i := range v.min {
		if math.IsInf(v.min[i], 1) && math.IsInf(v.max[i], -1) {
			return true
		}
	}
	return false
}

// String renders v as "BOX(xmin ymin, xmax ymax)", with " Z", " M" or " ZM"
// after BOX for other dimensions, or "BOX EMPTY" for an empty box. This is a
// WKT-like notation (WKT has no box type) that valueFromString parses back.
func (v BoxValue) String() string {
	if v.IsEmpty() {
		return "BOX" + dimSuffix(v.dim) + " EMPTY"
	}
	var b strings.Builder
	b.WriteString("BOX")
	b.WriteString(dimSuffix(v.dim))
	b.WriteByte('(')
	formatBoxCoord(&b, v.min)
	b.WriteString(", ")
	formatBoxCoord(&b, v.max)
	b.WriteByte(')')
	return b.String()
}

// formatBoxCoord writes one box corner. Unlike formatCoord it uses the
// shortest representation that round-trips, so a box survives
// ValueStr / AppendValueFromString exactly.
func formatBoxCoord(b *strings.Builder, coord []float64) {
	for i, c := range coord {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(strconv.FormatFloat(c, 'f', -1, 64))
	}
}

// jsonFloat returns f for finite values and the string form arrow-go uses for
// NaN and infinities ("NaN", "+Inf", "-Inf"), since JSON numbers cannot
// represent them.
func jsonFloat(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return f
}

// MarshalJSON encodes v as a flat array of the mins then the maxes, in the
// GeoArrow struct field order: [xmin, ymin, xmax, ymax] for XY and
// [xmin, ymin, zmin, xmax, ymax, zmax] for XYZ (the GeoJSON bbox layout).
// Infinite values become strings, as arrow-go encodes them.
func (v BoxValue) MarshalJSON() ([]byte, error) {
	out := make([]any, 0, len(v.min)+len(v.max))
	for _, f := range v.min {
		out = append(out, jsonFloat(f))
	}
	for _, f := range v.max {
		out = append(out, jsonFloat(f))
	}
	return json.Marshal(out)
}

// BoxType is the GeoArrow extension type for boxes (geoarrow.box). Storage is
// Struct<xmin, ymin, [zmin,] [mmin,] xmax, ymax, [zmax,] [mmax]> of float64.
type BoxType struct {
	arrow.ExtensionBase
	Extension
}

type boxOption func(*BoxType)

// BoxWithStorage overrides the Arrow storage type. The struct must follow the
// field names and order the spec requires for its dimension.
func BoxWithStorage(storage arrow.DataType) boxOption {
	return func(t *BoxType) {
		t.Storage = storage
	}
}

// BoxWithMetadata replaces the GeoArrow metadata on the type.
func BoxWithMetadata(metadata Metadata) boxOption {
	return func(t *BoxType) {
		t.meta = metadata
	}
}

// BoxWithDimension uses the box struct storage for the given dim.
func BoxWithDimension(dim Dimension) boxOption {
	return func(t *BoxType) {
		t.Storage = boxStorage(dim)
	}
}

// boxFieldNames returns the spec-mandated struct field names for dim: the
// mins in coordinate order followed by the maxes.
func boxFieldNames(dim Dimension) []string {
	switch dim {
	case XYZ:
		return []string{"xmin", "ymin", "zmin", "xmax", "ymax", "zmax"}
	case XYM:
		return []string{"xmin", "ymin", "mmin", "xmax", "ymax", "mmax"}
	case XYZM:
		return []string{"xmin", "ymin", "zmin", "mmin", "xmax", "ymax", "zmax", "mmax"}
	default:
		return []string{"xmin", "ymin", "xmax", "ymax"}
	}
}

func boxStorage(dim Dimension) arrow.DataType {
	names := boxFieldNames(dim)
	fields := make([]arrow.Field, len(names))
	for i, name := range names {
		fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: false}
	}
	return arrow.StructOf(fields...)
}

// boxDimension returns the dimension of box struct storage, or an error if the
// struct does not match any layout the spec allows.
func boxDimension(dt arrow.DataType) (Dimension, error) {
	st, ok := dt.(*arrow.StructType)
	if !ok {
		return 0, fmt.Errorf("unsupported storage type: %s", dt)
	}
	for _, dim := range []Dimension{XY, XYZ, XYM, XYZM} {
		names := boxFieldNames(dim)
		if st.NumFields() != len(names) {
			continue
		}
		match := true
		for i, name := range names {
			f := st.Field(i)
			if f.Name != name || f.Type.ID() != arrow.FLOAT64 {
				match = false
				break
			}
		}
		if match {
			return dim, nil
		}
	}
	return 0, fmt.Errorf("storage struct fields do not match a GeoArrow box layout: %s", dt)
}

// NewBoxType constructs a BoxType with the given options. The default storage
// is the XY struct <xmin, ymin, xmax, ymax>.
func NewBoxType(opts ...boxOption) *BoxType {
	t := &BoxType{
		ExtensionBase: arrow.ExtensionBase{Storage: boxStorage(XY)},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

func (*BoxType) ExtensionName() string {
	return ExtensionNameBox
}

func (*BoxType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}
	if _, err := boxDimension(storageType); err != nil {
		return nil, fmt.Errorf("geoarrow.box: %w", err)
	}
	return NewBoxType(BoxWithStorage(storageType), BoxWithMetadata(meta)), nil
}

func (t *BoxType) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*BoxType)
	if !ok {
		return false
	}
	if t == nil && o == nil {
		return true
	}
	if t == nil || o == nil {
		return false
	}
	return arrow.TypeEqual(t.Storage, o.Storage) && t.Equal(&o.Extension)
}

func (*BoxType) ArrayType() reflect.Type {
	return reflect.TypeFor[BoxArray]()
}

// dim returns the dimension of the type's storage. Only safe after
// construction / Deserialize has validated the storage shape.
func (t *BoxType) dim() Dimension {
	dim, _ := boxDimension(t.StorageType())
	return dim
}

func (t *BoxType) valueFromArray(a array.ExtensionArray, i int) BoxValue {
	dim := t.dim()
	if a.IsNull(i) {
		return BoxValue{dim: dim}
	}
	st := a.Storage().(*array.Struct)
	n := dim.NDim()
	lo, hi := make([]float64, n), make([]float64, n)
	for d := range n {
		lo[d] = st.Field(d).(*array.Float64).Value(i)
		hi[d] = st.Field(n + d).(*array.Float64).Value(i)
	}
	return BoxValue{min: lo, max: hi, dim: dim}
}

func (t *BoxType) appendValueToBuilder(b array.Builder, v BoxValue) {
	sb := b.(*array.StructBuilder)
	dim, _ := boxDimension(sb.Type())
	n := dim.NDim()
	if len(v.min) == 0 {
		v = NewEmptyBoxValue(dim)
	}
	if len(v.min) != n || len(v.max) != n {
		panic(fmt.Sprintf("geoarrow.box: value has %d dimensions, builder expects %d", len(v.min), n))
	}
	sb.Append(true)
	for d := range n {
		sb.FieldBuilder(d).(*array.Float64Builder).Append(v.min[d])
		sb.FieldBuilder(n + d).(*array.Float64Builder).Append(v.max[d])
	}
}

func (t *BoxType) valueFromString(s string) (BoxValue, error) {
	storageDim := t.dim()
	prefix, body, isEmpty, err := wktSplit(s, "BOX")
	if err != nil {
		return BoxValue{}, err
	}
	if isEmpty {
		return NewEmptyBoxValue(storageDim), nil
	}
	coords, stride, err := parseWKTFlatCoords(body)
	if err != nil {
		return BoxValue{}, fmt.Errorf("box: %w", err)
	}
	if stride == 0 || len(coords) != 2*stride {
		return BoxValue{}, fmt.Errorf("box: expected a min and a max vertex, got %d values", len(coords))
	}
	dim, err := wktDimension(prefix, stride)
	if err != nil {
		return BoxValue{}, fmt.Errorf("box: %w", err)
	}
	if err := checkStorageDim("box", dim, storageDim); err != nil {
		return BoxValue{}, err
	}
	return BoxValue{min: coords[:stride], max: coords[stride:], dim: dim}, nil
}

// decodeBoxFloats reads the flat JSON array after its opening '[': numbers, or
// the strings "NaN", "+Inf" and "-Inf" that MarshalJSON writes.
func decodeBoxFloats(dec *json.Decoder) ([]float64, error) {
	var out []float64
	for dec.More() {
		var raw any
		if err := dec.Decode(&raw); err != nil {
			return nil, err
		}
		switch x := raw.(type) {
		case float64:
			out = append(out, x)
		case json.Number: // array.FromJSON decodes with UseNumber
			f, err := x.Float64()
			if err != nil {
				return nil, fmt.Errorf("invalid box value %q: %w", x, err)
			}
			out = append(out, f)
		case string:
			f, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid box value %q: %w", x, err)
			}
			out = append(out, f)
		default:
			return nil, fmt.Errorf("invalid box value %v (%T)", raw, raw)
		}
	}
	if _, err := dec.Token(); err != nil { // closing ']'
		return nil, err
	}
	return out, nil
}

func (t *BoxType) unmarshalJSONOne(dec *json.Decoder) (BoxValue, bool, error) {
	isNull, err := decodeOpenOrNull(dec)
	if err != nil {
		return BoxValue{}, false, err
	}
	if isNull {
		return BoxValue{}, true, nil
	}
	vals, err := decodeBoxFloats(dec)
	if err != nil {
		return BoxValue{}, false, err
	}
	dim := t.dim()
	n := dim.NDim()
	if len(vals) != 2*n {
		return BoxValue{}, false, fmt.Errorf("box: expected %d values for %s, got %d", 2*n, dim, len(vals))
	}
	return BoxValue{min: vals[:n], max: vals[n:], dim: dim}, false, nil
}

func (t *BoxType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[BoxValue, *BoxType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, t),
	}
}

// BoxArray is the Arrow ExtensionArray for geoarrow.box.
type BoxArray = geometryArray[BoxValue, *BoxType]

// BoxBuilder is the Arrow Builder for geoarrow.box.
type BoxBuilder = valueBuilder[BoxValue, *BoxType]

var _ array.CustomExtensionBuilder = (*BoxType)(nil)
