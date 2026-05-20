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

type PointType struct {
	arrow.ExtensionBase
	Extension
}

type PointValue struct {
	coords []float64
	dim    Dimension
}

func NewPointValue(x, y float64) PointValue {
	return PointValue{coords: []float64{x, y}, dim: XY}
}

func NewPointValueZ(x, y, z float64) PointValue {
	return PointValue{coords: []float64{x, y, z}, dim: XYZ}
}

func NewPointValueM(x, y, m float64) PointValue {
	return PointValue{coords: []float64{x, y, m}, dim: XYM}
}

func NewPointValueZM(x, y, z, m float64) PointValue {
	return PointValue{coords: []float64{x, y, z, m}, dim: XYZM}
}

func (v PointValue) X() float64 {
	if len(v.coords) < 1 {
		return math.NaN()
	}
	return v.coords[0]
}

func (v PointValue) Y() float64 {
	if len(v.coords) < 2 {
		return math.NaN()
	}
	return v.coords[1]
}

func (v PointValue) Z() float64 {
	if v.dim != XYZ && v.dim != XYZM {
		return math.NaN()
	}
	return v.coords[2]
}

func (v PointValue) M() float64 {
	if v.dim != XYM && v.dim != XYZM {
		return math.NaN()
	}
	return v.coords[len(v.coords)-1]
}

func (v PointValue) Dimension() Dimension {
	return v.dim
}

func (v PointValue) GeometryType() GeometryTypeID {
	switch v.dim {
	case XY:
		return PointID
	case XYZ:
		return PointZID
	case XYM:
		return PointMID
	case XYZM:
		return PointZMID
	default:
		panic("invalid coordinate dimension for PointValue")
	}
}

func (v PointValue) Coordinates() []float64 {
	return v.coords
}

func (v PointValue) String() string {
	if v.IsEmpty() {
		return "POINT EMPTY"
	}
	b := strings.Builder{}
	b.WriteString("POINT")
	b.WriteString(dimSuffix(v.dim))
	b.WriteString("(")
	for i, coord := range v.coords {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(strconv.FormatFloat(coord, 'f', 6, 64))
	}
	b.WriteString(")")
	return b.String()
}

func (v PointValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.coords)
}

func (v PointValue) IsEmpty() bool {
	return len(v.coords) == 0
}

func NewPointType(opts ...pointOption) *PointType {
	pt := &PointType{
		ExtensionBase: arrow.ExtensionBase{Storage: coordStructStorage(XY)},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(pt)
		}
	}
	return pt
}

type pointOption func(*PointType)

// PointWithCRS configures the PointType with a specific CRS in the metadata
func PointWithCRS(crs json.RawMessage, crsType CRSType) pointOption {
	return func(pt *PointType) {
		pt.meta.CRS = crs
		pt.meta.CRSType = crsType
	}
}

func pointWithStorage(storage arrow.DataType) pointOption {
	return func(pt *PointType) {
		pt.Storage = storage
	}
}

func PointWithMetadata(metadata Metadata) pointOption {
	return func(pt *PointType) {
		pt.meta = metadata
	}
}

func PointWithDimension(dim Dimension) pointOption {
	return func(pt *PointType) {
		if dim > XYZM {
			panic("invalid dimension for PointType")
		}
		pt.Storage = coordStructStorage(dim)
	}
}

// PointWithInterleaved configures the PointType to use interleaved coordinate
// storage: FixedSizeList<float64>[n_dim] with field name "xy", "xyz", "xym", or "xyzm".
func PointWithInterleaved(dim Dimension) pointOption {
	return func(pt *PointType) {
		pt.Storage = coordStorage(dim, true)
	}
}

// interleavedFieldName returns the field name for interleaved coordinates per the spec.
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

// interleavedStorage returns the interleaved (FixedSizeList<float64>) coord
// storage for a given dimension.
func interleavedStorage(dim Dimension) arrow.DataType {
	return arrow.FixedSizeListOfField(int32(dim.NDim()), arrow.Field{
		Name: interleavedFieldName(dim), Type: arrow.PrimitiveTypes.Float64, Nullable: false,
	})
}

func (pt *PointType) ExtensionName() string {
	return ExtensionNamePoint
}

func (pt *PointType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}

	if _, err := checkCoordStorage(storageType); err != nil {
		return nil, fmt.Errorf("geoarrow.point: %w", err)
	}
	return NewPointType(pointWithStorage(storageType), PointWithMetadata(meta)), nil
}

func (pt *PointType) ExtensionEquals(other arrow.ExtensionType) bool {
	otherPt, ok := other.(*PointType)
	if !ok {
		return false
	}

	if pt == nil && otherPt == nil {
		return true
	}
	if pt == nil || otherPt == nil {
		return false
	}

	return pt.Storage.ID() == otherPt.Storage.ID() && pt.Equal(&otherPt.Extension)
}

func (pt *PointType) ArrayType() reflect.Type {
	return reflect.TypeFor[PointArray]()
}

// DimensionFromStructType determines the coordinate dimension from an Arrow struct type's fields.
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

func checkCoordStructFields(coord *arrow.StructType) error {
	dim := DimensionFromStructType(coord)

	expectedFields := dim.NDim()
	if coord.NumFields() != expectedFields {
		return fmt.Errorf("storage struct has %d fields but expected %d for dimension %d", coord.NumFields(), expectedFields, dim)
	}
	fieldNames := make(map[string]bool)
	for i := 0; i < coord.NumFields(); i++ {
		fieldNames[coord.Field(i).Name] = true
	}
	if !fieldNames["x"] || !fieldNames["y"] {
		return fmt.Errorf("storage struct must have 'x' and 'y' fields")
	}
	switch dim {
	case XYZ:
		if !fieldNames["z"] {
			return fmt.Errorf("storage struct must have 'z' field for XYZ dimension")
		}
	case XYM:
		if !fieldNames["m"] {
			return fmt.Errorf("storage struct must have 'm' field for XYM dimension")
		}
	case XYZM:
		if !fieldNames["z"] || !fieldNames["m"] {
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

// DimensionFromInterleavedType determines dimension from a FixedSizeList storage type.
// For length 3, the field name distinguishes XYZ ("xyz") from XYM ("xym").
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

// DimensionFromStorage determines the coordinate dimension from any supported
// storage type (struct or interleaved fixed-size list).
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

func (pt *PointType) valueFromArray(a array.ExtensionArray, i int) PointValue {
	if a.IsNull(i) {
		return PointValue{}
	}
	dim := DimensionFromStorage(pt.StorageType())
	coords := make([]float64, dim.NDim())
	readCoordAt(a.Storage(), i, coords)
	return PointValue{coords: coords, dim: dim}
}

func (pt *PointType) appendValueToBuilder(b array.Builder, v PointValue) {
	appendCoord(b, v.coords)
}

func (pt *PointType) valueFromString(s string) (PointValue, error) {
	body, isEmpty, err := wktBody(s, "POINT")
	if err != nil {
		return PointValue{}, err
	}
	if isEmpty {
		return PointValue{}, nil
	}

	coords, stride, err := parseWKTFlatCoords(body)
	if err != nil {
		return PointValue{}, fmt.Errorf("point WKT: %w", err)
	}
	if stride < 2 || stride > 4 {
		return PointValue{}, fmt.Errorf("invalid number of coordinates: %d", stride)
	}
	return PointValue{coords: coords, dim: dimFromWKTPrefix(wktPrefix(s), stride)}, nil
}

func (pt *PointType) unmarshalJSONOne(dec *json.Decoder) (PointValue, bool, error) {
	isNull, err := decodeOpenOrNull(dec)
	if err != nil {
		return PointValue{}, false, err
	}
	if isNull {
		return PointValue{}, true, nil
	}

	coords, err := decodeFloatsUntilClose(dec, nil)
	if err != nil {
		return PointValue{}, false, err
	}
	return PointValue{coords: coords, dim: DimensionFromStorage(pt.StorageType())}, false, nil
}

func (pt *PointType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[PointValue, *PointType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, pt),
	}
}

type PointArray = geometryArray[PointValue, *PointType]
type PointBuilder = valueBuilder[PointValue, *PointType]

var _ array.CustomExtensionBuilder = (*PointType)(nil)
