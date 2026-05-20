package geoarrow

import (
	"fmt"
	"math"
	"reflect"
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
		return "POINT" + dimSuffix(v.dim) + " EMPTY"
	}
	var b strings.Builder
	b.WriteString("POINT")
	b.WriteString(dimSuffix(v.dim))
	b.WriteByte('(')
	formatCoord(&b, v.coords)
	b.WriteByte(')')
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

// PointWithInterleaved configures the PointType to use interleaved coord
// storage: FixedSizeList<float64>[n_dim].
func PointWithInterleaved(dim Dimension) pointOption {
	return func(pt *PointType) {
		pt.Storage = interleavedStorage(dim)
	}
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
	prefix, body, isEmpty, err := wktSplit(s, "POINT")
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
	dim, err := dimFromWKTPrefix(prefix, stride)
	if err != nil {
		return PointValue{}, fmt.Errorf("point WKT: %w", err)
	}
	return PointValue{coords: coords, dim: dim}, nil
}

func (pt *PointType) unmarshalJSONOne(dec *json.Decoder) (PointValue, bool, error) {
	isNull, err := decodeOpenOrNull(dec)
	if err != nil {
		return PointValue{}, false, err
	}
	if isNull {
		return PointValue{}, true, nil
	}

	coords, err := decodeFloatsAfterOpen(dec)
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
