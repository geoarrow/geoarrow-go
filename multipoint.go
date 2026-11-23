package geoarrow

import (
	"fmt"
	"reflect"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// MultiPointValue represents a multipoint geometry. Points are stored as
// interleaved flat coordinates [x1,y1,x2,y2,...]. The returned slices alias
// the value's storage and must not be modified.
type MultiPointValue struct {
	coords []float64
	dim    Dimension
}

// NewMultiPointValue returns a multipoint of the given dimension from flat
// interleaved coords (length n_points * dim.NDim()).
func NewMultiPointValue(dim Dimension, coords []float64) MultiPointValue {
	return MultiPointValue{coords: coords, dim: dim}
}

// NumPoints returns the number of points in v.
func (v MultiPointValue) NumPoints() int {
	return len(v.coords) / v.dim.NDim()
}

// Point returns point i.
func (v MultiPointValue) Point(i int) PointValue {
	stride := v.dim.NDim()
	return PointValue{coords: v.coords[i*stride : (i+1)*stride], dim: v.dim}
}

// Coords returns the flat interleaved coordinates of v.
func (v MultiPointValue) Coords() []float64 {
	return v.coords
}

// Dimension returns the coordinate dimension of v.
func (v MultiPointValue) Dimension() Dimension {
	return v.dim
}

// GeometryType returns the GeoArrow GeometryTypeID for v.
func (v MultiPointValue) GeometryType() GeometryTypeID {
	return geometryTypeID(MultiPointID, v.dim)
}

// IsEmpty reports whether v has no points.
func (v MultiPointValue) IsEmpty() bool {
	return len(v.coords) == 0
}

// String renders v as WKT (e.g. "MULTIPOINT((0 0), (1 1))").
func (v MultiPointValue) String() string {
	if v.IsEmpty() {
		return "MULTIPOINT EMPTY"
	}
	var b strings.Builder
	b.WriteString("MULTIPOINT")
	b.WriteString(dimSuffix(v.dim))
	b.WriteByte('(')
	stride := v.dim.NDim()
	for i := range v.NumPoints() {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		formatCoord(&b, v.coords[i*stride:(i+1)*stride])
		b.WriteByte(')')
	}
	b.WriteByte(')')
	return b.String()
}

// MarshalJSON encodes v as a JSON array of coordinate arrays.
func (v MultiPointValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(vertices(v.coords, v.dim.NDim()))
}

// MultiPointType is the GeoArrow extension type for MultiPoint geometries
// (geoarrow.multipoint). Storage is List<points: Coord>.
type MultiPointType struct {
	arrow.ExtensionBase
	Extension
}

type multiPointOption func(*MultiPointType)

// MultiPointWithStorage overrides the Arrow storage type. Use when
// constructing a MultiPointType with non-default field names or a custom coord
// storage.
func MultiPointWithStorage(storage arrow.DataType) multiPointOption {
	return func(t *MultiPointType) {
		t.Storage = storage
	}
}

// MultiPointWithMetadata replaces the GeoArrow metadata on the type.
func MultiPointWithMetadata(metadata Metadata) multiPointOption {
	return func(t *MultiPointType) {
		t.meta = metadata
	}
}

// multiPointNesting names the List layers in multipoint storage from outside
// in.
var multiPointNesting = []string{"points"}

func multiPointStorage(coordType arrow.DataType) arrow.DataType {
	return nestedListStorage(coordType, multiPointNesting...)
}

// MultiPointWithDimension uses separated struct coord storage for the given
// dim.
func MultiPointWithDimension(dim Dimension) multiPointOption {
	return func(t *MultiPointType) {
		t.Storage = multiPointStorage(coordStructStorage(dim))
	}
}

// MultiPointWithInterleaved uses interleaved FSL coord storage for the given
// dim.
func MultiPointWithInterleaved(dim Dimension) multiPointOption {
	return func(t *MultiPointType) {
		t.Storage = multiPointStorage(interleavedStorage(dim))
	}
}

// NewMultiPointType constructs a MultiPointType with the given options. The
// default storage is separated XY Struct coords under "points".
func NewMultiPointType(opts ...multiPointOption) *MultiPointType {
	t := &MultiPointType{
		ExtensionBase: arrow.ExtensionBase{Storage: multiPointStorage(coordStructStorage(XY))},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

func (*MultiPointType) ExtensionName() string {
	return ExtensionNameMultiPoint
}

func (*MultiPointType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}
	coordType, err := unwrapNestedLists(storageType, len(multiPointNesting))
	if err != nil {
		return nil, fmt.Errorf("geoarrow.multipoint: %w", err)
	}
	if _, err := checkCoordStorage(coordType); err != nil {
		return nil, fmt.Errorf("geoarrow.multipoint: %w", err)
	}
	return NewMultiPointType(MultiPointWithStorage(storageType), MultiPointWithMetadata(meta)), nil
}

func (t *MultiPointType) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*MultiPointType)
	if !ok {
		return false
	}
	if t == nil && o == nil {
		return true
	}
	if t == nil || o == nil {
		return false
	}
	return sameNestedStorage(t.Storage, o.Storage, len(multiPointNesting)) && t.Equal(&o.Extension)
}

func (*MultiPointType) ArrayType() reflect.Type {
	return reflect.TypeFor[MultiPointArray]()
}

// dim returns the coordinate dimension of the type's storage. Only safe after
// construction / Deserialize has validated the storage shape.
func (t *MultiPointType) dim() Dimension {
	coord, _ := unwrapNestedLists(t.StorageType(), len(multiPointNesting))
	return DimensionFromStorage(coord)
}

func (t *MultiPointType) valueFromArray(a array.ExtensionArray, i int) MultiPointValue {
	dim := t.dim()
	if a.IsNull(i) {
		return MultiPointValue{dim: dim}
	}
	return MultiPointValue{coords: readCoordList(a, i, dim.NDim()), dim: dim}
}

func (t *MultiPointType) appendValueToBuilder(b array.Builder, v MultiPointValue) {
	dim := t.dim()
	checkAppendDim("geoarrow.multipoint", v.dim, dim, v.IsEmpty())
	appendCoordList(b, v.coords, dim.NDim())
}

func (t *MultiPointType) valueFromString(s string) (MultiPointValue, error) {
	storageDim := t.dim()
	prefix, body, isEmpty, err := wktSplit(s, "MULTIPOINT")
	if err != nil {
		return MultiPointValue{}, err
	}
	if isEmpty {
		return MultiPointValue{dim: storageDim}, nil
	}
	coords, stride, err := parseWKTMultiPointCoords(body)
	if err != nil {
		return MultiPointValue{}, fmt.Errorf("multipoint WKT: %w", err)
	}
	dim, err := wktDimension(prefix, stride)
	if err != nil {
		return MultiPointValue{}, fmt.Errorf("multipoint WKT: %w", err)
	}
	if err := checkStorageDim("multipoint", dim, storageDim); err != nil {
		return MultiPointValue{}, err
	}
	return MultiPointValue{coords: coords, dim: dim}, nil
}

func (t *MultiPointType) unmarshalJSONOne(dec *json.Decoder) (MultiPointValue, bool, error) {
	isNull, err := decodeOpenOrNull(dec)
	if err != nil {
		return MultiPointValue{}, false, err
	}
	if isNull {
		return MultiPointValue{}, true, nil
	}
	coords, err := decodeCoordList(dec)
	if err != nil {
		return MultiPointValue{}, false, err
	}
	return MultiPointValue{coords: coords, dim: t.dim()}, false, nil
}

func (t *MultiPointType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[MultiPointValue, *MultiPointType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, t),
	}
}

// MultiPointArray is the Arrow ExtensionArray for geoarrow.multipoint.
type MultiPointArray = geometryArray[MultiPointValue, *MultiPointType]

// MultiPointBuilder is the Arrow Builder for geoarrow.multipoint.
type MultiPointBuilder = valueBuilder[MultiPointValue, *MultiPointType]

var _ array.CustomExtensionBuilder = (*MultiPointType)(nil)
