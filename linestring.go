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

// LineStringValue represents a linestring geometry. Vertices are stored as
// interleaved flat coordinates [x1,y1,x2,y2,...]. The returned slices alias
// the value's storage and must not be modified.
type LineStringValue struct {
	coords []float64
	dim    Dimension
}

// NewLineStringValue returns a linestring of the given dimension from flat
// interleaved coords (length n_vertices * dim.NDim()).
func NewLineStringValue(dim Dimension, coords []float64) LineStringValue {
	return LineStringValue{coords: coords, dim: dim}
}

// NumVertices returns the number of vertices in v.
func (v LineStringValue) NumVertices() int {
	return len(v.coords) / v.dim.NDim()
}

// Vertex returns the coordinates of vertex i.
func (v LineStringValue) Vertex(i int) []float64 {
	stride := v.dim.NDim()
	return v.coords[i*stride : (i+1)*stride]
}

// Coords returns the flat interleaved coordinates of v.
func (v LineStringValue) Coords() []float64 {
	return v.coords
}

// Dimension returns the coordinate dimension of v.
func (v LineStringValue) Dimension() Dimension {
	return v.dim
}

// GeometryType returns the GeoArrow GeometryTypeID for v.
func (v LineStringValue) GeometryType() GeometryTypeID {
	return geometryTypeID(LineStringID, v.dim)
}

// IsEmpty reports whether v has no vertices.
func (v LineStringValue) IsEmpty() bool {
	return len(v.coords) == 0
}

// String renders v as WKT (e.g. "LINESTRING(0 0, 1 1)").
func (v LineStringValue) String() string {
	if v.IsEmpty() {
		return "LINESTRING EMPTY"
	}
	var b strings.Builder
	b.WriteString("LINESTRING")
	b.WriteString(dimSuffix(v.dim))
	formatCoordRing(&b, v.coords, v.dim.NDim())
	return b.String()
}

// MarshalJSON encodes v as a JSON array of coordinate arrays.
func (v LineStringValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(vertices(v.coords, v.dim.NDim()))
}

// LineStringType is the GeoArrow extension type for LineString geometries
// (geoarrow.linestring). Storage is List<vertices: Coord>.
type LineStringType struct {
	arrow.ExtensionBase
	Extension
}

type lineStringOption func(*LineStringType)

// LineStringWithStorage overrides the Arrow storage type. Use when
// constructing a LineStringType with non-default field names or a custom coord
// storage.
func LineStringWithStorage(storage arrow.DataType) lineStringOption {
	return func(t *LineStringType) {
		t.Storage = storage
	}
}

// LineStringWithMetadata replaces the GeoArrow metadata on the type.
func LineStringWithMetadata(metadata Metadata) lineStringOption {
	return func(t *LineStringType) {
		t.meta = metadata
	}
}

// lineStringNesting names the List layers in linestring storage from outside
// in.
var lineStringNesting = []string{"vertices"}

func lineStringStorage(coordType arrow.DataType) arrow.DataType {
	return nestedListStorage(coordType, lineStringNesting...)
}

// LineStringWithDimension uses separated struct coord storage for the given
// dim.
func LineStringWithDimension(dim Dimension) lineStringOption {
	return func(t *LineStringType) {
		t.Storage = lineStringStorage(coordStructStorage(dim))
	}
}

// LineStringWithInterleaved uses interleaved FSL coord storage for the given
// dim.
func LineStringWithInterleaved(dim Dimension) lineStringOption {
	return func(t *LineStringType) {
		t.Storage = lineStringStorage(interleavedStorage(dim))
	}
}

// NewLineStringType constructs a LineStringType with the given options. The
// default storage is separated XY Struct coords under "vertices".
func NewLineStringType(opts ...lineStringOption) *LineStringType {
	t := &LineStringType{
		ExtensionBase: arrow.ExtensionBase{Storage: lineStringStorage(coordStructStorage(XY))},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

func (*LineStringType) ExtensionName() string {
	return ExtensionNameLineString
}

func (*LineStringType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}
	coordType, err := unwrapNestedLists(storageType, len(lineStringNesting))
	if err != nil {
		return nil, fmt.Errorf("geoarrow.linestring: %w", err)
	}
	if _, err := checkCoordStorage(coordType); err != nil {
		return nil, fmt.Errorf("geoarrow.linestring: %w", err)
	}
	return NewLineStringType(LineStringWithStorage(storageType), LineStringWithMetadata(meta)), nil
}

func (t *LineStringType) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*LineStringType)
	if !ok {
		return false
	}
	if t == nil && o == nil {
		return true
	}
	if t == nil || o == nil {
		return false
	}
	return sameNestedStorage(t.Storage, o.Storage, len(lineStringNesting)) && t.Equal(&o.Extension)
}

func (*LineStringType) ArrayType() reflect.Type {
	return reflect.TypeFor[LineStringArray]()
}

// dim returns the coordinate dimension of the type's storage. Only safe after
// construction / Deserialize has validated the storage shape.
func (t *LineStringType) dim() Dimension {
	coord, _ := unwrapNestedLists(t.StorageType(), len(lineStringNesting))
	return DimensionFromStorage(coord)
}

func (t *LineStringType) valueFromArray(a array.ExtensionArray, i int) LineStringValue {
	dim := t.dim()
	if a.IsNull(i) {
		return LineStringValue{dim: dim}
	}
	return LineStringValue{coords: readCoordList(a, i, dim.NDim()), dim: dim}
}

func (t *LineStringType) appendValueToBuilder(b array.Builder, v LineStringValue) {
	dim := t.dim()
	checkAppendDim("geoarrow.linestring", v.dim, dim, v.IsEmpty())
	appendCoordList(b, v.coords, dim.NDim())
}

func (t *LineStringType) valueFromString(s string) (LineStringValue, error) {
	storageDim := t.dim()
	prefix, body, isEmpty, err := wktSplit(s, "LINESTRING")
	if err != nil {
		return LineStringValue{}, err
	}
	if isEmpty {
		return LineStringValue{dim: storageDim}, nil
	}
	coords, stride, err := parseWKTFlatCoords(body)
	if err != nil {
		return LineStringValue{}, fmt.Errorf("linestring WKT: %w", err)
	}
	dim, err := wktDimension(prefix, stride)
	if err != nil {
		return LineStringValue{}, fmt.Errorf("linestring WKT: %w", err)
	}
	if err := checkStorageDim("linestring", dim, storageDim); err != nil {
		return LineStringValue{}, err
	}
	return LineStringValue{coords: coords, dim: dim}, nil
}

func (t *LineStringType) unmarshalJSONOne(dec *json.Decoder) (LineStringValue, bool, error) {
	isNull, err := decodeOpenOrNull(dec)
	if err != nil {
		return LineStringValue{}, false, err
	}
	if isNull {
		return LineStringValue{}, true, nil
	}
	coords, err := decodeCoordList(dec)
	if err != nil {
		return LineStringValue{}, false, err
	}
	return LineStringValue{coords: coords, dim: t.dim()}, false, nil
}

func (t *LineStringType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[LineStringValue, *LineStringType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, t),
	}
}

// LineStringArray is the Arrow ExtensionArray for geoarrow.linestring.
type LineStringArray = geometryArray[LineStringValue, *LineStringType]

// LineStringBuilder is the Arrow Builder for geoarrow.linestring.
type LineStringBuilder = valueBuilder[LineStringValue, *LineStringType]

var _ array.CustomExtensionBuilder = (*LineStringType)(nil)
