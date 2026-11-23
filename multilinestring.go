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

// MultiLineStringValue represents a multilinestring geometry. Each linestring
// is stored as interleaved flat coordinates. The returned slices alias the
// value's storage and must not be modified.
type MultiLineStringValue struct {
	lines [][]float64
	dim   Dimension
}

// NewMultiLineStringValue returns a multilinestring of the given dimension
// whose linestrings are flat interleaved coord slices (each of length
// n_vertices * dim.NDim()).
func NewMultiLineStringValue(dim Dimension, lines [][]float64) MultiLineStringValue {
	return MultiLineStringValue{lines: lines, dim: dim}
}

// NumLineStrings returns the number of linestrings in v.
func (v MultiLineStringValue) NumLineStrings() int {
	return len(v.lines)
}

// LineString returns linestring i.
func (v MultiLineStringValue) LineString(i int) LineStringValue {
	return LineStringValue{coords: v.lines[i], dim: v.dim}
}

// Dimension returns the coordinate dimension of v.
func (v MultiLineStringValue) Dimension() Dimension {
	return v.dim
}

// GeometryType returns the GeoArrow GeometryTypeID for v.
func (v MultiLineStringValue) GeometryType() GeometryTypeID {
	return geometryTypeID(MultiLineStringID, v.dim)
}

// IsEmpty reports whether v has no linestrings.
func (v MultiLineStringValue) IsEmpty() bool {
	return len(v.lines) == 0
}

// String renders v as WKT (e.g. "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))").
func (v MultiLineStringValue) String() string {
	if v.IsEmpty() {
		return "MULTILINESTRING EMPTY"
	}
	var b strings.Builder
	b.WriteString("MULTILINESTRING")
	b.WriteString(dimSuffix(v.dim))
	b.WriteByte('(')
	stride := v.dim.NDim()
	for i, line := range v.lines {
		if i > 0 {
			b.WriteString(", ")
		}
		formatCoordRingOrEmpty(&b, line, stride)
	}
	b.WriteByte(')')
	return b.String()
}

// MarshalJSON encodes v as a JSON array of linestrings, each a JSON array of
// coordinate arrays.
func (v MultiLineStringValue) MarshalJSON() ([]byte, error) {
	stride := v.dim.NDim()
	lines := make([][][]float64, len(v.lines))
	for i, line := range v.lines {
		lines[i] = vertices(line, stride)
	}
	return json.Marshal(lines)
}

// MultiLineStringType is the GeoArrow extension type for MultiLineString
// geometries (geoarrow.multilinestring). Storage is
// List<linestrings: List<vertices: Coord>>.
type MultiLineStringType struct {
	arrow.ExtensionBase
	Extension
}

type multiLineStringOption func(*MultiLineStringType)

// MultiLineStringWithStorage overrides the Arrow storage type. Use when
// constructing a MultiLineStringType with non-default field names or a custom
// coord storage.
func MultiLineStringWithStorage(storage arrow.DataType) multiLineStringOption {
	return func(t *MultiLineStringType) {
		t.Storage = storage
	}
}

// MultiLineStringWithMetadata replaces the GeoArrow metadata on the type.
func MultiLineStringWithMetadata(metadata Metadata) multiLineStringOption {
	return func(t *MultiLineStringType) {
		t.meta = metadata
	}
}

// multiLineStringNesting names the List layers in multilinestring storage
// from outside in.
var multiLineStringNesting = []string{"linestrings", "vertices"}

func multiLineStringStorage(coordType arrow.DataType) arrow.DataType {
	return nestedListStorage(coordType, multiLineStringNesting...)
}

// MultiLineStringWithDimension uses separated struct coord storage for the
// given dim.
func MultiLineStringWithDimension(dim Dimension) multiLineStringOption {
	return func(t *MultiLineStringType) {
		t.Storage = multiLineStringStorage(coordStructStorage(dim))
	}
}

// MultiLineStringWithInterleaved uses interleaved FSL coord storage for the
// given dim.
func MultiLineStringWithInterleaved(dim Dimension) multiLineStringOption {
	return func(t *MultiLineStringType) {
		t.Storage = multiLineStringStorage(interleavedStorage(dim))
	}
}

// NewMultiLineStringType constructs a MultiLineStringType with the given
// options. The default storage is separated XY Struct coords nested under
// "linestrings" / "vertices".
func NewMultiLineStringType(opts ...multiLineStringOption) *MultiLineStringType {
	t := &MultiLineStringType{
		ExtensionBase: arrow.ExtensionBase{Storage: multiLineStringStorage(coordStructStorage(XY))},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

func (*MultiLineStringType) ExtensionName() string {
	return ExtensionNameMultiLineString
}

func (*MultiLineStringType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}
	coordType, err := unwrapNestedLists(storageType, len(multiLineStringNesting))
	if err != nil {
		return nil, fmt.Errorf("geoarrow.multilinestring: %w", err)
	}
	if _, err := checkCoordStorage(coordType); err != nil {
		return nil, fmt.Errorf("geoarrow.multilinestring: %w", err)
	}
	return NewMultiLineStringType(MultiLineStringWithStorage(storageType), MultiLineStringWithMetadata(meta)), nil
}

func (t *MultiLineStringType) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*MultiLineStringType)
	if !ok {
		return false
	}
	if t == nil && o == nil {
		return true
	}
	if t == nil || o == nil {
		return false
	}
	return sameNestedStorage(t.Storage, o.Storage, len(multiLineStringNesting)) && t.Equal(&o.Extension)
}

func (*MultiLineStringType) ArrayType() reflect.Type {
	return reflect.TypeFor[MultiLineStringArray]()
}

// dim returns the coordinate dimension of the type's storage. Only safe after
// construction / Deserialize has validated the storage shape.
func (t *MultiLineStringType) dim() Dimension {
	coord, _ := unwrapNestedLists(t.StorageType(), len(multiLineStringNesting))
	return DimensionFromStorage(coord)
}

func (t *MultiLineStringType) valueFromArray(a array.ExtensionArray, i int) MultiLineStringValue {
	dim := t.dim()
	if a.IsNull(i) {
		return MultiLineStringValue{dim: dim}
	}
	return MultiLineStringValue{lines: readCoordListList(a, i, dim.NDim()), dim: dim}
}

func (t *MultiLineStringType) appendValueToBuilder(b array.Builder, v MultiLineStringValue) {
	dim := t.dim()
	checkAppendDim("geoarrow.multilinestring", v.dim, dim, v.IsEmpty())
	appendCoordListList(b, v.lines, dim.NDim())
}

func (t *MultiLineStringType) valueFromString(s string) (MultiLineStringValue, error) {
	storageDim := t.dim()
	prefix, body, isEmpty, err := wktSplit(s, "MULTILINESTRING")
	if err != nil {
		return MultiLineStringValue{}, err
	}
	if isEmpty {
		return MultiLineStringValue{dim: storageDim}, nil
	}
	lines, stride, err := parseWKTCoordItems(body)
	if err != nil {
		return MultiLineStringValue{}, fmt.Errorf("multilinestring WKT: %w", err)
	}
	dim, err := wktDimension(prefix, stride)
	if err != nil {
		return MultiLineStringValue{}, fmt.Errorf("multilinestring WKT: %w", err)
	}
	if err := checkStorageDim("multilinestring", dim, storageDim); err != nil {
		return MultiLineStringValue{}, err
	}
	return MultiLineStringValue{lines: lines, dim: dim}, nil
}

func (t *MultiLineStringType) unmarshalJSONOne(dec *json.Decoder) (MultiLineStringValue, bool, error) {
	isNull, err := decodeOpenOrNull(dec)
	if err != nil {
		return MultiLineStringValue{}, false, err
	}
	if isNull {
		return MultiLineStringValue{}, true, nil
	}
	lines, err := decodeCoordListList(dec)
	if err != nil {
		return MultiLineStringValue{}, false, err
	}
	return MultiLineStringValue{lines: lines, dim: t.dim()}, false, nil
}

func (t *MultiLineStringType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[MultiLineStringValue, *MultiLineStringType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, t),
	}
}

// MultiLineStringArray is the Arrow ExtensionArray for
// geoarrow.multilinestring.
type MultiLineStringArray = geometryArray[MultiLineStringValue, *MultiLineStringType]

// MultiLineStringBuilder is the Arrow Builder for geoarrow.multilinestring.
type MultiLineStringBuilder = valueBuilder[MultiLineStringValue, *MultiLineStringType]

var _ array.CustomExtensionBuilder = (*MultiLineStringType)(nil)
