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

// MultiPolygonValue represents a multipolygon geometry: zero or more polygons,
// each with an exterior ring followed by hole rings. Each ring is stored as
// interleaved flat coordinates. The returned slices alias the value's storage
// and must not be modified.
type MultiPolygonValue struct {
	polygons [][][]float64
	dim      Dimension
}

// NewMultiPolygonValue returns a multipolygon of the given dimension.
// polygons[i][j] is ring j of polygon i as flat interleaved coords (length
// n_vertices * dim.NDim()); ring 0 is the exterior boundary.
func NewMultiPolygonValue(dim Dimension, polygons [][][]float64) MultiPolygonValue {
	return MultiPolygonValue{polygons: polygons, dim: dim}
}

// NumPolygons returns the number of polygons in v.
func (v MultiPolygonValue) NumPolygons() int {
	return len(v.polygons)
}

// Polygon returns polygon i.
func (v MultiPolygonValue) Polygon(i int) PolygonValue {
	return PolygonValue{rings: v.polygons[i], dim: v.dim}
}

// Dimension returns the coordinate dimension of v.
func (v MultiPolygonValue) Dimension() Dimension {
	return v.dim
}

// GeometryType returns the GeoArrow GeometryTypeID for v.
func (v MultiPolygonValue) GeometryType() GeometryTypeID {
	return geometryTypeID(MultiPolygonID, v.dim)
}

// IsEmpty reports whether v has no polygons.
func (v MultiPolygonValue) IsEmpty() bool {
	return len(v.polygons) == 0
}

// String renders v as WKT (e.g. "MULTIPOLYGON(((0 0, 1 0, 0 1, 0 0)))").
func (v MultiPolygonValue) String() string {
	if v.IsEmpty() {
		return "MULTIPOLYGON EMPTY"
	}
	var b strings.Builder
	b.WriteString("MULTIPOLYGON")
	b.WriteString(dimSuffix(v.dim))
	b.WriteByte('(')
	stride := v.dim.NDim()
	for i, rings := range v.polygons {
		if i > 0 {
			b.WriteString(", ")
		}
		if len(rings) == 0 {
			b.WriteString("EMPTY")
			continue
		}
		b.WriteByte('(')
		for j, ring := range rings {
			if j > 0 {
				b.WriteString(", ")
			}
			formatCoordRingOrEmpty(&b, ring, stride)
		}
		b.WriteByte(')')
	}
	b.WriteByte(')')
	return b.String()
}

// MarshalJSON encodes v as a JSON array of polygons, each a JSON array of
// rings, each a JSON array of coordinate arrays.
func (v MultiPolygonValue) MarshalJSON() ([]byte, error) {
	stride := v.dim.NDim()
	polys := make([][][][]float64, len(v.polygons))
	for i, rings := range v.polygons {
		out := make([][][]float64, len(rings))
		for j, ring := range rings {
			out[j] = vertices(ring, stride)
		}
		polys[i] = out
	}
	return json.Marshal(polys)
}

// MultiPolygonType is the GeoArrow extension type for MultiPolygon geometries
// (geoarrow.multipolygon). Storage is
// List<polygons: List<rings: List<vertices: Coord>>>.
type MultiPolygonType struct {
	arrow.ExtensionBase
	Extension
}

type multiPolygonOption func(*MultiPolygonType)

// MultiPolygonWithStorage overrides the Arrow storage type. Use when
// constructing a MultiPolygonType with non-default field names or a custom
// coord storage.
func MultiPolygonWithStorage(storage arrow.DataType) multiPolygonOption {
	return func(t *MultiPolygonType) {
		t.Storage = storage
	}
}

// MultiPolygonWithMetadata replaces the GeoArrow metadata on the type.
func MultiPolygonWithMetadata(metadata Metadata) multiPolygonOption {
	return func(t *MultiPolygonType) {
		t.meta = metadata
	}
}

// multiPolygonNesting names the List layers in multipolygon storage from
// outside in.
var multiPolygonNesting = []string{"polygons", "rings", "vertices"}

func multiPolygonStorage(coordType arrow.DataType) arrow.DataType {
	return nestedListStorage(coordType, multiPolygonNesting...)
}

// MultiPolygonWithDimension uses separated struct coord storage for the given
// dim.
func MultiPolygonWithDimension(dim Dimension) multiPolygonOption {
	return func(t *MultiPolygonType) {
		t.Storage = multiPolygonStorage(coordStructStorage(dim))
	}
}

// MultiPolygonWithInterleaved uses interleaved FSL coord storage for the given
// dim.
func MultiPolygonWithInterleaved(dim Dimension) multiPolygonOption {
	return func(t *MultiPolygonType) {
		t.Storage = multiPolygonStorage(interleavedStorage(dim))
	}
}

// NewMultiPolygonType constructs a MultiPolygonType with the given options.
// The default storage is separated XY Struct coords nested under "polygons" /
// "rings" / "vertices".
func NewMultiPolygonType(opts ...multiPolygonOption) *MultiPolygonType {
	t := &MultiPolygonType{
		ExtensionBase: arrow.ExtensionBase{Storage: multiPolygonStorage(coordStructStorage(XY))},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

func (*MultiPolygonType) ExtensionName() string {
	return ExtensionNameMultiPolygon
}

func (*MultiPolygonType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}
	coordType, err := unwrapNestedLists(storageType, len(multiPolygonNesting))
	if err != nil {
		return nil, fmt.Errorf("geoarrow.multipolygon: %w", err)
	}
	if _, err := checkCoordStorage(coordType); err != nil {
		return nil, fmt.Errorf("geoarrow.multipolygon: %w", err)
	}
	return NewMultiPolygonType(MultiPolygonWithStorage(storageType), MultiPolygonWithMetadata(meta)), nil
}

func (t *MultiPolygonType) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*MultiPolygonType)
	if !ok {
		return false
	}
	if t == nil && o == nil {
		return true
	}
	if t == nil || o == nil {
		return false
	}
	return sameNestedStorage(t.Storage, o.Storage, len(multiPolygonNesting)) && t.Equal(&o.Extension)
}

func (*MultiPolygonType) ArrayType() reflect.Type {
	return reflect.TypeFor[MultiPolygonArray]()
}

// dim returns the coordinate dimension of the type's storage. Only safe after
// construction / Deserialize has validated the storage shape.
func (t *MultiPolygonType) dim() Dimension {
	coord, _ := unwrapNestedLists(t.StorageType(), len(multiPolygonNesting))
	return DimensionFromStorage(coord)
}

func (t *MultiPolygonType) valueFromArray(a array.ExtensionArray, i int) MultiPolygonValue {
	dim := t.dim()
	if a.IsNull(i) {
		return MultiPolygonValue{dim: dim}
	}
	return MultiPolygonValue{polygons: readCoordListListList(a, i, dim.NDim()), dim: dim}
}

func (t *MultiPolygonType) appendValueToBuilder(b array.Builder, v MultiPolygonValue) {
	dim := t.dim()
	checkAppendDim("geoarrow.multipolygon", v.dim, dim, v.IsEmpty())
	appendCoordListListList(b, v.polygons, dim.NDim())
}

func (t *MultiPolygonType) valueFromString(s string) (MultiPolygonValue, error) {
	storageDim := t.dim()
	prefix, body, isEmpty, err := wktSplit(s, "MULTIPOLYGON")
	if err != nil {
		return MultiPolygonValue{}, err
	}
	if isEmpty {
		return MultiPolygonValue{dim: storageDim}, nil
	}

	polyItems, err := splitTopLevelItems(body)
	if err != nil {
		return MultiPolygonValue{}, fmt.Errorf("multipolygon WKT: %w", err)
	}
	polygons := make([][][]float64, 0, len(polyItems))
	stride := 0
	for _, item := range polyItems {
		inner, polyEmpty, err := wktUnwrap(item)
		if err != nil {
			return MultiPolygonValue{}, fmt.Errorf("multipolygon WKT: %w", err)
		}
		if polyEmpty {
			polygons = append(polygons, nil)
			continue
		}
		rings, s, err := parseWKTCoordItems(inner)
		if err != nil {
			return MultiPolygonValue{}, fmt.Errorf("multipolygon WKT: %w", err)
		}
		if stride, err = mergeStride(stride, s); err != nil {
			return MultiPolygonValue{}, fmt.Errorf("multipolygon WKT: %w", err)
		}
		polygons = append(polygons, rings)
	}

	dim, err := wktDimension(prefix, stride)
	if err != nil {
		return MultiPolygonValue{}, fmt.Errorf("multipolygon WKT: %w", err)
	}
	if err := checkStorageDim("multipolygon", dim, storageDim); err != nil {
		return MultiPolygonValue{}, err
	}
	return MultiPolygonValue{polygons: polygons, dim: dim}, nil
}

func (t *MultiPolygonType) unmarshalJSONOne(dec *json.Decoder) (MultiPolygonValue, bool, error) {
	isNull, err := decodeOpenOrNull(dec)
	if err != nil {
		return MultiPolygonValue{}, false, err
	}
	if isNull {
		return MultiPolygonValue{}, true, nil
	}
	polygons, err := decodeCoordListListList(dec)
	if err != nil {
		return MultiPolygonValue{}, false, err
	}
	return MultiPolygonValue{polygons: polygons, dim: t.dim()}, false, nil
}

func (t *MultiPolygonType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[MultiPolygonValue, *MultiPolygonType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, t),
	}
}

// MultiPolygonArray is the Arrow ExtensionArray for geoarrow.multipolygon.
type MultiPolygonArray = geometryArray[MultiPolygonValue, *MultiPolygonType]

// MultiPolygonBuilder is the Arrow Builder for geoarrow.multipolygon.
type MultiPolygonBuilder = valueBuilder[MultiPolygonValue, *MultiPolygonType]

var _ array.CustomExtensionBuilder = (*MultiPolygonType)(nil)
