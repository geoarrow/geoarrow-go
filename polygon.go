package geoarrow

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// PolygonValue represents a polygon geometry with zero or more rings.
// The first ring is the exterior boundary; subsequent rings are holes.
// Each ring is stored as interleaved flat coordinates [x1,y1,x2,y2,...].
type PolygonValue struct {
	rings [][]float64
	dim   Dimension
}

func NewPolygonValue(dim Dimension, rings [][]float64) PolygonValue {
	return PolygonValue{rings: rings, dim: dim}
}

func (v PolygonValue) NumRings() int {
	return len(v.rings)
}

// Ring returns the flat interleaved coordinates for ring i.
func (v PolygonValue) Ring(i int) []float64 {
	return v.rings[i]
}

// NumVertices returns the number of vertices in ring i.
func (v PolygonValue) NumVertices(i int) int {
	return len(v.rings[i]) / v.dim.NDim()
}

func (v PolygonValue) Dimension() Dimension {
	return v.dim
}

func (v PolygonValue) GeometryType() GeometryTypeID {
	switch v.dim {
	case XY:
		return PolygonID
	case XYZ:
		return PolygonZID
	case XYM:
		return PolygonMID
	case XYZM:
		return PolygonZMID
	default:
		panic("invalid coordinate dimension for PolygonValue")
	}
}

func (v PolygonValue) IsEmpty() bool {
	return len(v.rings) == 0
}

func (v PolygonValue) String() string {
	if v.IsEmpty() {
		return "POLYGON EMPTY"
	}

	b := strings.Builder{}
	b.WriteString("POLYGON")
	b.WriteString(dimSuffix(v.dim))
	b.WriteString("(")

	stride := v.dim.NDim()
	for i, ring := range v.rings {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		nVerts := len(ring) / stride
		for vi := range nVerts {
			if vi > 0 {
				b.WriteString(", ")
			}
			for d := range stride {
				if d > 0 {
					b.WriteString(" ")
				}
				b.WriteString(strconv.FormatFloat(ring[vi*stride+d], 'f', 6, 64))
			}
		}
		b.WriteString(")")
	}
	b.WriteString(")")
	return b.String()
}

func (v PolygonValue) MarshalJSON() ([]byte, error) {
	stride := v.dim.NDim()
	rings := make([][][]float64, len(v.rings))
	for i, ring := range v.rings {
		nVerts := len(ring) / stride
		verts := make([][]float64, nVerts)
		for vi := range nVerts {
			verts[vi] = ring[vi*stride : (vi+1)*stride]
		}
		rings[i] = verts
	}
	return json.Marshal(rings)
}

// PolygonType is the extension type for Polygon geometries.
// Storage: List<List<Struct<x: double, y: double, [z: double, [m: double]]>>>
type PolygonType struct {
	arrow.ExtensionBase
	Extension
}

type polygonOption func(*PolygonType)

func PolygonWithStorage(storage arrow.DataType) polygonOption {
	return func(pt *PolygonType) {
		pt.Storage = storage
	}
}

func PolygonWithMetadata(metadata Metadata) polygonOption {
	return func(pt *PolygonType) {
		pt.meta = metadata
	}
}

// polygonStorage builds the nested storage type List<rings: List<vertices: Coord>>
// for a polygon, given a coord storage type (struct or FSL).
func polygonStorage(coordType arrow.DataType) arrow.DataType {
	return listOfNamed("rings", listOfNamed("vertices", coordType))
}

func defaultPolygonStorage() arrow.DataType {
	return polygonStorage(coordStructStorage(XY))
}

// PolygonWithDimension uses separated struct coord storage for the given dim.
func PolygonWithDimension(dim Dimension) polygonOption {
	return func(pt *PolygonType) {
		pt.Storage = polygonStorage(coordStructStorage(dim))
	}
}

// PolygonWithInterleaved uses interleaved FSL coord storage for the given dim.
func PolygonWithInterleaved(dim Dimension) polygonOption {
	return func(pt *PolygonType) {
		pt.Storage = polygonStorage(interleavedStorage(dim))
	}
}

func NewPolygonType(opts ...polygonOption) *PolygonType {
	pt := &PolygonType{
		ExtensionBase: arrow.ExtensionBase{Storage: defaultPolygonStorage()},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(pt)
		}
	}
	return pt
}

func (*PolygonType) ExtensionName() string {
	return ExtensionNamePolygon
}

func (*PolygonType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}
	if _, err := checkCoordStorage(polygonCoordStorage(storageType)); err != nil {
		return nil, fmt.Errorf("geoarrow.polygon: %w", err)
	}
	return NewPolygonType(PolygonWithStorage(storageType), PolygonWithMetadata(meta)), nil
}

func (pt *PolygonType) ExtensionEquals(other arrow.ExtensionType) bool {
	otherPt, ok := other.(*PolygonType)
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

func (*PolygonType) ArrayType() reflect.Type {
	return reflect.TypeFor[PolygonArray]()
}

// polygonCoordStorage extracts the coord storage type (struct or FSL) from
// the nested List<List<Coord>> polygon storage type.
func polygonCoordStorage(storage arrow.DataType) arrow.DataType {
	outerList := storage.(*arrow.ListType)
	innerList := outerList.ElemField().Type.(*arrow.ListType)
	return innerList.ElemField().Type
}

func (pt *PolygonType) valueFromArray(a array.ExtensionArray, i int) PolygonValue {
	if a.IsNull(i) {
		return PolygonValue{}
	}

	outerList := a.Storage().(*array.List)
	ringStart, ringEnd := outerList.ValueOffsets(i)

	innerList := outerList.ListValues().(*array.List)
	coordArr := innerList.ListValues()

	dim := DimensionFromStorage(polygonCoordStorage(pt.StorageType()))
	stride := dim.NDim()

	rings := make([][]float64, ringEnd-ringStart)
	for r := ringStart; r < ringEnd; r++ {
		vertStart, vertEnd := innerList.ValueOffsets(int(r))
		rings[r-ringStart] = readCoordsRange(coordArr, int(vertStart), int(vertEnd), stride)
	}

	return PolygonValue{rings: rings, dim: dim}
}

func (pt *PolygonType) appendValueToBuilder(b array.Builder, v PolygonValue) {
	outerListBuilder := b.(*array.ListBuilder)
	outerListBuilder.Append(true)

	innerListBuilder := outerListBuilder.ValueBuilder().(*array.ListBuilder)
	coordBuilder := innerListBuilder.ValueBuilder()

	stride := v.dim.NDim()
	for _, ring := range v.rings {
		innerListBuilder.Append(true)
		appendCoords(coordBuilder, ring, stride)
	}
}

func (pt *PolygonType) valueFromString(s string) (PolygonValue, error) {
	prefix, body, isEmpty, err := wktSplit(s, "POLYGON")
	if err != nil {
		return PolygonValue{}, err
	}
	if isEmpty {
		return PolygonValue{}, nil
	}

	ringStrs := splitTopLevelGroups(body)
	rings := make([][]float64, 0, len(ringStrs))
	dim := XY
	for i, ringStr := range ringStrs {
		coords, stride, err := parseWKTFlatCoords(ringStr)
		if err != nil {
			return PolygonValue{}, fmt.Errorf("polygon WKT: %w", err)
		}
		if i == 0 {
			dim = dimFromWKTPrefix(prefix, stride)
		}
		rings = append(rings, coords)
	}

	return PolygonValue{rings: rings, dim: dim}, nil
}

func (pt *PolygonType) unmarshalJSONOne(dec *json.Decoder) (PolygonValue, bool, error) {
	isNull, err := decodeOpenOrNull(dec)
	if err != nil {
		return PolygonValue{}, false, err
	}
	if isNull {
		return PolygonValue{}, true, nil
	}

	dim := DimensionFromStorage(polygonCoordStorage(pt.StorageType()))

	var rings [][]float64
	for dec.More() {
		if err := expectDelim(dec, '['); err != nil {
			return PolygonValue{}, false, fmt.Errorf("polygon ring: %w", err)
		}
		ring, err := decodeCoordList(dec)
		if err != nil {
			return PolygonValue{}, false, err
		}
		rings = append(rings, ring)
	}
	if _, err := dec.Token(); err != nil { // outer ']'
		return PolygonValue{}, false, err
	}

	return PolygonValue{rings: rings, dim: dim}, false, nil
}

func (pt *PolygonType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[PolygonValue, *PolygonType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, pt),
	}
}

type PolygonArray = geometryArray[PolygonValue, *PolygonType]
type PolygonBuilder = valueBuilder[PolygonValue, *PolygonType]

var _ array.CustomExtensionBuilder = (*PolygonType)(nil)
