package geoarrow

import (
	"encoding/hex"
	"fmt"
	"reflect"

	json "github.com/goccy/go-json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

type WKBType struct {
	arrow.ExtensionBase
	Extension
}

type WKBBytes []byte

// String returns a hexadecimal string representation of the WKB bytes
func (b WKBBytes) String() string {
	return fmt.Sprintf("%x", []byte(b))
}

func (b WKBBytes) IsEmpty() bool {
	return len(b) == 0
}

func (b WKBBytes) isoType() uint32 {
	// decode the endian byte and geometry type from the WKB to determine the coordinate dimension
	if len(b) < 5 {
		return 0 // invalid WKB, not enough bytes for header
	}
	endian := b[0]
	if endian == 0 { // big endian
		return uint32(b[1])<<24 | uint32(b[2])<<16 | uint32(b[3])<<8 | uint32(b[4])
	}
	return uint32(b[4])<<24 | uint32(b[3])<<16 | uint32(b[2])<<8 | uint32(b[1])
}

func (b WKBBytes) Dimension() Dimension {
	t := b.isoType()
	switch {
	case t >= 3001 && t <= 3006:
		return XYZM
	case t >= 2001 && t <= 2006:
		return XYM
	case t >= 1001 && t <= 1006:
		return XYZ
	case t >= 1 && t <= 6:
		return XY
	default:
		return 0
	}
}

func (b WKBBytes) GeometryType() GeometryTypeID {
	t := b.isoType()
	if t == 0 {
		return 0
	}
	// ISO WKB encodes the geometry type as a combination of the base type
	// (1-6) and dimension (offset by 1000 for Z, 2000 for M, 3000 for ZM)
	base := t % 1000
	dimOffset := (t / 1000) * 10
	if base < 1 || base > 6 {
		return 0
	}
	return GeometryTypeID(base + dimOffset)
}

func (b WKBBytes) MarshalJSON() ([]byte, error) {
	if b == nil {
		return json.Marshal(nil)
	}
	return json.Marshal(b.String())
}

type wkbOption func(*WKBType)

// WKBWithLargeBinaryStorage configures the WKBType to use LargeBinary storage
func WKBWithLargeBinaryStorage() wkbOption {
	return func(wkb *WKBType) {
		wkb.Storage = &arrow.LargeBinaryType{}
	}
}

// WKBWithBinaryStorage configures the WKBType to use Binary storage (default is LargeBinary)
func WKBWithBinaryStorage() wkbOption {
	return func(wkb *WKBType) {
		wkb.Storage = &arrow.BinaryType{}
	}
}

// WKBWithMetadata configures the WKBType with custom metadata
func WKBWithMetadata(metadata Metadata) wkbOption {
	return func(wkb *WKBType) {
		wkb.meta = metadata
	}
}

func NewWKBType(opts ...wkbOption) *WKBType {
	wkb := &WKBType{
		ExtensionBase: arrow.ExtensionBase{Storage: &arrow.LargeBinaryType{}},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(wkb)
		}
	}
	return wkb
}

func (*WKBType) ExtensionName() string {
	return ExtensionNameWKB
}

// ParquetLogicalType lets a downstream Parquet writer that looks for this interface method
// write WKB as the naive Parquet GEOMETRY or GEOGRAPHY logical type, depending on
// GeoArrow metadata.
func (wkb *WKBType) ParquetLogicalType() schema.LogicalType {
	if wkb.meta.Edges != "" {
		return schema.GeographyLogicalType{
			Algorithm: schema.GeographyEdgeInterpolationAlgorithm(wkb.meta.Edges),
			Crs:       wkb.meta.ParquetCRS(),
		}
	}
	return schema.GeometryLogicalType{Crs: wkb.meta.ParquetCRS()}
}

// ArrowTypeFromParquet maps Parquet GEOMETRY and GEOGRAPHY logical byte-array
// columns back to the GeoArrow WKB extension type, allowing read support for the extension
func (*WKBType) ArrowTypeFromParquet(logical schema.LogicalType, storageType arrow.DataType) (arrow.ExtensionType, error) {
	if storageType == nil {
		return nil, nil
	}

	// WKB is represented as variable-width bytes. Other Parquet physical
	// storage cannot safely be interpreted as this extension type.
	var storageOpt wkbOption
	switch storageType.ID() {
	case arrow.BINARY:
		storageOpt = WKBWithBinaryStorage()
	case arrow.LARGE_BINARY:
		storageOpt = WKBWithLargeBinaryStorage()
	default:
		return nil, nil
	}

	// Preserve CRS and edge interpolation metadata encoded in the Parquet
	// logical type so schema conversion is reversible where possible.
	meta := NewMetadata()
	switch logical := logical.(type) {
	case schema.GeometryLogicalType:
		if logical.IsCRSSet() {
			meta.SetCRSString(logical.CRS())
		}
	case schema.GeographyLogicalType:
		if logical.IsCRSSet() {
			meta.SetCRSString(logical.CRS())
		}
		edges, ok := edgeInterpolationFromParquet(logical.EdgeInterpolationAlgorithm())
		if !ok {
			return nil, fmt.Errorf("unsupported Parquet geography edge interpolation algorithm: %s", logical.EdgeInterpolationAlgorithm())
		}
		meta.Edges = edges
	default:
		return nil, nil
	}

	// Return a new type instead of the registered receiver so the storage type
	// and metadata match the Parquet column being converted.
	return NewWKBType(storageOpt, WKBWithMetadata(meta)), nil
}

// edgeInterpolationFromParquet maps Parquet geography edge interpolation values
// to GeoArrow metadata values. This is semantically the same but is mapped
// explicitly to ensure there is no implicit usage of unknown/typo values and things
// are kept in sync between geoarrow-go and arrow-go
func edgeInterpolationFromParquet(alg schema.GeographyEdgeInterpolationAlgorithm) (EdgeInterpolation, bool) {
	switch alg {
	case schema.GeographyEdgeSpherical:
		return EdgeSpherical, true
	case schema.GeographyEdgeVincenty:
		return EdgeVincenty, true
	case schema.GeographyEdgeThomas:
		return EdgeThomas, true
	case schema.GeographyEdgeAndoyer:
		return EdgeAndoyer, true
	case schema.GeographyEdgeKarney:
		return EdgeKarney, true
	default:
		return "", false
	}
}

func (*WKBType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}

	switch storageType.ID() {
	case arrow.BINARY:
		return NewWKBType(WKBWithBinaryStorage(), WKBWithMetadata(meta)), nil
	case arrow.LARGE_BINARY:
		return NewWKBType(WKBWithLargeBinaryStorage(), WKBWithMetadata(meta)), nil
	}

	return nil, fmt.Errorf("unsupported storage type for WKB extension: %s", storageType)
}

func (wkb *WKBType) ExtensionEquals(other arrow.ExtensionType) bool {
	otherWKB, ok := other.(*WKBType)
	if !ok {
		return false
	}
	if wkb == nil && otherWKB == nil {
		return true
	}
	if wkb == nil || otherWKB == nil {
		return false
	}
	return otherWKB.StorageType().ID() == wkb.StorageType().ID() &&
		otherWKB.Equal(&wkb.Extension)
}

func (*WKBType) ArrayType() reflect.Type {
	return reflect.TypeOf(WKBArray{})
}

func (*WKBType) valueFromArray(a array.ExtensionArray, i int) WKBBytes {
	if a.IsNull(i) {
		return nil
	}
	switch arr := a.Storage().(type) {
	case *array.Binary:
		return WKBBytes(arr.Value(i))
	case *array.LargeBinary:
		return WKBBytes(arr.Value(i))
	default:
		return nil
	}
}

func (wkb *WKBType) appendValueToBuilder(b array.Builder, v WKBBytes) {
	b.(*array.BinaryBuilder).Append([]byte(v))
}

func (wkb *WKBType) valueFromString(s string) (WKBBytes, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return WKBBytes(b), nil
}

func (wkb *WKBType) unmarshalJSONOne(dec *json.Decoder) (WKBBytes, bool, error) {
	t, err := dec.Token()
	if err != nil {
		return nil, false, err
	}

	if t == nil {
		return nil, true, nil
	}

	s, ok := t.(string)
	if !ok {
		return nil, false, fmt.Errorf("expected string for WKB value, got %T", t)
	}

	v, err := wkb.valueFromString(s)
	if err != nil {
		return nil, false, err
	}
	return v, false, nil
}

func (wkb *WKBType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[WKBBytes, *WKBType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, wkb),
	}
}

// WKBArray is a type alias to represent an array of WKB encoded geometries.
type WKBArray = geometryArray[WKBBytes, *WKBType]
type WKBBuilder = valueBuilder[WKBBytes, *WKBType]

var _ arrow.ExtensionType = (*WKBType)(nil)
var _ array.ExtensionArray = (*WKBArray)(nil)
var _ array.CustomExtensionBuilder = (*WKBType)(nil)

// ExtensionCustomParquetType is the interface used by writers to specify when which
// logical type in Parquet to use to represent the extension type.
var _ pqarrow.ExtensionCustomParquetType = (*WKBType)(nil)

// ExtensionCustomArrowReadType is the interface used by readers to specify how to
// convert an associated Parquet LogicalType back to an Arrow ExtensionType.
var _ pqarrow.ExtensionCustomArrowReadType = (*WKBType)(nil)
