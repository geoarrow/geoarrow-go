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

// WKTString is a single well-known text geometry value. It is a thin wrapper
// over the stored text: the type does not validate or normalise the WKT.
// Dimension and GeometryType are read from the text's leading keyword
// ("POINT Z (1 2 3)") and are best effort for malformed text.
type WKTString string

// wktKeywords maps each WKT geometry keyword to its base GeometryTypeID.
// Longer keywords that share a prefix with a shorter one come first.
var wktKeywords = []struct {
	name string
	id   GeometryTypeID
}{
	{"GEOMETRYCOLLECTION", 0}, // has no GeometryTypeID
	{"MULTILINESTRING", MultiLineStringID},
	{"MULTIPOLYGON", MultiPolygonID},
	{"MULTIPOINT", MultiPointID},
	{"LINESTRING", LineStringID},
	{"POLYGON", PolygonID},
	{"POINT", PointID},
}

// header parses the geometry keyword and dimension from the start of the text.
// The dimension comes from an ISO marker ("POINT Z", "POINTZ") when present,
// and otherwise from the number of ordinates in the first vertex. base is 0
// for GEOMETRYCOLLECTION and for unrecognised text.
func (s WKTString) header() (base GeometryTypeID, dim Dimension) {
	text := strings.TrimSpace(string(s))
	open := strings.IndexByte(text, '(')
	head := text
	if open >= 0 {
		head = text[:open]
	}
	fields := strings.Fields(head)
	if len(fields) == 0 {
		return 0, XY
	}

	keyword := strings.ToUpper(fields[0])
	marker := ""
	if len(fields) > 1 {
		marker = strings.ToUpper(fields[1])
	}
	known := false
	for _, k := range wktKeywords {
		if keyword == k.name {
			base, known = k.id, true
			break
		}
		// Fused marker spelling, e.g. "POINTZ" or "LINESTRINGZM".
		if rest, ok := strings.CutPrefix(keyword, k.name); ok && (rest == "Z" || rest == "M" || rest == "ZM") {
			base, known, marker = k.id, true, rest
			break
		}
	}
	if !known {
		return 0, XY
	}

	switch marker {
	case "ZM":
		return base, XYZM
	case "Z":
		return base, XYZ
	case "M":
		return base, XYM
	}
	if open < 0 || base == 0 {
		return base, XY
	}
	// No marker: infer from the first vertex, e.g. "POINT (1 2 3)".
	rest := text[open:]
	if i := strings.IndexAny(rest, "0123456789+-."); i >= 0 {
		rest = rest[i:]
		if j := strings.IndexAny(rest, ",)"); j >= 0 {
			rest = rest[:j]
		}
		switch len(strings.Fields(rest)) {
		case 3:
			return base, XYZ
		case 4:
			return base, XYZM
		}
	}
	return base, XY
}

// String returns the WKT text.
func (s WKTString) String() string {
	return string(s)
}

// IsEmpty reports whether the text spells an empty geometry: a geometry
// keyword, an optional Z/M/ZM marker, then EMPTY ("POINT EMPTY",
// "POLYGON Z EMPTY", "LINESTRINGZM EMPTY"). Blank text is not an empty
// geometry, so the "" that a null slot reads back as is not reported as one;
// use the array's IsNull to detect nulls.
func (s WKTString) IsEmpty() bool {
	fields := strings.Fields(strings.ToUpper(string(s)))
	n := len(fields)
	if n < 2 || n > 3 || fields[n-1] != "EMPTY" {
		return false
	}
	marker := ""
	if n == 3 {
		marker = fields[1]
		if marker != "Z" && marker != "M" && marker != "ZM" {
			return false
		}
	}
	for _, k := range wktKeywords {
		if fields[0] == k.name {
			return true
		}
		// Fused marker spelling, e.g. "POINTZ EMPTY", unless a marker
		// was also given separately.
		if rest, ok := strings.CutPrefix(fields[0], k.name); ok && marker == "" &&
			(rest == "Z" || rest == "M" || rest == "ZM") {
			return true
		}
	}
	return false
}

// Dimension returns the coordinate dimension declared by the text.
func (s WKTString) Dimension() Dimension {
	_, dim := s.header()
	return dim
}

// GeometryType returns the GeometryTypeID named by the text, or 0 for
// GEOMETRYCOLLECTION and unrecognised text.
func (s WKTString) GeometryType() GeometryTypeID {
	base, dim := s.header()
	if base == 0 {
		return 0
	}
	return geometryTypeID(base, dim)
}

// MarshalJSON encodes the WKT text as a JSON string.
func (s WKTString) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s))
}

// WKTType is the GeoArrow extension type for well-known text geometries
// (geoarrow.wkt). Storage is Utf8, LargeUtf8 or Utf8View.
type WKTType struct {
	arrow.ExtensionBase
	Extension
}

type wktOption func(*WKTType)

// WKTWithStringStorage configures the WKTType to use Utf8 storage.
func WKTWithStringStorage() wktOption {
	return func(t *WKTType) {
		t.Storage = arrow.BinaryTypes.String
	}
}

// WKTWithLargeStringStorage configures the WKTType to use LargeUtf8 storage
// (the default, matching WKBType's LargeBinary default).
func WKTWithLargeStringStorage() wktOption {
	return func(t *WKTType) {
		t.Storage = arrow.BinaryTypes.LargeString
	}
}

// WKTWithStringViewStorage configures the WKTType to use Utf8View storage.
func WKTWithStringViewStorage() wktOption {
	return func(t *WKTType) {
		t.Storage = arrow.BinaryTypes.StringView
	}
}

// WKTWithMetadata configures the WKTType with custom metadata.
func WKTWithMetadata(metadata Metadata) wktOption {
	return func(t *WKTType) {
		t.meta = metadata
	}
}

// NewWKTType constructs a WKTType with the given options. The default storage
// is LargeUtf8.
func NewWKTType(opts ...wktOption) *WKTType {
	t := &WKTType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.BinaryTypes.LargeString},
		Extension:     Extension{meta: NewMetadata()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

func (*WKTType) ExtensionName() string {
	return ExtensionNameWKT
}

func (*WKTType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	var meta Metadata
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}

	switch storageType.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return NewWKTType(func(t *WKTType) { t.Storage = storageType }, WKTWithMetadata(meta)), nil
	}
	return nil, fmt.Errorf("unsupported storage type for WKT extension: %s", storageType)
}

func (t *WKTType) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*WKTType)
	if !ok {
		return false
	}
	if t == nil && o == nil {
		return true
	}
	if t == nil || o == nil {
		return false
	}
	return t.Storage.ID() == o.Storage.ID() && t.Equal(&o.Extension)
}

func (*WKTType) ArrayType() reflect.Type {
	return reflect.TypeFor[WKTArray]()
}

// valueFromArray returns "" for a null slot. "" is not an empty geometry
// (IsEmpty is false), so a null is never mistaken for e.g. "POINT EMPTY".
func (*WKTType) valueFromArray(a array.ExtensionArray, i int) WKTString {
	if a.IsNull(i) {
		return ""
	}
	switch arr := a.Storage().(type) {
	case *array.String:
		return WKTString(arr.Value(i))
	case *array.LargeString:
		return WKTString(arr.Value(i))
	case *array.StringView:
		return WKTString(arr.Value(i))
	default:
		return ""
	}
}

func (*WKTType) appendValueToBuilder(b array.Builder, v WKTString) {
	switch bb := b.(type) {
	case *array.StringBuilder:
		bb.Append(string(v))
	case *array.LargeStringBuilder:
		bb.Append(string(v))
	case *array.StringViewBuilder:
		bb.Append(string(v))
	default:
		// Unreachable: the builder type follows the validated storage type.
		panic(fmt.Sprintf("geoarrow.wkt: unsupported storage builder %T", b))
	}
}

// valueFromString stores s as given; WKT is not validated.
func (*WKTType) valueFromString(s string) (WKTString, error) {
	return WKTString(s), nil
}

func (*WKTType) unmarshalJSONOne(dec *json.Decoder) (WKTString, bool, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", false, err
	}
	if tok == nil {
		return "", true, nil
	}
	s, ok := tok.(string)
	if !ok {
		return "", false, fmt.Errorf("expected string for WKT value, got %T", tok)
	}
	return WKTString(s), false, nil
}

func (t *WKTType) NewBuilder(mem memory.Allocator) array.Builder {
	return &valueBuilder[WKTString, *WKTType]{
		ExtensionBuilder: array.NewExtensionBuilder(mem, t),
	}
}

// WKTArray is the Arrow ExtensionArray for geoarrow.wkt.
type WKTArray = geometryArray[WKTString, *WKTType]

// WKTBuilder is the Arrow Builder for geoarrow.wkt.
type WKTBuilder = valueBuilder[WKTString, *WKTType]

var _ array.CustomExtensionBuilder = (*WKTType)(nil)
