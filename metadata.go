package geoarrow

import (
	"bytes"
	"strconv"
	"strings"

	json "github.com/goccy/go-json"
)

// EdgeInterpolation represents the edge interpolation method for the geometry data
type EdgeInterpolation string

const (
	// EdgePlanar indicates that edges should be interpreted as straight lines in a Cartesian plane
	EdgePlanar EdgeInterpolation = ""
	// EdgeSpherical indicates that edges should be interpreted as great circle arcs on a sphere
	EdgeSpherical EdgeInterpolation = "spherical"
	// Below are intpolations methods defined on the ellipsoid specified in the CRS.
	// EdgeVincenty indicates that edges should be interpreted using Vincenty's formula.
	EdgeVincenty EdgeInterpolation = "vincenty"
	// EdgeThomas indicates that edges should be interpreted using Thomas' formula.
	EdgeThomas EdgeInterpolation = "thomas"
	// EdgeAndoyer indicates that edges should be interpreted using Andoyer's formula.
	EdgeAndoyer EdgeInterpolation = "andoyer"
	// EdgeKarney indicates that edges should be interpreted using Karney's formula.
	EdgeKarney EdgeInterpolation = "karney"
)

// CRSType represents the type of coordinate reference system (CRS) used in the geometry metadata
type CRSType string

const (
	// CRS formatted in PROJJSON format
	// https://proj.org/specifications/projjson.html
	CRSTypePROJJSON CRSType = "projjson"
	// CRS formatted in WKT2:2019 format
	// https://www.ogc.org/publications/standard/wkt-crs/
	CRSTypeWKT22019 CRSType = "wkt2:2019"
	// CRS formatted in AUTHORITY:CODE format, e.g. "EPSG:4326"
	CRSTypeAuthorityCode CRSType = "authority_code"
	// CRS as an opaque identifier
	CRSTypeSRID CRSType = "srid"
)

type Metadata struct {
	// CRS as a PROJJSON object or a string in the format specified by CRSType
	CRS     json.RawMessage   `json:"crs,omitempty"`
	CRSType CRSType           `json:"crs_type,omitempty"`
	Edges   EdgeInterpolation `json:"edges,omitempty"`
}

// NewMetadata creates a new Metadata instance with default values (empty CRS and planar edges)
func NewMetadata() Metadata {
	return Metadata{}
}

const (
	// parquetSRIDPrefix is the Parquet spec prefix for CRS values that are
	// integer identifiers from a catalog, e.g. "srid:4326".
	parquetSRIDPrefix = "srid:"
	// parquetDefaultCRS is the CRS Parquet assumes when none is written.
	parquetDefaultCRS = "OGC:CRS84"
)

// SetCRSString stores a Parquet CRS string in GeoArrow metadata. Parquet CRS
// values are opaque strings; GeoArrow metadata requires non-PROJJSON CRS values
// to be encoded as JSON strings and tagged with a CRS type where one is known.
//
//   - Inline PROJJSON objects are stored as-is with crs_type "projjson".
//   - "srid:<id>" values are stored as "<id>" with crs_type "srid".
//   - Any other string (e.g. "OGC:CRS84" or a "projjson:<key>" reference)
//     is stored as a JSON string with no crs_type.
func (m *Metadata) SetCRSString(crs string) {
	if crs == "" {
		m.CRS = nil
		m.CRSType = ""
		return
	}

	raw := json.RawMessage(crs)
	if json.Valid(raw) && bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) {
		m.CRS = raw
		m.CRSType = CRSTypePROJJSON
		return
	}

	if id, ok := strings.CutPrefix(crs, parquetSRIDPrefix); ok && id != "" {
		m.CRS, _ = json.Marshal(id)
		m.CRSType = CRSTypeSRID
		return
	}

	m.CRS, _ = json.Marshal(crs)
	m.CRSType = ""
}

// ParquetCRS returns the CRS value to write into a Parquet logical type,
// following the Parquet geospatial spec:
//
//   - An empty string is returned for the default OGC:CRS84 so the CRS is
//     omitted from the logical type.
//   - SRIDs are written as "srid:<id>".
//   - Inline PROJJSON and other strings are written as-is.
func (m Metadata) ParquetCRS() string {
	if len(m.CRS) == 0 {
		return ""
	}

	var crs string
	if err := json.Unmarshal(m.CRS, &crs); err != nil {
		// Not a JSON string, e.g. an inline PROJJSON object.
		if isPROJJSONCRS84(m.CRS) {
			return ""
		}
		return string(m.CRS)
	}

	switch {
	case crs == "":
		return ""
	case m.CRSType == CRSTypeSRID:
		if strings.HasPrefix(crs, parquetSRIDPrefix) {
			return crs
		}
		return parquetSRIDPrefix + crs
	case crs == parquetDefaultCRS:
		return ""
	}
	return crs
}

// isPROJJSONCRS84 reports whether a PROJJSON object identifies itself as
// OGC:CRS84 via its "id" member.
func isPROJJSONCRS84(raw json.RawMessage) bool {
	var projjson struct {
		ID *struct {
			Authority string          `json:"authority"`
			Code      json.RawMessage `json:"code"`
		} `json:"id"`
	}
	if err := json.Unmarshal(raw, &projjson); err != nil || projjson.ID == nil {
		return false
	}

	code := string(projjson.ID.Code)
	if unquoted, err := strconv.Unquote(code); err == nil {
		code = unquoted
	}
	return projjson.ID.Authority == "OGC" && code == "CRS84"
}
