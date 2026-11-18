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
	// parquetSRIDPrefix is the Parquet spec prefix for CRS values given as a
	// spatial reference identifier (SRID), e.g. "srid:4326". SRIDs are opaque
	// integer keys whose meaning depends on the producer (such as a database's
	// spatial_ref_sys table), so they are not interpreted as EPSG codes.
	//
	// The prefix is matched case-sensitively, as the Parquet spec writes it and
	// as arrow-rs matches it, so e.g. "SRID:0" is not recognized as an SRID.
	parquetSRIDPrefix = "srid:"
	// parquetDefaultCRS is the CRS Parquet assumes when none is written.
	parquetDefaultCRS = "OGC:CRS84"
	// parquetUnknownSRID is the SRID conventionally used to mean an unknown CRS
	// (as arrow-rs does). The Parquet spec only shows "srid:0" as an example and
	// does not define it as unknown. Parquet cannot express "unknown" by
	// omitting the CRS, since an omitted CRS means parquetDefaultCRS, while an
	// omitted GeoArrow CRS means unknown.
	parquetUnknownSRID = "0"
)

// SetCRSString stores a Parquet CRS string in GeoArrow metadata. Parquet CRS
// values are opaque strings; GeoArrow metadata requires non-PROJJSON CRS values
// to be encoded as JSON strings and tagged with a CRS type where one is known.
//
//   - An empty string (Parquet's unset CRS) means the Parquet default and is
//     stored as "OGC:CRS84", since an omitted GeoArrow CRS means unknown.
//   - "srid:0" is the conventional unknown-CRS value (used by arrow-rs, but not
//     defined as such by the Parquet spec) and leaves the GeoArrow CRS omitted.
//     The prefix is matched case-sensitively, so "SRID:0" is not unknown.
//   - Inline PROJJSON objects are stored as-is with crs_type "projjson".
//   - Any other string, including other "srid:<id>" values (kept whole, as
//     arrow-rs does, rather than split into "<id>" plus crs_type "srid"),
//     "OGC:CRS84" or a "projjson:<key>" reference, is stored as a JSON string
//     with no crs_type.
//
// The unknown-CRS rules (unset <-> "OGC:CRS84", "srid:0" <-> omitted) follow
// issue #12 and arrow-rs; they are not established as identical in Arrow C++ or
// GDAL. Canonicalizing WGS84 (see ParquetCRS) is the behavior reported for
// arrow-rs, Arrow C++ and GDAL.
func (m *Metadata) SetCRSString(crs string) {
	if crs == "" {
		crs = parquetDefaultCRS
	}

	raw := json.RawMessage(crs)
	if json.Valid(raw) && bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) {
		m.CRS = raw
		m.CRSType = CRSTypePROJJSON
		return
	}

	if isUnknownSRID(crs) {
		m.CRS = nil
		m.CRSType = ""
		return
	}

	m.CRS, _ = json.Marshal(crs)
	m.CRSType = ""
}

// ParquetCRS returns the CRS value to write into a Parquet logical type,
// following the Parquet geospatial spec:
//
//   - An omitted (unknown) GeoArrow CRS is written as "srid:0", because an
//     unset Parquet CRS means OGC:CRS84.
//   - An explicit JSON null is treated the same as an omitted CRS.
//   - An empty string is returned for OGC:CRS84 and EPSG:4326 (as a string or
//     as PROJJSON identifying either) so the CRS is omitted from the logical
//     type. This WGS84 canonicalization is the behavior reported for arrow-rs,
//     Arrow C++ and GDAL; the "srid:0" unknown-CRS rule follows arrow-rs.
//   - A bare SRID with crs_type "srid" (e.g. "4326" from another producer) is
//     written as "srid:<id>".
//   - Inline PROJJSON and other strings, including "srid:<id>" values, are
//     written as-is.
func (m Metadata) ParquetCRS() string {
	if crs := bytes.TrimSpace(m.CRS); len(crs) == 0 || bytes.Equal(crs, []byte("null")) {
		return parquetSRIDPrefix + parquetUnknownSRID
	}

	var crs string
	if err := json.Unmarshal(m.CRS, &crs); err != nil {
		// Not a JSON string, e.g. an inline PROJJSON object.
		if isPROJJSONDefaultCRS(m.CRS) {
			return ""
		}
		return string(m.CRS)
	}

	if crs == "" {
		return ""
	}
	if m.CRSType == CRSTypeSRID {
		if !strings.HasPrefix(crs, parquetSRIDPrefix) {
			crs = parquetSRIDPrefix + crs
		}
	}

	if crs == parquetDefaultCRS || crs == "EPSG:4326" {
		return ""
	}
	return crs
}

// isUnknownSRID reports whether crs is the unknown-CRS SRID, "srid:0".
func isUnknownSRID(crs string) bool {
	return crs == parquetSRIDPrefix+parquetUnknownSRID
}

// isPROJJSONDefaultCRS reports whether a PROJJSON object identifies itself as
// OGC:CRS84 or EPSG:4326 via its "id" member.
func isPROJJSONDefaultCRS(raw json.RawMessage) bool {
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
	switch projjson.ID.Authority {
	case "OGC":
		return code == "CRS84"
	case "EPSG":
		return code == "4326"
	}
	return false
}
