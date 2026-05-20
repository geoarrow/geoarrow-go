package geoarrow

import (
	"fmt"

	json "github.com/goccy/go-json"
)

// Dimension identifies which coordinate axes are present in a GeoArrow
// geometry's vertices.
type Dimension int

// Dimensions defined by the GeoArrow specification. The numeric value is an
// internal index; ordering matches NDim().
const (
	XY   Dimension = iota // X, Y (2D planar)
	XYZ                   // X, Y, Z (3D)
	XYM                   // X, Y, M (2D with measure)
	XYZM                  // X, Y, Z, M (3D with measure)
)

// String returns the canonical short name ("XY", "XYZ", "XYM", "XYZM").
func (d Dimension) String() string {
	switch d {
	case XY:
		return "XY"
	case XYZ:
		return "XYZ"
	case XYM:
		return "XYM"
	case XYZM:
		return "XYZM"
	default:
		return "unknown"
	}
}

// NDim returns the number of float64 coordinate values per vertex for d
// (2 for XY, 3 for XYZ/XYM, 4 for XYZM).
func (d Dimension) NDim() int {
	switch d {
	case XY:
		return 2
	case XYZ, XYM:
		return 3
	case XYZM:
		return 4
	default:
		return 0
	}
}

// GeometryTypeID represents the specific geometry type (e.g., Point,
// LineString, Polygon) and its coordinate dimension (e.g., XY, XYZ, XYM, XYZM)
type GeometryTypeID int

// GeometryTypeID constants defined by the GeoArrow specification. The base
// type (1-6) is offset by 10 for Z, 20 for M, 30 for ZM. See
// https://github.com/geoarrow/geoarrow.
const (
	PointID             GeometryTypeID = 1
	LineStringID        GeometryTypeID = 2
	PolygonID           GeometryTypeID = 3
	MultiPointID        GeometryTypeID = 4
	MultiLineStringID   GeometryTypeID = 5
	MultiPolygonID      GeometryTypeID = 6
	PointZID            GeometryTypeID = 11
	LineStringZID       GeometryTypeID = 12
	PolygonZID          GeometryTypeID = 13
	MultiPointZID       GeometryTypeID = 14
	MultiLineStringZID  GeometryTypeID = 15
	MultiPolygonZID     GeometryTypeID = 16
	PointMID            GeometryTypeID = 21
	LineStringMID       GeometryTypeID = 22
	PolygonMID          GeometryTypeID = 23
	MultiPointMID       GeometryTypeID = 24
	MultiLineStringMID  GeometryTypeID = 25
	MultiPolygonMID     GeometryTypeID = 26
	PointZMID           GeometryTypeID = 31
	LineStringZMID      GeometryTypeID = 32
	PolygonZMID         GeometryTypeID = 33
	MultiPointZMID      GeometryTypeID = 34
	MultiLineStringZMID GeometryTypeID = 35
	MultiPolygonZMID    GeometryTypeID = 36
)

// GeometryValue represents a single concrete value of a GeoArrow geometry
type GeometryValue interface {
	fmt.Stringer
	json.Marshaler
	IsEmpty() bool
	Dimension() Dimension
	GeometryType() GeometryTypeID
}
