package geoarrow

import (
	"fmt"
	"strconv"
	"strings"
)

// dimSuffix returns the WKT dimension marker for use after the type name:
// "", " Z", " M", or " ZM".
func dimSuffix(dim Dimension) string {
	switch dim {
	case XYZ:
		return " Z"
	case XYM:
		return " M"
	case XYZM:
		return " ZM"
	default:
		return ""
	}
}

// dimFromWKTPrefix infers the coordinate dimension from a WKT type prefix and
// the number of values per vertex. The prefix is the substring before the
// first '(' (e.g. "POLYGON Z" or "MULTILINESTRING").
func dimFromWKTPrefix(prefix string, nCoords int) Dimension {
	upper := strings.ToUpper(prefix)
	hasZ := strings.Contains(upper, " Z")
	hasZM := strings.Contains(upper, " ZM") || strings.Contains(upper, "ZM")
	hasM := !hasZM && strings.Contains(upper, " M")

	switch nCoords {
	case 2:
		return XY
	case 3:
		if hasM && !hasZ {
			return XYM
		}
		return XYZ
	case 4:
		return XYZM
	default:
		// Fall back to whatever the suffix says, default XY.
		switch {
		case hasZM:
			return XYZM
		case hasZ:
			return XYZ
		case hasM:
			return XYM
		default:
			return XY
		}
	}
}

// splitTopLevelGroups returns the substrings between matching top-level
// parentheses inside body. Nested groups remain in their parent's substring.
// e.g. "(1 2, 3 4), (5 6, 7 8)" → ["1 2, 3 4", "5 6, 7 8"].
func splitTopLevelGroups(body string) []string {
	var out []string
	depth := 0
	start := 0
	for i, ch := range body {
		switch ch {
		case '(':
			if depth == 0 {
				start = i + 1
			}
			depth++
		case ')':
			depth--
			if depth == 0 {
				out = append(out, body[start:i])
			}
		}
	}
	return out
}

// parseWKTFlatCoords parses a comma-separated list of whitespace-separated
// coordinates ("1 2, 3 4, 5 6") into a flat interleaved []float64 and
// returns the inferred stride (values per vertex).
func parseWKTFlatCoords(s string) (coords []float64, stride int, err error) {
	for part := range strings.SplitSeq(s, ",") {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) == 0 {
			continue
		}
		if stride == 0 {
			stride = len(fields)
		} else if len(fields) != stride {
			return nil, 0, fmt.Errorf("inconsistent coord stride: got %d, want %d", len(fields), stride)
		}
		for _, f := range fields {
			val, perr := strconv.ParseFloat(f, 64)
			if perr != nil {
				return nil, 0, fmt.Errorf("invalid coordinate %q: %w", f, perr)
			}
			coords = append(coords, val)
		}
	}
	return coords, stride, nil
}

// wktBody returns the substring of s strictly between the first '(' and the
// matching final ')'. It returns ("", true) if the WKT explicitly encodes an
// empty geometry (e.g. "POLYGON EMPTY").
func wktBody(s, typeName string) (body string, isEmpty bool, err error) {
	s = strings.TrimSpace(s)
	upper := strings.ToUpper(s)
	if !strings.HasPrefix(upper, strings.ToUpper(typeName)) {
		return "", false, fmt.Errorf("invalid %s WKT: %s", typeName, s)
	}
	if strings.Contains(upper, "EMPTY") {
		return "", true, nil
	}

	openParen := strings.Index(s, "(")
	if openParen == -1 {
		return "", false, fmt.Errorf("invalid %s WKT: missing '(': %s", typeName, s)
	}
	closeParen := strings.LastIndex(s, ")")
	if closeParen == -1 || closeParen < openParen {
		return "", false, fmt.Errorf("invalid %s WKT: missing ')': %s", typeName, s)
	}
	return s[openParen+1 : closeParen], false, nil
}

// wktPrefix returns the substring of s before the first '(' (the type
// keyword plus any dimension marker like " Z").
func wktPrefix(s string) string {
	prefix, _, ok := strings.Cut(s, "(")
	if !ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(prefix)
}
