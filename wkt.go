package geoarrow

import (
	"fmt"
	"strconv"
	"strings"
)

// dimSuffix returns the WKT dimension marker placed after the type name:
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

// dimFromWKTPrefix infers dimension from a WKT type prefix (e.g.
// "POLYGON Z") and the observed values-per-vertex.
func dimFromWKTPrefix(prefix string, nCoords int) Dimension {
	upper := strings.ToUpper(prefix)
	hasZM := strings.Contains(upper, "ZM")
	hasZ := !hasZM && strings.Contains(upper, " Z")
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
// parens in body. Nested groups remain inside their parent's substring.
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

// parseWKTFlatCoords parses comma-separated, whitespace-delimited coords
// ("1 2, 3 4") into a flat interleaved slice plus the inferred stride.
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

// wktSplit returns the WKT prefix (type keyword + optional dim marker) and
// body (substring between the first '(' and matching final ')'). isEmpty is
// true when the input encodes an explicit empty geometry like "POLYGON EMPTY".
func wktSplit(s, typeName string) (prefix, body string, isEmpty bool, err error) {
	s = strings.TrimSpace(s)
	openParen := strings.Index(s, "(")
	if openParen == -1 {
		// No body — must match either "<typeName> EMPTY" or be invalid.
		if strings.EqualFold(strings.TrimSpace(s), typeName+" EMPTY") {
			return strings.TrimSpace(s), "", true, nil
		}
		return "", "", false, fmt.Errorf("invalid %s WKT: %s", typeName, s)
	}
	prefix = strings.TrimSpace(s[:openParen])
	if !strings.EqualFold(prefix[:min(len(prefix), len(typeName))], typeName) {
		return "", "", false, fmt.Errorf("invalid %s WKT: %s", typeName, s)
	}
	// "EMPTY" only ever appears in the prefix region; safe to check there.
	if strings.Contains(strings.ToUpper(prefix), "EMPTY") {
		return prefix, "", true, nil
	}

	closeParen := strings.LastIndex(s, ")")
	if closeParen < openParen {
		return "", "", false, fmt.Errorf("invalid %s WKT: missing ')': %s", typeName, s)
	}
	return prefix, s[openParen+1 : closeParen], false, nil
}
