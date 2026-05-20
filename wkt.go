package geoarrow

import (
	"fmt"
	"strconv"
	"strings"
)

// formatCoord writes a single WKT vertex ("x y", "x y z", etc.) to b,
// using f-format with 6 fractional digits to match the existing output.
func formatCoord(b *strings.Builder, coord []float64) {
	for i, c := range coord {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(strconv.FormatFloat(c, 'f', 6, 64))
	}
}

// formatCoordRing writes a parenthesised, comma-separated sequence of vertices
// drawn from a flat interleaved ring slice.
func formatCoordRing(b *strings.Builder, ring []float64, stride int) {
	nVerts := len(ring) / stride
	b.WriteByte('(')
	for vi := range nVerts {
		if vi > 0 {
			b.WriteString(", ")
		}
		formatCoord(b, ring[vi*stride:(vi+1)*stride])
	}
	b.WriteByte(')')
}

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
// "POLYGON Z") and the observed values-per-vertex. Errors when the prefix
// marker conflicts with nCoords (e.g. "POINT Z(1 2)").
func dimFromWKTPrefix(prefix string, nCoords int) (Dimension, error) {
	upper := strings.ToUpper(prefix)
	hasZM := strings.Contains(upper, " ZM")
	hasZ := !hasZM && strings.Contains(upper, " Z")
	hasM := !hasZM && strings.Contains(upper, " M")

	want := XY
	switch {
	case hasZM:
		want = XYZM
	case hasZ:
		want = XYZ
	case hasM:
		want = XYM
	}

	switch nCoords {
	case 2:
		if hasZM || hasZ || hasM {
			return 0, fmt.Errorf("WKT prefix %q declares 3D/4D but vertex has 2 coords", prefix)
		}
		return XY, nil
	case 3:
		if hasZM {
			return 0, fmt.Errorf("WKT prefix %q declares ZM but vertex has 3 coords", prefix)
		}
		if hasM {
			return XYM, nil
		}
		return XYZ, nil
	case 4:
		if !hasZM && (hasZ || hasM) {
			return 0, fmt.Errorf("WKT prefix %q declares Z or M but vertex has 4 coords", prefix)
		}
		return XYZM, nil
	default:
		return want, fmt.Errorf("invalid coord stride %d", nCoords)
	}
}

// splitTopLevelGroups returns the substrings between matching top-level
// parens in body. Nested groups remain inside their parent's substring.
// Errors on unbalanced parens.
func splitTopLevelGroups(body string) ([]string, error) {
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
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced ')' at offset %d", i)
			}
			if depth == 0 {
				out = append(out, body[start:i])
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced '(' (depth %d at end of input)", depth)
	}
	return out, nil
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
// true when the input encodes an explicit empty geometry, with or without a
// dimension marker (e.g. "POLYGON EMPTY", "POLYGON Z EMPTY").
func wktSplit(s, typeName string) (prefix, body string, isEmpty bool, err error) {
	s = strings.TrimSpace(s)
	openParen := strings.Index(s, "(")
	var head string
	if openParen == -1 {
		head = s
	} else {
		head = strings.TrimSpace(s[:openParen])
	}
	if !hasTypeKeyword(head, typeName) {
		return "", "", false, fmt.Errorf("invalid %s WKT: %s", typeName, s)
	}
	if openParen == -1 {
		if strings.HasSuffix(strings.ToUpper(head), " EMPTY") {
			return head, "", true, nil
		}
		return "", "", false, fmt.Errorf("invalid %s WKT: missing '(': %s", typeName, s)
	}
	if strings.HasSuffix(strings.ToUpper(head), " EMPTY") {
		return head, "", true, nil
	}

	closeParen := strings.LastIndex(s, ")")
	if closeParen < openParen {
		return "", "", false, fmt.Errorf("invalid %s WKT: missing ')': %s", typeName, s)
	}
	return head, s[openParen+1 : closeParen], false, nil
}

// hasTypeKeyword reports whether head begins with typeName as a whole word
// (case-insensitive), so e.g. "POLYGONFOO" does not match "POLYGON".
func hasTypeKeyword(head, typeName string) bool {
	if len(head) < len(typeName) {
		return false
	}
	if !strings.EqualFold(head[:len(typeName)], typeName) {
		return false
	}
	if len(head) == len(typeName) {
		return true
	}
	next := head[len(typeName)]
	return next == ' ' || next == '\t'
}
