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

// dimFromWKTMarker infers the dimension solely from the marker in a WKT
// prefix ("MULTIPOINT Z" -> XYZ), for geometries with no vertices to inspect.
func dimFromWKTMarker(prefix string) Dimension {
	upper := strings.ToUpper(prefix)
	switch {
	case strings.Contains(upper, " ZM"):
		return XYZM
	case strings.Contains(upper, " Z"):
		return XYZ
	case strings.Contains(upper, " M"):
		return XYM
	default:
		return XY
	}
}

// wktDimension resolves the dimension of a parsed WKT geometry. stride is the
// observed values-per-vertex, or 0 when the geometry has no vertices, in which
// case only the prefix marker is available.
func wktDimension(prefix string, stride int) (Dimension, error) {
	if stride == 0 {
		return dimFromWKTMarker(prefix), nil
	}
	return dimFromWKTPrefix(prefix, stride)
}

// checkStorageDim errors when a parsed geometry's dimension does not match the
// dimension of the type's coordinate storage, which would otherwise corrupt
// the builder.
func checkStorageDim(typeName string, got, want Dimension) error {
	if got != want {
		return fmt.Errorf("%s: WKT dimension %s does not match storage dimension %s", typeName, got, want)
	}
	return nil
}

// splitTopLevelItems splits body on commas that are not nested inside
// parentheses. A body that is only whitespace yields no items.
func splitTopLevelItems(body string) ([]string, error) {
	if strings.TrimSpace(body) == "" {
		return nil, nil
	}
	var out []string
	depth, start := 0, 0
	add := func(end int) error {
		item := strings.TrimSpace(body[start:end])
		if item == "" {
			return fmt.Errorf("empty item at offset %d", start)
		}
		out = append(out, item)
		return nil
	}
	for i, ch := range body {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced ')' at offset %d", i)
			}
		case ',':
			if depth == 0 {
				if err := add(i); err != nil {
					return nil, err
				}
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced '(' (depth %d at end of input)", depth)
	}
	if err := add(len(body)); err != nil {
		return nil, err
	}
	return out, nil
}

// wktUnwrap returns the contents of a parenthesised WKT item "(...)". isEmpty
// is true for the bare keyword EMPTY.
func wktUnwrap(item string) (inner string, isEmpty bool, err error) {
	item = strings.TrimSpace(item)
	if strings.EqualFold(item, "EMPTY") {
		return "", true, nil
	}
	if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
		return "", false, fmt.Errorf("expected parenthesised group, got %q", item)
	}
	return item[1 : len(item)-1], false, nil
}

// mergeStride reconciles the vertex width seen so far (0 = none yet) with the
// width of a newly parsed group (0 = group had no vertices).
func mergeStride(cur, got int) (int, error) {
	switch {
	case got == 0:
		return cur, nil
	case cur == 0:
		return got, nil
	case cur != got:
		return 0, fmt.Errorf("inconsistent coord stride: got %d, want %d", got, cur)
	}
	return cur, nil
}

// parseWKTCoordItems parses a body like "(1 2, 3 4), EMPTY, (5 6, 7 8)" into
// one flat interleaved slice per item (nil for EMPTY). stride is the shared
// vertex width, or 0 if no item has vertices.
func parseWKTCoordItems(body string) (groups [][]float64, stride int, err error) {
	items, err := splitTopLevelItems(body)
	if err != nil {
		return nil, 0, err
	}
	groups = make([][]float64, 0, len(items))
	for _, item := range items {
		inner, isEmpty, err := wktUnwrap(item)
		if err != nil {
			return nil, 0, err
		}
		if isEmpty {
			groups = append(groups, nil)
			continue
		}
		coords, s, err := parseWKTFlatCoords(inner)
		if err != nil {
			return nil, 0, err
		}
		if stride, err = mergeStride(stride, s); err != nil {
			return nil, 0, err
		}
		groups = append(groups, coords)
	}
	return groups, stride, nil
}

// parseWKTMultiPointCoords parses a MULTIPOINT body in either accepted form,
// "(1 2), (3 4)" or "1 2, 3 4", into a flat interleaved slice.
func parseWKTMultiPointCoords(body string) (coords []float64, stride int, err error) {
	items, err := splitTopLevelItems(body)
	if err != nil {
		return nil, 0, err
	}
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item), "EMPTY") {
			return nil, 0, fmt.Errorf("empty point members are not supported")
		}
		if strings.HasPrefix(item, "(") {
			inner, _, err := wktUnwrap(item)
			if err != nil {
				return nil, 0, err
			}
			item = inner
		}
		pt, s, err := parseWKTFlatCoords(item)
		if err != nil {
			return nil, 0, err
		}
		if s == 0 || len(pt) != s {
			return nil, 0, fmt.Errorf("point member %q must have exactly one vertex", item)
		}
		if stride, err = mergeStride(stride, s); err != nil {
			return nil, 0, err
		}
		coords = append(coords, pt...)
	}
	return coords, stride, nil
}

// formatCoordRingOrEmpty is formatCoordRing, but spells a vertex-less ring
// "EMPTY" so the output parses back.
func formatCoordRingOrEmpty(b *strings.Builder, ring []float64, stride int) {
	if len(ring) == 0 {
		b.WriteString("EMPTY")
		return
	}
	formatCoordRing(b, ring, stride)
}

// vertices splits a flat interleaved slice into per-vertex slices.
func vertices(flat []float64, stride int) [][]float64 {
	n := len(flat) / stride
	out := make([][]float64, n)
	for i := range n {
		out[i] = flat[i*stride : (i+1)*stride]
	}
	return out
}
