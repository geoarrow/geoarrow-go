package geoarrow

import (
	"fmt"

	json "github.com/goccy/go-json"
)

// expectDelim consumes one token and asserts it equals want.
func expectDelim(dec *json.Decoder, want json.Delim) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := t.(json.Delim)
	if !ok || d != want {
		return fmt.Errorf("expected %q, got %T(%v)", want, t, t)
	}
	return nil
}

// decodeOpenOrNull consumes one token; if null returns isNull=true, if '['
// returns isNull=false, otherwise errors.
func decodeOpenOrNull(dec *json.Decoder) (isNull bool, err error) {
	t, err := dec.Token()
	if err != nil {
		return false, err
	}
	if t == nil {
		return true, nil
	}
	d, ok := t.(json.Delim)
	if !ok || d != '[' {
		return false, fmt.Errorf("expected '[' or null, got %T(%v)", t, t)
	}
	return false, nil
}

// decodeFloatsAfterOpen consumes float tokens until the next ']' (which it
// also consumes). Caller must have already consumed the opening '['.
func decodeFloatsAfterOpen(dec *json.Decoder) ([]float64, error) {
	var out []float64
	for dec.More() {
		var f float64
		if err := dec.Decode(&f); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	if _, err := dec.Token(); err != nil { // closing ']'
		return nil, err
	}
	return out, nil
}

// decodeCoordList decodes a JSON array of coord arrays ([[x,y], [x,y], ...])
// into a flat interleaved slice. Caller must have already consumed the
// outer '['; the matching ']' is consumed before returning.
func decodeCoordList(dec *json.Decoder) ([]float64, error) {
	var out []float64
	for dec.More() {
		if err := expectDelim(dec, '['); err != nil {
			return nil, err
		}
		coord, err := decodeFloatsAfterOpen(dec)
		if err != nil {
			return nil, err
		}
		out = append(out, coord...)
	}
	if _, err := dec.Token(); err != nil { // closing ']'
		return nil, err
	}
	return out, nil
}
