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

// decodeOpenOrNull peeks the next token. If it is null, returns (isNull=true).
// If it is '[', it is consumed and (isNull=false) is returned. Anything else
// is an error.
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

// decodeFloatsUntilClose consumes float tokens from dec until the next ']'
// (which it also consumes), appending them to dst. The caller must have
// already consumed the opening '['.
func decodeFloatsUntilClose(dec *json.Decoder, dst []float64) ([]float64, error) {
	for dec.More() {
		var f float64
		if err := dec.Decode(&f); err != nil {
			return nil, err
		}
		dst = append(dst, f)
	}
	if _, err := dec.Token(); err != nil { // closing ']'
		return nil, err
	}
	return dst, nil
}

// decodeCoordArray decodes one bracketed coord array ([x, y, ...]) and
// appends its values to dst. The opening '[' has NOT yet been consumed.
func decodeCoordArray(dec *json.Decoder, dst []float64) ([]float64, error) {
	if err := expectDelim(dec, '['); err != nil {
		return nil, err
	}
	return decodeFloatsUntilClose(dec, dst)
}

// decodeCoordList decodes a JSON array of coord arrays ([[x,y], [x,y], ...])
// into a flat interleaved slice. The outer '[' must already be consumed; the
// closing ']' is consumed before returning.
func decodeCoordList(dec *json.Decoder) ([]float64, error) {
	var out []float64
	for dec.More() {
		var err error
		out, err = decodeCoordArray(dec, out)
		if err != nil {
			return nil, err
		}
	}
	if _, err := dec.Token(); err != nil { // closing ']'
		return nil, err
	}
	return out, nil
}
