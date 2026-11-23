package geoarrow

import (
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Helpers shared by the nested-List geometry types (LineString, MultiPoint,
// MultiLineString, MultiPolygon). Each reads or writes slot i / one value of a
// List<...<coord>> storage array or builder, with the number of List layers
// fixed by the helper name.

// readCoordList reads slot i of a List<coord> into flat interleaved coords.
func readCoordList(a array.ExtensionArray, i, stride int) []float64 {
	list := a.Storage().(*array.List)
	start, end := list.ValueOffsets(i)
	return readCoordsRange(list.ListValues(), int(start), int(end), stride)
}

// appendCoordList appends one List<coord> value to builder b.
func appendCoordList(b array.Builder, coords []float64, stride int) {
	lb := b.(*array.ListBuilder)
	lb.Append(true)
	appendCoords(lb.ValueBuilder(), coords, stride)
}

// readCoordListList reads slot i of a List<List<coord>>.
func readCoordListList(a array.ExtensionArray, i, stride int) [][]float64 {
	outer := a.Storage().(*array.List)
	start, end := outer.ValueOffsets(i)
	inner := outer.ListValues().(*array.List)
	coordArr := inner.ListValues()

	out := make([][]float64, end-start)
	for j := start; j < end; j++ {
		s, e := inner.ValueOffsets(int(j))
		out[j-start] = readCoordsRange(coordArr, int(s), int(e), stride)
	}
	return out
}

// appendCoordListList appends one List<List<coord>> value to builder b.
func appendCoordListList(b array.Builder, lists [][]float64, stride int) {
	outer := b.(*array.ListBuilder)
	outer.Append(true)
	inner := outer.ValueBuilder().(*array.ListBuilder)
	coordBuilder := inner.ValueBuilder()
	for _, coords := range lists {
		inner.Append(true)
		appendCoords(coordBuilder, coords, stride)
	}
}

// readCoordListListList reads slot i of a List<List<List<coord>>>.
func readCoordListListList(a array.ExtensionArray, i, stride int) [][][]float64 {
	outer := a.Storage().(*array.List)
	start, end := outer.ValueOffsets(i)
	mid := outer.ListValues().(*array.List)
	inner := mid.ListValues().(*array.List)
	coordArr := inner.ListValues()

	out := make([][][]float64, end-start)
	for j := start; j < end; j++ {
		rs, re := mid.ValueOffsets(int(j))
		rings := make([][]float64, re-rs)
		for r := rs; r < re; r++ {
			vs, ve := inner.ValueOffsets(int(r))
			rings[r-rs] = readCoordsRange(coordArr, int(vs), int(ve), stride)
		}
		out[j-start] = rings
	}
	return out
}

// appendCoordListListList appends one List<List<List<coord>>> value to b.
func appendCoordListListList(b array.Builder, nested [][][]float64, stride int) {
	outer := b.(*array.ListBuilder)
	outer.Append(true)
	mid := outer.ValueBuilder().(*array.ListBuilder)
	inner := mid.ValueBuilder().(*array.ListBuilder)
	coordBuilder := inner.ValueBuilder()
	for _, rings := range nested {
		mid.Append(true)
		for _, coords := range rings {
			inner.Append(true)
			appendCoords(coordBuilder, coords, stride)
		}
	}
}
