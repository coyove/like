package array16

import (
	"bytes"
	"fmt"
)

func Foreach(a []byte, f func(v uint16) bool) {
	r := Reader{Data: a}
	for v, ok := r.Next(); ok; v, ok = r.Next() {
		if !f(v) {
			return
		}
	}
}

func ForeachBlock(a []byte, f func(v uint16) bool) {
	r := BlockReader{Data: a}
	for v, ok := r.Next(); ok; v, ok = r.Next() {
		if !f(v) {
			return
		}
	}
}

func Len(a []byte) (c int) {
	r := Reader{Data: a}
	for _, ok := r.Next(); ok; _, ok = r.Next() {
		c++
	}
	return
}

func Stringify(v []byte) string {
	p := bytes.NewBufferString("[")
	Foreach(v, func(pos uint16) bool {
		fmt.Fprintf(p, "%d,", pos)
		return true
	})
	buf := append(bytes.TrimRight(p.Bytes(), ","), ']')
	return string(buf)
}

func FindConsecutiveValues(a []byte, rest [][]byte) (uint16, bool) {
	r := Reader{Data: a}
	rr := make([]Reader, len(rest))
	for i := range rest {
		rr[i] = Reader{Data: rest[i]}
	}

NEXT:
	for v, ok := r.Next(); ok; v, ok = r.Next() {
		for i, rr := range rr {
			pos := v + uint16(i) + 1
			rr.Forward(pos)
			if rr.Current() != pos {
				continue NEXT
			}
		}
		return v, true
	}
	return 0, false
}
