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
		fmt.Fprintf(p, "%d ", pos)
		return true
	})
	buf := append(bytes.TrimSpace(p.Bytes()), ']')
	return string(buf)
}
