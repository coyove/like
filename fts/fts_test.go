package fts

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestVarint(t *testing.T) {
	// v := AppendSortedUvarint(nil, 128)
	// fmt.Println(v)
	// fmt.Println(SortedUvarint(v))
	// return
	rand.Seed(time.Now().Unix())

	tmp := make([]uint64, 1e3)
	buf := make([][]byte, len(tmp))

	for i := 0; i < 1e4; i++ {
		for i := range tmp {
			switch rand.Intn(2) {
			case 1:
				tmp[i] = uint64(rand.Intn(1000))
			case 2:
				tmp[i] = uint64(rand.Intn(10000000))
			default:
				tmp[i] = rand.Uint64()
			}
			buf[i] = AppendSortedUvarint(buf[i][:0], tmp[i])
		}

		sort.Slice(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
		sort.Slice(buf, func(i, j int) bool { return bytes.Compare(buf[i], buf[j]) == -1 })

		for i, v := range tmp {
			v2, _ := SortedUvarint(buf[i])
			if v2 != v {
				for i := range tmp {
					fmt.Println(tmp[i], buf[i])
				}
				t.Fatal(v, v2, buf[i])
			}
		}
	}
}
