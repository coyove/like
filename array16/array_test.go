package array16

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestFuzzy(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 1e3; i++ {
		n := rand.Intn(10000) + 10000
		v := make([]uint16, n)
		dedup := map[uint16]bool{}
		for i := range v {
		AGAIN:
			x := uint16(rand.Uint64())
			if dedup[x] {
				goto AGAIN
			}
			dedup[x] = true
			v[i] = x
		}
		lk := Compress(v)
		for i := 0; i < 65536; i++ {
			v := uint16(i)
			if dedup[v] {
				if _, ok := Contains(lk, v); !ok {
					t.Fatal(v)
				}
			} else {
				if _, ok := Contains(lk, v); ok {
					t.Fatal(v)
				}
			}
		}

		c := 0
		ForeachBlock(CompressFull(v), func(v uint16) bool {
			if !dedup[v] {
				t.Fatal(v)
			}
			c++
			return true
		})
		if c != len(dedup) {
			t.Fatal(c, len(dedup))
		}

		fmt.Println(i)
	}
}
