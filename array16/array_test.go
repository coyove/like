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
			x := uint16(rand.Intn(20))
			start := SubSat(v, x)
			end := AddSat(v, x)
			if dedup[v] {
				if !Contains(lk, start, end) {
					t.Fatal(v)
				}
			} else {
				if Contains(lk, v, v) {
					t.Fatal(v)
				}
			}
		}

		c := 0
		ForeachFull(CompressFull(v), func(v uint16) bool {
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

func TestContainsRange(t *testing.T) {
	v := []uint16{3, 5, 6, 7, 9, 11, 13, 150, 1700, 1900, 2100, 23000, 25000, 27000}
	lk := Compress(v)
	if !Contains(lk, 2100, 2100) {
		t.Fatal(lk)
	}
	if Contains(lk, 2101, 2200) {
		t.Fatal(lk)
	}
	if !Contains(lk, 22888, 23000) {
		t.Fatal(lk)
	}
	if !Contains(lk, 22888, 23001) {
		t.Fatal(lk)
	}
	if Contains(lk, 27001, 65535) {
		t.Fatal(lk)
	}
	if !Contains(lk, 22888, 23000) {
		t.Fatal(lk)
	}
	if !Contains(lk, 0, 4) {
		t.Fatal(lk)
	}
	if Contains(lk, 0, 2) {
		t.Fatal(lk)
	}
	if !Contains(lk, 0, 23001) {
		t.Fatal(lk)
	}
}
