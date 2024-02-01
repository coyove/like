package array16

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
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

		rd := Reader{Data: lk}
		for v, ok := rd.Next(); ok; v, ok = rd.Next() {
			if v2 := rd.Current(); v2 != v {
				t.Fatal(v, v2)
			}
		}

		fmt.Println(i)
	}
}

func con() ([]uint16, []byte, [][]uint16, [][]byte) {
	base := uint16(rand.Intn(65536))
	n := uint16(rand.Intn(100) + 100)

	gen := func(v uint16, odd bool) (out []uint16) {
		out = append(out, v)
		n := rand.Intn(30) + 30
		for len(out) < n {
			x := uint16(rand.Uint64())
			if odd {
				x = x<<1 | 1
			} else {
				x = x << 1
			}
			if x == v {
				continue
			}
			out = append(out, x)
		}
		return
	}

	a := gen(base, true)
	r := make([][]uint16, n)
	rb := make([][]byte, n)
	for i := range r {
		r[i] = gen(base+uint16(i)+1, false)
		rb[i] = Compress(r[i])
	}

	idx, found := FindConsecutiveValues(Compress(a), rb)
	if !found || idx != base {
		fmt.Println(base, "\t", Stringify(Compress(a)))
		for i, r := range r {
			fmt.Println(base+uint16(i)+1, "\t", Stringify(Compress(r)))
		}
		panic(fmt.Sprintf("%v %v", idx, found))
	}

	return a, Compress(a), r, rb
}

func BenchmarkConsecutive(b *testing.B) {
	b.StopTimer()
	_, ab, _, rb := con()
	b.StartTimer()

	out, _ := os.Create("cpuprofile")
	pprof.StartCPUProfile(out)
	defer pprof.StopCPUProfile()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		FindConsecutiveValues(ab, rb)
	}
}

func TestConsecutive(t *testing.T) {
	// fmt.Println(FindConsecutiveValues(Compress([]uint16{
	// 	915, 1715, 1867, 4666, 5347, 6223, 7082, 7173, 7802, 7866, 9431, 10377, 10666, 10900, 11297, 12857, 12933, 13132, 14075, 18143, 19395, 20906, 21606, 22783, 23139, 24176, 24198, 24309, 24961, 25293, 25414, 26604, 27205, 27394, 29107, 29933,
	// 	30223, 30440, 30469, 30939, 31196, 31596, 31967, 31999, 32518, 32915, 34328, 35185, 35244, 37612, 38128, 38382, 38695,
	// }), [][]byte{
	// 	Compress([]uint16{
	// 		878, 3976, 5018, 5734, 5823, 6273, 6885, 6994, 7286, 9645, 9998, 10054, 10278, 10592, 10747, 12538, 13107, 13700, 13923, 14095, 14226, 14514, 14982, 15246, 15741, 16725, 16889, 16979, 17324, 18047, 18424, 18737, 19021, 19422, 20277, 20434, 24038,
	// 		24313, 24684, 25009, 25878, 25996, 26033, 28014, 28835, 30086, 31028, 31413, 31844, 33177, 33350, 33823, 33842, 35526, 35837, 36849, 37216, 38187, 38696,
	// 	}),
	// }))
	// return

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 1e4; i++ {
		con()
	}
}
