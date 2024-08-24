package array16

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"testing"
)

func TestFuzzy(t *testing.T) {
	if false {
		zzz := []uint16{202, 1491, 1708, 2332, 2373, 3076, 3196, 3280, 3418, 4031, 4428, 4500, 5124, 5857, 5940, 6530, 6565, 7214, 7940, 9539, 10118, 10684, 10751, 11282, 11332, 11418, 11920, 12697, 13270, 13325, 13609, 13639, 14766, 15348, 15895, 16098, 16830, 16853, 17690, 17746, 17824, 18347, 18577, 18929, 19365, 19679, 19794, 19854, 19921, 20796, 20905, 21192, 21437, 21923, 22267, 22545, 23076, 23537, 23873, 26203, 26437, 26794, 27419, 27832, 28139, 28433, 29031, 29265, 29998, 30054, 30930, 31161, 31184, 31720, 33844, 34792, 34800, 34819, 34923, 35083, 35170, 35604, 35921, 36131, 36745, 37078, 37261, 37421, 37698, 38370, 39164, 39259, 39487, 40752, 40756, 41397, 41844, 42167, 42714, 43550, 44183, 45565, 46667, 47390, 48043, 48363, 48376, 49153, 50784, 51675, 52082, 52773, 54263, 54351, 54866, 55028, 56096, 56370, 56652, 57200, 57643, 58491, 58588, 58795, 58895, 59229, 60238, 62468, 62706, 63120, 63254, 64060, 64664, 65517, 65524}
		out := Compress(zzz)
		fmt.Println(Contains(out, 34795, 34805))
		return
	}

	out, _ := os.Create("cpuprofile")
	defer out.Close()

	pprof.StartCPUProfile(out)
	defer pprof.StopCPUProfile()

	zzz := 0.0
	for i := 0; i < 1e3; i++ {
		n := rand.Intn(10000) + 10000
		if rand.Intn(2) == 0 {
			n = rand.Intn(100) + 100
		}

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
		zzz += float64(len(v)*2) / float64(len(lk))
		for i := 0; i < 65536; i++ {
			i16 := uint16(i)
			x := uint16(rand.Intn(20))
			start := SubSat(i16, x)
			end := AddSat(i16, x)
			if dedup[i16] {
				if _, ok := Contains(lk, start, end); !ok {
					xx, _ := json.Marshal(v)
					t.Fatal(string(xx), start, end)
				}
			} else {
				if _, ok := Contains(lk, i16, i16); ok {
					t.Fatal(i16)
				}
			}
		}

		// c := 0
		// ForeachFull(CompressFull(v), func(v uint16) bool {
		// 	if !dedup[v] {
		// 		t.Fatal(v)
		// 	}
		// 	c++
		// 	return true
		// })
		// if c != len(dedup) {
		// 	t.Fatal(c, len(dedup))
		// }

		fmt.Println(i)
	}
	fmt.Println(zzz / 1000)
}

func TestContainsRange(t *testing.T) {
	v := []uint16{3, 5, 6, 7, 9, 11, 13, 150, 1700, 1900, 2100, 23000, 25000, 27000}
	lk := Compress(v)
	if _, ok := Contains(lk, 2100, 2100); !ok {
		t.Fatal(lk)
	}
	if _, ok := Contains(lk, 2101, 2200); ok {
		t.Fatal(lk)
	}
	if _, ok := Contains(lk, 22888, 23000); !ok {
		t.Fatal(lk)
	}
	if _, ok := Contains(lk, 22888, 23001); !ok {
		t.Fatal(lk)
	}
	if _, ok := Contains(lk, 27001, 65535); ok {
		t.Fatal(lk)
	}
	if _, ok := Contains(lk, 22888, 23000); !ok {
		t.Fatal(lk)
	}
	if _, ok := Contains(lk, 0, 4); !ok {
		t.Fatal(lk)
	}
	if _, ok := Contains(lk, 0, 2); ok {
		t.Fatal(lk)
	}
	if _, ok := Contains(lk, 0, 23001); !ok {
		t.Fatal(lk)
	}
}

func TestBits(t *testing.T) {
	for i := 0; i < 1e6; i++ {
		var b bits16
		var tmp []uint16
		var ws []int
		for {
			v := uint16(rand.Intn(65536))
			w := rand.Intn(15) + 1
			if !b.append(v, w) {
				break
			}
			tmp = append(tmp, v&(1<<w-1))
			ws = append(ws, w)
		}

		b.ptr = 0
		for i := 0; i < len(ws); i++ {
			v, ok := b.read(ws[i])
			if !ok {
				break
			}
			if v != tmp[i] {
				fmt.Printf("%016b\n%016b\n%016b\n%016b\n%016b\n%016b\n%016b\n%016b\n",
					uint16(b.value[0]>>48),
					uint16(b.value[0]>>32),
					uint16(b.value[0]>>16),
					uint16(b.value[0]>>0),
					uint16(b.value[1]>>48),
					uint16(b.value[1]>>32),
					uint16(b.value[1]>>16),
					uint16(b.value[1]>>0),
				)
				t.Fatalf("%v<>%d %v w=%v %v", v, tmp[i], b, ws[i], ws)
			}
		}
		// fmt.Println(b.ptr)
	}
}
