package array16

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

func Compress(a []uint16) (res []byte) {
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return compressSize(nil, a)
}

func compressSize(out []byte, a []uint16) []byte {
	if len(a) == 0 {
		return out
	}

	d := make([]uint16, 0, len(a))
	for i := 1; i < len(a); i++ {
		d = append(d, a[i]-a[i-1])
	}
	// fmt.Println(d)

	var buf bits16
	buf.append(a[0], 16)
	a = a[1:]

	for len(d) > 0 {
		off := d[0]
		if off < 32 && buf.remains() >= 6 {
			buf.appendBit(0)
			buf.append(off, 5)
		} else if off < 1024 && buf.remains() >= 12 {
			buf.appendBit(1)
			buf.appendBit(0)
			buf.append(off, 10)
		} else if off < 4096 && buf.remains() >= 15 {
			buf.appendBit(1)
			buf.appendBit(1)
			buf.appendBit(0)
			buf.append(off, 12)
		} else if off < 32768 && buf.remains() >= 18 {
			buf.appendBit(1)
			buf.appendBit(1)
			buf.appendBit(1)
			buf.append(off, 15)
		} else {
			buf.append(0, 6)
			out = buf.writeFull(out)
			buf = bits16{}
			buf.append(a[0], 16)
		}
		a = a[1:]
		d = d[1:]
	}

	buf.append(0, 6)
	out = buf.write(out)
	return out
}

const blockSize = 16

func Contains(buf []byte, startValue, endValue uint16) (uint16, bool) {
	i := 0
	j := len(buf) / blockSize
	if j*blockSize < len(buf) {
		j++
	}
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		if head := binary.BigEndian.Uint16(buf[h*blockSize:]); uint16(head) < startValue {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	i *= blockSize
	if i < len(buf) {
		if head := binary.BigEndian.Uint16(buf[i:]); startValue <= uint16(head) && uint16(head) <= endValue {
			return uint16(head), true
		}
	}

	for i -= blockSize; i >= 0 && i < len(buf); i += blockSize {
		end := i + blockSize
		if end > len(buf) {
			end = len(buf) // the last block may be shorter then 'block' bytes
		}
		found := -2
		ForeachBlock(buf[i:end], func(head uint16) bool {
			if startValue <= head && head <= endValue {
				found = int(head)
				return false
			}
			if head > endValue {
				found = -1
				return false
			}
			return true
		})

		if found >= 0 {
			return uint16(found), true
		} else if found == -1 {
			return 0, false
		}
	}
	return 0, false
}

func Foreach(a []byte, f func(v uint16) bool) {
	for i := 0; i < len(a); i += blockSize {
		end := i + blockSize
		if end > len(a) {
			end = len(a) // the last block may be shorter then 'block' bytes
		}
		if !ForeachBlock(a[i:end], f) {
			break
		}
	}
}

func Len(a []byte) (c int) {
	for i := 0; i < len(a); i += blockSize {
		end := i + blockSize
		if end > len(a) {
			end = len(a)
		}
		ForeachBlock(a[i:end], func(uint16) bool { c++; return true })
	}
	return
}

var zzz [8]int

func ForeachBlock(x []byte, f func(v uint16) bool) bool {
	if len(x) == 0 {
		return true
	}

	var tmp [16]byte
	copy(tmp[:], x)
	rd := bits16{}
	rd.value[0] = binary.BigEndian.Uint64(tmp[:8])
	rd.value[1] = binary.BigEndian.Uint64(tmp[8:])

	head, ok := rd.read(16)
	if !ok {
		panic("bad read")
	}
	if !f(head) {
		return false
	}

	for {
		h, ok := rd.read(3)
		if !ok {
			break
		}

		switch h {
		case 0b000, 0b001, 0b010, 0b011:
			v, ok := rd.read(3) // 5
			v = h<<3 | v
			if !ok || v == 0 {
				break
			}
			head += v
			zzz[h]++
		case 0b100, 0b101:
			zzz[h]++
			v, ok := rd.read(9) // 10
			v = (h&1)<<9 | v
			if !ok || v == 0 {
				panic("bad read")
			}
			head += v
		case 0b110:
			zzz[h]++
			v, ok := rd.read(12) // 12
			if !ok || v == 0 {
				panic("bad read")
			}
			head += v
		case 0b111:
			zzz[h]++
			v, ok := rd.read(15) // 15
			if !ok || v == 0 {
				panic("bad read")
			}
			head += v
		}

		if !f(head) {
			return false
		}
	}
	// fmt.Println(zzz)
	return true
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
