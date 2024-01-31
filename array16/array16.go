package array16

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

const blockSize = 16

func Compress(a []uint16) (res []byte) {
	return compressSize(a, blockSize)
}

func CompressFull(a []uint16) (res []byte) {
	return compressSize(a, 1e8)
}

func compressSize(a []uint16, block int) (res []byte) {
	if len(a) == 0 {
		return nil
	}

	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })

	d := make([]uint16, 0, len(a))
	for i := 1; i < len(a); i++ {
		d = append(d, a[i]-a[i-1])
	}
	// fmt.Println(d)

	var final []byte
	buf := binary.AppendUvarint(nil, uint64(a[0]))
	for i := 1; len(d) > 0; {
		pack5, pack3 := false, false
		if len(d) >= 5 && d[0] < 64 && d[1] < 64 && d[2] < 64 && d[3] < 64 && d[4] < 64 {
			pack5 = len(buf)+4 <= block
		}
		if !pack5 {
			if len(d) >= 3 && d[0] < 1024 && d[1] < 1024 && d[2] < 1024 {
				s := 0
				for i := 0; i < 3; i++ {
					if d[i] >= 64 {
						s++
					}
				}
				if s >= 2 {
					pack3 = len(buf)+4 <= block
				}
			}
		}
		if pack5 {
			var x uint32 = 1<<31 | 1<<30 | uint32(d[0])<<24 | uint32(d[1])<<18 | uint32(d[2])<<12 | uint32(d[3])<<6 | uint32(d[4])
			buf = binary.BigEndian.AppendUint32(buf, x)
			d = d[5:]
			i += 5
		} else if pack3 {
			var x uint32 = 1<<31 | 0<<30 | uint32(d[0])<<20 | uint32(d[1])<<10 | uint32(d[2])
			buf = binary.BigEndian.AppendUint32(buf, x)
			d = d[3:]
			i += 3
		} else {
			buf = appendUvarint(buf, uint64(d[0]))
			if len(buf) > block {
				final = append(final, buf[:block]...)
				buf = binary.AppendUvarint(buf[:0], uint64(a[i]))
			}
			d = d[1:]
			i++
		}
	}
	final = append(final, buf...)
	return final
}

func Contains(a []byte, v uint16) int {
	i := 0
	j := len(a) / blockSize
	if j*blockSize < len(a) {
		j++
	}
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		if head, _ := binary.Uvarint(a[h*blockSize:]); uint16(head) < v {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	i *= blockSize
	if i < len(a) {
		if head, _ := binary.Uvarint(a[i:]); uint16(head) == v {
			return i
		}
	}

	i -= blockSize
	if i >= 0 && i < len(a) {
		end := i + blockSize
		if end > len(a) {
			end = len(a) // the last block may be shorter then 'block' bytes
		}
		found := false
		ForeachFull(a[i:end], func(head uint16) bool {
			if head == v {
				found = true
				return false
			}
			return true
		})
		if found {
			return i
		}
	}
	return -1
}

func Foreach(a []byte, f func(v uint16) bool) {
	for i := 0; i < len(a); i += blockSize {
		end := i + blockSize
		if end > len(a) {
			end = len(a) // the last block may be shorter then 'block' bytes
		}
		if !ForeachFull(a[i:end], f) {
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
		ForeachFull(a[i:end], func(uint16) bool { c++; return true })
	}
	return
}

func ForeachFull(x []byte, f func(v uint16) bool) bool {
	if len(x) == 0 {
		return true
	}
	v, w := binary.Uvarint(x)
	head := uint16(v)
	if !f(head) {
		return false
	}
	x = x[w:]

	for len(x) > 0 {
		if x[0]>>7 == 0 {
			next, w := uvarint(x)
			if w <= 0 {
				break
			}
			head += uint16(next)
			if !f(head) {
				return false
			}
			x = x[w:]
		} else if x[0]>>6 == 2 { // 0b10, pack3
			y := binary.BigEndian.Uint32(x)
			for _, a := range [3]uint32{(y >> 20) & 0x3ff, (y >> 10) & 0x3ff, (y) & 0x3ff} {
				if head += uint16(a); !f(head) {
					return false
				}
			}
			x = x[4:]
		} else { // 0b11, pack5
			y := binary.BigEndian.Uint32(x)
			for _, a := range [5]uint32{(y >> 24) & 0x3f, (y >> 18) & 0x3f, (y >> 12) & 0x3f, (y >> 6) & 0x3f, (y) & 0x3f} {
				if head += uint16(a); !f(head) {
					return false
				}
			}
			x = x[4:]
		}
	}
	return true
}

func appendUvarint(buf []byte, x uint64) []byte {
	if x < 0x40 {
		return append(buf, byte(x|0x40))
	}
	buf = append(buf, byte(x&0x3f))
	x >>= 6
	for x >= 0x80 {
		buf = append(buf, byte(x&0x7f))
		x >>= 7
	}
	return append(buf, byte(x|0x80))
}

func uvarint(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range buf {
		if i == 10 {
			return 0, -(i + 1)
		}
		if i == 0 {
			x |= uint64(b & 0x3f)
			if b >= 0x40 {
				return x, i + 1
			}
			s = 6
		} else {
			x |= uint64(b&0x7f) << s
			if b >= 0x80 {
				return x, i + 1
			}
			s += 7
		}
	}
	return 0, 0
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
