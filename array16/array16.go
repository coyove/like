package array16

import (
	"encoding/binary"
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

func Contains(a []byte, v uint16) (Reader, bool) {
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
		r := Reader{Data: a[i:]}
		if head, ok := r.Next(); ok && uint16(head) == v {
			return r, true
		}
	}

	i -= blockSize
	if i < 0 {
		rn := Reader{Data: a}
		rn.Next()
		return rn, false
	}

	end := i + blockSize
	if end > len(a) {
		end = len(a) // the last block may be shorter then 'block' bytes
	}

	r := Reader{Data: a[end:], br: BlockReader{Data: a[i:end]}}
	for head, ok := r.Next(); ok; head, ok = r.Next() {
		if head >= v {
			return r, head == v
		}
	}
	return r, false
}

type Reader struct {
	Data []byte
	br   BlockReader
}

func (r *Reader) Forward(v uint16) {
	if v <= r.br.Current {
		return
	}

	for i := 0; i < 2; {
		n, ok := r.Next()
		if !ok {
			return
		}
		if v <= n {
			return
		}
		if len(r.br.Data) == 0 {
			i++
		}
	}

	// fmt.Println("-", r.Current(), v, r.br.Data, r.Data)

	// Too many Next, use binary search.
	*r, _ = Contains(r.Data, v)

	// fmt.Println("+", r.Current(), v)
}

func (r *Reader) Current() uint16 {
	return r.br.Current
}

func (r *Reader) Next() (uint16, bool) {
	v, ok := r.br.Next()
	if ok || len(r.Data) == 0 {
		return v, ok
	}

	end := blockSize
	if end > len(r.Data) {
		end = len(r.Data)
	}
	r.br = BlockReader{Data: r.Data[:end]}
	r.Data = r.Data[end:]
	return r.Next()
}

type BlockReader struct {
	Data    []byte
	Current uint16
	fired   bool
	pack    byte
}

func (br *BlockReader) Next() (uint16, bool) {
	if len(br.Data) == 0 {
		return 0, false
	}

	if !br.fired {
		v, w := binary.Uvarint(br.Data)
		br.Data = br.Data[w:]
		br.Current = uint16(v)
		br.fired = true
		return uint16(v), true
	}

	x := br.Data
	if x[0]>>7 == 0 {
		next, w := uvarint(x)
		if w <= 0 {
			// End
			return 0, false
		}
		br.Data = br.Data[w:]
		br.Current += uint16(next)
		return br.Current, true
	}

	if x[0]>>6 == 2 { // 0b10, pack3
		y := binary.BigEndian.Uint32(x)
		br.Current += uint16((y >> [...]int{20, 10, 0}[br.pack]) & 0x3ff)
		if br.pack < 2 {
			br.pack++
		} else {
			br.pack = 0
			br.Data = br.Data[4:]
		}
		return br.Current, true
	}

	// 0b11, pack5
	y := binary.BigEndian.Uint32(x)
	br.Current += uint16((y >> [...]int{24, 18, 12, 6, 0}[br.pack]) & 0x3f)
	if br.pack < 4 {
		br.pack++
	} else {
		br.pack = 0
		br.Data = br.Data[4:]
	}
	return br.Current, true
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
	if len(buf) == 0 {
		return 0, 0
	}

	b := buf[0]
	x := uint64(b & 0x3f)
	if b >= 0x40 {
		return x, 1
	}
	s := 6

	for i := 1; i < len(buf); i++ {
		b := buf[i]
		x |= uint64(b&0x7f) << s
		if b >= 0x80 {
			return x, i + 1
		}
		s += 7
	}

	return 0, -1
}
