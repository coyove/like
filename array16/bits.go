package array16

import (
	"encoding/binary"
	"fmt"
)

type bits16 struct {
	value [2]uint64
	ptr   int
}

func (b *bits16) remains() int {
	return 128 - b.ptr
}

func (b *bits16) appendBit(v byte) bool {
	if b.ptr >= 128 {
		return false
	}
	if b.ptr < 64 {
		b.value[0] |= uint64(v) << (63 - b.ptr)
	} else {
		b.value[1] |= uint64(v) << (63 - (b.ptr - 64))
	}
	b.ptr++
	return true
}

func (b *bits16) append(v uint16, w int) bool {
	for w--; w >= 0; w-- {
		if !b.appendBit(byte(v>>w) & 1) {
			return false
		}
	}
	return true
}

func (b *bits16) appendHeader(v uint16) {
	if v < 128 {
		b.appendBit(0)
		b.append(v, 7)
	} else if v < 128*128 {
		b.appendBit(1)
		b.append(v>>7, 7)
		b.appendBit(0)
		b.append(v, 7)
	} else {
		b.appendBit(1)
		b.append(v>>9, 7)
		b.appendBit(1)
		b.append(v, 9)
	}
}

func (b *bits16) writeFull(out []byte) []byte {
	out = binary.BigEndian.AppendUint64(out, b.value[0])
	out = binary.BigEndian.AppendUint64(out, b.value[1])
	return out
}

func (b *bits16) write(out []byte) []byte {
	end := b.ptr / 8
	if end*8 != b.ptr {
		end++
	}
	for i := 0; i < end*8; i += 8 {
		if i < 64 {
			out = append(out, byte(b.value[0]>>(56-i)))
		} else {
			out = append(out, byte(b.value[1]>>(64+56-i)))
		}
	}
	return out
}

func (b *bits16) read(w int) (v uint16, ok bool) {
	end := b.ptr + w
	if end > 128 {
		return 0, false
	}
	if end <= 64 {
		v = uint16((b.value[0] >> (64 - end)))
	} else if b.ptr >= 64 {
		v = uint16((b.value[1] >> (128 - end)))
	} else { // ptr < 64 and end > 64
		a := uint16(b.value[0])
		b := uint16(b.value[1] >> (128 - end))
		v = a<<(end-64) | b
	}

	b.ptr = end
	return v & (1<<w - 1), true
}

func readHeader(in []byte) (v uint16, ptr int) {
	hdr := in[0]
	if hdr < 128 {
		return uint16(hdr), 8
	}
	hdr2 := in[1]
	if hdr2 < 128 {
		return uint16(hdr&0x7f)<<7 | uint16(hdr2), 16
	}
	return uint16(hdr&0x7f)<<9 | uint16(in[1]&0x7f)<<2 | uint16(in[2]>>6), 18
}

func (b *bits16) String() string {
	return fmt.Sprintf("%016b\n%016b\n%016b\n%016b\n%016b\n%016b\n%016b\n%016b\n",
		uint16(b.value[0]>>48),
		uint16(b.value[0]>>32),
		uint16(b.value[0]>>16),
		uint16(b.value[0]>>0),
		uint16(b.value[1]>>48),
		uint16(b.value[1]>>32),
		uint16(b.value[1]>>16),
		uint16(b.value[1]>>0),
	)
}
