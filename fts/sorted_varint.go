package fts

import (
	"encoding/binary"
)

func AppendSortedUvarint(b []byte, v uint64) []byte {
	// 	1 << 13, // 0
	// 	1 << 21, // 1
	// 	1 << 29, // 2
	// 	1 << 37, // 3
	// 	1 << 45, // 4
	// 	1 << 53, // 5
	// 	1 << 61, // 6
	for i := 0; i < 7; i++ {
		body := (i + 1) * 8

		if v < 1<<(5+body) {
			b = append(b, byte(i)<<5|byte(v>>body))
			for ii := i; ii >= 0; ii-- {
				b = append(b, byte(v>>(ii*8)))
			}
			return b
		}
	}
	b = append(b, 7<<5)
	b = binary.BigEndian.AppendUint64(b, v)
	return b
}

func SortedUvarint(b []byte) (uint64, int) {
	if len(b) == 0 {
		return 0, -1
	}

	if w := int(b[0] >> 5); w < 7 {
		if len(b) < 1+w+1 {
			return 0, -w * 10
		}
		v := uint64(b[0] << 3 >> 3)
		for i := 0; i <= w; i++ {
			v = v<<8 | uint64(b[1+i])
		}
		return v, 1 + w + 1
	}

	if len(b) < 9 {
		return 0, -2
	}
	return binary.BigEndian.Uint64(b[1:]), 9
}
