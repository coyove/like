package like

import (
	"hash/crc32"
	"unicode"
	"unicode/utf8"

	"github.com/coyove/like/array16"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

var letterBMP1Fast = func() (letter [65536 / 8]byte) {
	for i := 0; i < 65536; i++ {
		if isNonLogoLetter(rune(i)) {
			letter[i/8] |= 1 << (i % 8)
		}
	}
	return
}()

var continueBMP1Fast = func() (cont [65536 / 8]byte) {
	// id_continue := set(unicode.IsLetter).
	// 	add(unicode.Nl).
	// 	add(unicode.Other_ID_Start).
	// 	sub(unicode.Pattern_Syntax).
	// 	sub(unicode.Pattern_White_Space).
	// 	add(unicode.Mn).
	// 	add(unicode.Mc).
	// 	add(unicode.Nd).
	// 	add(unicode.Pc).
	// 	add(unicode.Other_ID_Continue).
	// 	sub(unicode.Pattern_Syntax).
	// 	sub(unicode.Pattern_White_Space)
	for i := 0; i < 65536; i++ {
		r := rune(i)
		if unicode.IsLetter(r) ||
			unicode.Is(unicode.Nl, r) ||
			unicode.Is(unicode.Other_ID_Start, r) ||
			unicode.Is(unicode.Mn, r) ||
			unicode.Is(unicode.Mc, r) ||
			unicode.Is(unicode.Nd, r) ||
			unicode.Is(unicode.Pc, r) ||
			unicode.Is(unicode.Other_ID_Continue, r) {

			if !unicode.Is(unicode.Pattern_Syntax, r) && !unicode.Is(unicode.Pattern_White_Space, r) {
				cont[i/8] |= 1 << (i % 8)
			}
		}
	}
	return
}()

var normBMP1Fast = func() (res [65536]uint16) {
	var tmp [64]byte
	var accent = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	for i := 0; i < 65536; i++ {
		n := utf8.EncodeRune(tmp[:], rune(i))
		output, _, e := transform.Append(accent, tmp[n:n], tmp[:n])
		if e != nil {
			res[i] = uint16(unicode.ToLower(rune(i)))
		} else {
			nr, _ := utf8.DecodeRune(output)
			res[i] = uint16(unicode.ToLower(nr))
		}
	}
	return
}()

func normalizeNonLogoLetter(r rune) rune {
	if r < 65536 && letterBMP1Fast[r/8]&(1<<(r%8)) > 0 {
		goto NORM
	}
	if isNonLogoLetter(r) {
		goto NORM
	}
	return 0

NORM:
	if r < 65536 {
		return rune(normBMP1Fast[r])
	}
	return unicode.ToLower(r)
}

func isContinue(r rune) bool {
	if r < 65536 && continueBMP1Fast[r/8]&(1<<(r%8)) > 0 {
		return true
	}
	return 65536 <= r && r < 0xF0000
}

func hashTrigram(a, b, c rune) rune {
	var tmp [12]byte
	utf8.AppendRune(tmp[:0], a)
	utf8.AppendRune(tmp[4:4], b)
	utf8.AppendRune(tmp[8:8], c)
	h := crc32.ChecksumIEEE(tmp[:])
	return rune(h&0xF0FFFF + 0xF0000)
}

func Collect(source string, maxRunes uint16) (map[rune][]byte, bool) {
	m := map[rune][]uint16{}
	full := true

	CollectFunc(source, false, func(i int, off [2]int, r rune, gram []rune) bool {
		if i >= int(maxRunes) {
			full = false
			return false
		}
		m[r] = append(m[r], uint16(i))
		return true
	})

	res := make(map[rune][]byte, len(m))
	for k, v := range m {
		res[k] = array16.Compress(v)
	}
	return res, full
}

func CollectFunc(source string, indexOnly bool, f func(int, [2]int, rune, []rune) bool) {
	if len(source) == 0 {
		return
	}

	var grams []rune
	var offs [][2]int
	var i int

	for off := 0; off < len(source); {
		r, w := utf8.DecodeRuneInString(source[off:])
		if r == utf8.RuneError {
			return
		}

		prevOff := [2]int{off, w}
		off += w

		if !isContinue(r) {
			continue
		}

		grams = grams[:0]

		if r2 := normalizeNonLogoLetter(r); r2 > 0 {
			grams = append(grams[:0], r2)
			offs = append(offs[:0], prevOff)
			for {
				r, w := utf8.DecodeRuneInString(source[off:])
				cr := normalizeNonLogoLetter(r)
				if cr == 0 {
					if r == 0x200D || unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Mc, r) {
						// ZWJ, Mn & Mc after letters should be considered.
						cr = r
					} else {
						break
					}
				}
				offs = append(offs, [2]int{off, w})
				grams = append(grams, cr)
				off += w
			}
			switch len(grams) {
			case 2:
				if indexOnly {
					r = -1
				} else {
					r = rune(hashTrigram(grams[0], grams[1], 0))
				}
				prevOff[1] += offs[1][1] // offs[1].w
			case 1:
				r = grams[0]
				grams = grams[:0]
			default:
				for ii := 0; ii < len(grams)-2; ii++ {
					if indexOnly {
						r2 = -1
					} else {
						r2 = rune(hashTrigram(grams[ii], grams[ii+1], grams[ii+2]))
					}
					if !f(i, [2]int{
						offs[ii][0],
						offs[ii][1] + offs[ii+1][1] + offs[ii+2][1],
					}, r2, grams[ii:ii+3]) {
						return
					}
					i++
				}
				continue
			}
		}

		if !f(i, prevOff, r, grams) {
			return
		}
		i++
	}
}
