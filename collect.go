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

type set func(rune) bool

func (a set) add(rt *unicode.RangeTable) set {
	b := in(rt)
	return func(r rune) bool { return a(r) || b(r) }
}

func (a set) sub(rt *unicode.RangeTable) set {
	b := in(rt)
	return func(r rune) bool { return a(r) && !b(r) }
}

func in(rt *unicode.RangeTable) set {
	return func(r rune) bool { return unicode.Is(rt, r) }
}

var id_continue = set(unicode.IsLetter).
	add(unicode.Nl).
	add(unicode.Other_ID_Start).
	sub(unicode.Pattern_Syntax).
	sub(unicode.Pattern_White_Space).
	add(unicode.Mn).
	add(unicode.Mc).
	add(unicode.Nd).
	add(unicode.Pc).
	add(unicode.Other_ID_Continue).
	sub(unicode.Pattern_Syntax).
	sub(unicode.Pattern_White_Space)

var continueBMP1Fast, letterBMP1Fast = func() (cont, letter [65536 / 8]byte) {
	for i := 0; i < 65536; i++ {
		if id_continue(rune(i)) {
			cont[i/8] |= 1 << (i % 8)
		}
		if isLetterNotCJK(rune(i)) {
			letter[i/8] |= 1 << (i % 8)
		}
	}
	return
}()

var accent = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)

func normalize(r rune) rune {
	var tmp [32]byte
	n := utf8.EncodeRune(tmp[:], r)
	output, _, e := transform.Append(accent, tmp[16:16], tmp[:n])
	if e != nil {
		return unicode.ToLower(r)
	}
	nr, _ := utf8.DecodeRune(output)
	return unicode.ToLower(nr)
}

func isLetterNotCJK(r rune) bool {
	if unicode.IsNumber(r) {
		return true
	}
	return unicode.IsLetter(r) && !unicode.Is(unicode.Lo, r)
}

func convertLetter(r rune) rune {
	if r < 65536 && letterBMP1Fast[r/8]&(1<<(r%8)) > 0 {
		return normalize(r)
	}
	if isLetterNotCJK(r) {
		return normalize(r)
	}
	return 0
}

func isContinue(r rune) bool {
	if r < 65536 && continueBMP1Fast[r/8]&(1<<(r%8)) > 0 {
		return true
	}
	return r < 0xE0000
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

	CollectFunc(source, func(i int, off [2]int, r rune, gram []rune) bool {
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

func CollectChars(source string, maxRunes uint16) (res []rune) {
	CollectFunc(source, func(i int, off [2]int, r rune, gram []rune) bool {
		if i >= int(maxRunes) {
			return false
		}
		res = append(res, r)
		return true
	})
	return
}

func CollectFunc(source string, f func(int, [2]int, rune, []rune) bool) {
	if len(source) == 0 {
		return
	}

	var grams []rune
	var offs [][2]int
	var i int

	for off := 0; off < len(source); {
		r, w := utf8.DecodeRuneInString(source[off:])
		prevOff := [2]int{off, w}
		off += w

		if !isContinue(r) {
			continue
		}

		grams = grams[:0]

		if r2 := convertLetter(r); r2 > 0 {
			grams = append(grams[:0], r2)
			offs = append(offs[:0], prevOff)
			for {
				r, w := utf8.DecodeRuneInString(source[off:])
				r = convertLetter(r)
				if r == 0 {
					break
				}
				offs = append(offs, [2]int{off, w})
				grams = append(grams, r)
				off += w
			}
			switch len(grams) {
			case 2:
				r = rune(hashTrigram(grams[0], grams[1], 0))
				prevOff[1] += offs[1][1] // offs[1].w
			case 1:
				r = grams[0]
				grams = grams[:0]
			default:
				for ii := 0; ii < len(grams)-2; ii++ {
					r2 = rune(hashTrigram(grams[ii], grams[ii+1], grams[ii+2]))
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
