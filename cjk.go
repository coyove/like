package like

import "unicode"

/*
U+2E80 - U+2EFF	CJK Radicals Supplement	115
U+2F00 - U+2FDF	Kangxi Radicals	214
U+2FF0 - U+2FFF	Ideographic Description Characters	12
U+3000 - U+303F	CJK Symbols and Punctuation	64
U+3040 - U+309F	Hiragana	93
U+30A0 - U+30FF	Katakana	96
U+3100 - U+312F	Bopomofo	43
U+3130 - U+318F	Hangul Compatibility Jamo	94
U+3190 - U+319F	Kanbun	16
U+31A0 - U+31BF	Bopomofo Extended	32
U+31C0 - U+31EF	CJK Strokes	36
U+31F0 - U+31FF	Katakana Phonetic Extensions	16
U+3200 - U+32FF	Enclosed CJK Letters and Months	255
U+3300 - U+33FF	CJK Compatibility	256
U+3400 - U+4DBF	CJK Unified Ideographs Extension A	6,592
U+4DC0 - U+4DFF	Yijing Hexagram Symbols	64
U+4E00 - U+9FFF	CJK Unified Ideographs	20,989
U+A000 - U+A48F	Yi Syllables	1,165
U+A490 - U+A4CF	Yi Radicals	55

U+A500 - U+A63F	Vai	300

U+AC00 - U+D7AF	Hangul Syllables	11,172
U+D7B0 - U+D7FF	Hangul Jamo Extended-B	72

U+F900 - U+FAFF	CJK Compatibility Ideographs	472
*/

func isNonLogoLetter(r rune) bool {
	if unicode.IsNumber(r) {
		return true
	}

	if r >= 65536 {
		return unicode.IsLetter(r) && !unicode.Is(unicode.Lo, r)
	}

	switch {
	case 0x2E80 <= r && r <= 0xA4CF,
		0xA500 <= r && r <= 0xA63F,
		0xAC00 <= r && r <= 0xD7FF,
		0xF900 <= r && r <= 0xFAFF:
		return false
	}
	return unicode.IsLetter(r)
}
