package like

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

type Metrics struct {
	Query          string   `json:"query"`
	Chars          [][]rune `json:"chars,omitempty"`
	CharsEx        [][]rune `json:"chars_exclude,omitempty"`
	CharsOr        [][]rune `json:"chars_or,omitempty"`
	Collected      []string `json:"collected,omitempty"`
	Error          string   `json:"error"`
	Seek           int      `json:"seek"`
	Scan           int      `json:"scan"`
	SwitchHead     int      `json:"switch_head"`
	FastSwitchHead int      `json:"fast_switch_head"`
	Miss           int      `json:"miss"`
}

func (d *Metrics) Collect(term string, maxChars uint16) (parts []rune) {
	CollectFunc(term, func(i int, off [2]int, r rune, gram []rune) bool {
		if i >= int(maxChars) {
			return false
		}
		parts = append(parts, r)
		switch len(gram) {
		case 2, 3:
			d.Collected = append(d.Collected, string(gram))
		default:
			d.Collected = append(d.Collected, string(r))
		}
		return true
	})
	return
}

func (d *Metrics) String() string {
	buf, _ := json.Marshal(d)
	return string(buf)
}

func (d *Metrics) Highlight(content string, left, right string) (out string) {
	if len(d.CharsOr) > 0 {
		d.Chars = append(d.Chars, d.CharsOr...)
	}
	if len(d.Chars) == 0 {
		return content
	}

	type radix struct {
		ch   rune
		end  bool
		next map[rune]*radix
		up   *radix
	}

	root := &radix{next: map[rune]*radix{}}
	for _, m := range d.Chars {
		n := root
		for i, r := range m {
			if n.next == nil {
				n.next = map[rune]*radix{}
			}
			if n.next[r] == nil {
				n.next[r] = &radix{ch: r, up: n}
			}
			n = n.next[r]
			if i == len(m)-1 {
				n.end = true
			}
		}
	}

	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("#FIXME:%v\n#", d.Chars) + content + "\n#FIXME"
		}
	}()

	var spans [][2]int
	var expands [][4]int
	CollectFunc(content, func(_ int, pos [2]int, r rune, grams []rune) bool {
		if len(root.next) == 0 {
			return false
		}
		if root.next[r] == nil {
			return true
		}

		n := root
		ok, start, end := false, pos[0], pos[0]
		CollectFunc(content[pos[0]:], func(_ int, pos2 [2]int, r2 rune, grams2 []rune) bool {
			if n.next[r2] == nil {
				ok = false
				return false
			}
			n = n.next[r2]
			if !n.end {
				return true
			}
			if len(n.next) == 0 {
				for n0 := n.up; n0 != nil; n0 = n0.up {
					delete(n0.next, n.ch)
					if len(n0.next) > 0 {
						break
					}
					n = n0
				}
			}
			end = pos[0] + pos2[0] + pos2[1]
			ok = true
			return false
		})
		if !ok {
			return true
		}

		if end > start {
			if len(spans) > 0 && start < spans[len(spans)-1][1] {
				spans[len(spans)-1][1] = end
				expands[len(expands)-1][1] = end
			} else {
				expands = append(expands, [4]int{start, end, len(spans), len(spans) + 1})
				spans = append(spans, [2]int{start, end})
			}
		}
		return true
	})

	const abbr = 60

	for i := len(expands) - 1; i > 0; i-- {
		if expands[i][0]-expands[i-1][1] < abbr*2+10 {
			expands[i-1][1] = expands[i][1]
			expands[i-1][3] = expands[i][3]
			expands = append(expands[:i], expands[i+1:]...)
		}
	}

	p := bytes.Buffer{}
	eoc := false
	for _, e := range expands {
		start, end := e[0], e[1]
		start0, end0 := e[0], e[1]
		for start-start0 < abbr && start0 > 0 {
			r, w := utf8.DecodeLastRuneInString(content[:start0])
			if r == utf8.RuneError {
				break
			}
			start0 -= w
		}

		for end0-end < abbr && end0 < len(content) {
			r, w := utf8.DecodeRuneInString(content[end0:])
			if r == utf8.RuneError {
				break
			}
			end0 += w
		}

		if start0 > 0 {
			p.WriteString("...")
		}

		for i := e[2]; i < e[3]; i++ {
			p.WriteString(content[start0:spans[i][0]])
			p.WriteString(left)
			p.WriteString(content[spans[i][0]:spans[i][1]])
			p.WriteString(right)
			start0 = spans[i][1]
		}

		p.WriteString(content[start0:end0])
		eoc = eoc || end0 == len(content)
	}

	if !eoc {
		p.WriteString("...")
	}
	return p.String()
}
