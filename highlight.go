package like

import (
	"bytes"
	"encoding/json"
	"sort"
	"unicode"
	"unicode/utf8"

	"github.com/coyove/like/array16"
	"github.com/dop251/scsu"
)

type Metrics struct {
	Chars     []*segchars `json:"chars,omitempty"`
	CharsEx   []*segchars `json:"chars_exclude,omitempty"`
	Collected []string    `json:"collected,omitempty"`
	FuzzyDist uint16      `json:"fuzzy_dist,omitempty"`
	FuzzyMiss int         `json:"fuzzy_miss,omitempty"`

	Deduplicator func(Document) bool `json:"-"`

	Query          string `json:"query"`
	Error          string `json:"error"`
	Seek           int    `json:"seek"`
	Scan           int    `json:"scan"`
	SwitchHead     int    `json:"switch_head"`
	FastSwitchHead int    `json:"fast_switch_head"`
	Miss           int    `json:"miss"`
	Timeout        bool   `json:"timeout,omitempty"`
}

func (d *Metrics) Collect(term string, maxChars uint16) (parts []rune) {
	CollectFunc(term, false, func(i int, off [2]int, r rune, gram []rune) bool {
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

type Highlighter struct {
	Left, Right string
	Expand      int
	Gap         int
}

func (d Document) Highlight(hl *Highlighter) (out string) {
	tx, err := d.db.Store.Begin(false)
	if err != nil {
		return ""
	}
	defer tx.Rollback()

	bk := tx.Bucket([]byte(d.db.Namespace + "content"))
	if bk == nil {
		return ""
	}
	data := bk.Get(d.ID)

	content, _ := scsu.Decode(data)
	if len(d.Segs) == 0 || len(content) == 0 {
		return ""
	}

	segs := d.Segs
	sort.Slice(segs, func(i, j int) bool {
		return segs[i][0] < segs[j][0]
	})
	for i := len(segs) - 1; i > 0; i-- {
		if segs[i][0] <= array16.AddSat(segs[i-1][1], uint16(hl.Gap)) {
			segs[i-1][1] = segs[i][1]
			segs = append(segs[:i], segs[i+1:]...)
		}
	}

	var spans []int
	CollectFunc(content, true, func(i int, pos [2]int, _ rune, grams []rune) bool {
		if len(segs) == 0 {
			return false
		}
		seg := segs[0]
		if i < int(seg[0]) {
			// Continue
		} else if i == int(seg[0]) && seg[0] == seg[1] {
			spans = append(spans, pos[0], pos[0]+pos[1])
			segs = segs[1:]
		} else if i == int(seg[0]) {
			spans = append(spans, pos[0])
		} else if i < int(seg[1]) {
			// Continue
		} else if i == int(seg[1]) {
			spans = append(spans, pos[0]+pos[1])
			segs = segs[1:]
		}
		return true
	})

	if len(spans)%2 != 0 {
		spans = append(spans, len(content))
	}
	if len(spans) == 0 {
		return ""
	}

	p := bytes.Buffer{}
	if spans[0] != 0 {
		p.WriteString("...")
	}

	var prev []rune
	for i := 0; i < len(spans); i += 2 {
		start, end := spans[i], spans[i+1]
		text := content[start:end]

		prev = prev[:0]
		for n := 0; start > 0 && n < hl.Expand; {
			r, w := utf8.DecodeLastRuneInString(content[:start])
			if r == utf8.RuneError {
				break
			}
			if !omitWS(r, p.Bytes()) {
				prev = append(prev, r)
				n++
			}
			start -= w
		}
		for i := len(prev) - 1; i >= 0; i-- {
			p.WriteRune(prev[i])
		}

		p.WriteString(hl.Left)
		p.WriteString(text)
		p.WriteString(hl.Right)

		for n := 0; end < len(content) && n < hl.Expand; {
			r, w := utf8.DecodeRuneInString(content[end:])
			if r == utf8.RuneError {
				break
			}
			if !omitWS(r, p.Bytes()) {
				p.WriteRune(r)
				n++
			}
			end += w
		}

		if end != len(content) {
			p.WriteString("...")
		}
	}

	return p.String()
}

func omitWS(r rune, buf []byte) bool {
	if r == '\n' {
		return true
	}
	if len(buf) > 0 {
		prev, _ := utf8.DecodeLastRune(buf)
		if unicode.IsSpace(prev) && unicode.IsSpace(r) {
			return true
		}
	}
	return false
}
