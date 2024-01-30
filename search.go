package like

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/coyove/bbolt"
	"github.com/coyove/like/array16"
)

type matchspan struct {
	index int
	last  rune
}

type Document struct {
	Index   uint64
	ID      []byte
	Score   uint32
	Content string
	matchAt []matchspan
}

func (d Document) IntID() (v uint64) {
	if len(d.ID) == 8 {
		return binary.BigEndian.Uint64(d.ID)
	}
	return 0
}

func (d Document) StringID() (v string) {
	return *(*string)(unsafe.Pointer(&d.ID))
}

func (d Document) SetID(v []byte) Document {
	d.ID = v
	return d
}

func (d Document) SetIntID(v uint64) Document {
	d.ID = binary.BigEndian.AppendUint64(nil, v)
	return d
}

func (d Document) SetStringID(v string) Document {
	d.ID = append(d.ID[:0], v...)
	return d
}

func (d Document) String() string {
	p := "Document(#%d, %q"
	for _, r := range *(*string)(unsafe.Pointer(&d.ID)) {
		if !unicode.IsPrint(r) {
			p = "Document(#%d, %x"
			break
		}
	}
	p = fmt.Sprintf(p, d.Index, d.ID)
	p += fmt.Sprintf(", %d, %db", d.Score, len(d.Content))
	if len(d.matchAt) > 0 {
		p += fmt.Sprintf(", m=%v", d.matchAt)
	}
	return p + ")"
}

func (d Document) Highlight(class string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("#FIXME:%v\n#", d.String()) + d.Content + "\n#FIXME"
		}
	}()

	if len(d.matchAt) == 0 {
		return ""
	}

	sort.Slice(d.matchAt, func(i, j int) bool {
		return d.matchAt[i].index < d.matchAt[j].index
	})

	var spans [][2]int
	var expands [][4]int
	CollectFunc(d.Content, func(i int, pos [2]int, r rune, grams []rune) bool {
		if i == d.matchAt[0].index {
			start, end := pos[0], pos[0]
			CollectFunc(d.Content[pos[0]:], func(_ int, pos2 [2]int, r2 rune, grams2 []rune) bool {
				if r2 == d.matchAt[0].last {
					end = pos[0] + pos2[0] + pos2[1]
					return false
				}
				return true
			})

			if end > start {
				expands = append(expands, [4]int{start, end, len(spans), len(spans) + 1})
				spans = append(spans, [2]int{start, end})
			}
			d.matchAt = d.matchAt[1:]
		}
		return len(d.matchAt) > 0
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
	for _, e := range expands {
		start, end := e[0], e[1]
		start0, end0 := e[0], e[1]
		for start-start0 < abbr && start0 > 0 {
			r, w := utf8.DecodeLastRuneInString(d.Content[:start0])
			if r == utf8.RuneError {
				break
			}
			start0 -= w
		}

		for end0-end < abbr && end0 < len(d.Content) {
			r, w := utf8.DecodeRuneInString(d.Content[end0:])
			if r == utf8.RuneError {
				break
			}
			end0 += w
		}

		p.WriteString("...")

		for i := e[2]; i < e[3]; i++ {
			p.WriteString(d.Content[start0:spans[i][0]])
			p.WriteString("<span class='" + class + "'>")
			p.WriteString(d.Content[spans[i][0]:spans[i][1]])
			p.WriteString("</span>")
			start0 = spans[i][1]
		}

		p.WriteString(d.Content[start0:end0])
	}

	p.WriteString("...")
	return p.String()
}

type cursor struct {
	*bbolt.Cursor
	key, value []byte
}

func Search(db *bbolt.DB, ns, query string, start []byte, n int) (res []Document, next []byte, err error) {
	var chars []rune
	var segs [][2]int
	for len(query) > 0 {
		var term string
		if i := strings.IndexFunc(query, unicode.IsSpace); i == -1 {
			term, query = query, ""
		} else {
			term, query = query[:i], strings.TrimSpace(query[i+1:])
		}
		old := len(chars)
		chars = append(chars, CollectChars(term, 100)...)
		if len(chars) > old {
			segs = append(segs, [2]int{old, len(chars) - old})
		}
	}
	if len(chars) == 0 {
		return
	}

	cursors := make([]*cursor, len(chars))

	tx, err := db.Begin(false)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	bkId := tx.Bucket([]byte(ns))
	bkIndex := tx.Bucket([]byte(ns + "index"))
	if bkId == nil || bkIndex == nil {
		return
	}

	for i, r := range chars {
		bk := tx.Bucket(binary.BigEndian.AppendUint32([]byte(ns), uint32(r)))
		if bk == nil {
			return
		}
		c := bk.Cursor()
		k, v := c.Last()
		if len(start) > 0 {
			k, v = c.Seek(start)
		}
		cursors[i] = &cursor{c, k, v}
	}

SWITCH_HEAD:
	for head := cursors[0]; len(head.key) > 0; {
		for _, cur := range cursors {
			if bytes.Equal(head.key, cur.key) {
				continue
			}

			cur.key, cur.value = cur.Seek(head.key)
			if len(cur.key) == 0 {
				cur.key, cur.value = cur.Last()
			}

			cmp := bytes.Compare(cur.key, head.key)
			if cmp == 1 {
				cur.key, cur.value = cur.Prev()
				if len(cur.key) == 0 {
					break SWITCH_HEAD
				}
				cmp = bytes.Compare(cur.key, head.key)
			}

			if cmp == 0 {
				continue
			}

			head = cur
			continue SWITCH_HEAD
		}

		// 	for i, c := range cursors {
		// 		array16.Foreach(c.value, func(pos uint16) bool {
		// 			fmt.Println(string(chars[i]), pos)
		// 			return true
		// 		})
		// 	}

		var matchAt []matchspan
		var miss int
		for _, seg := range segs {
			start, length := seg[0], seg[1]
			m0 := -1
			array16.Foreach(cursors[start].value, func(pos uint16) bool {
				m0 = -1
				for i := start + 1; i < start+length; i++ {
					if array16.Contains(cursors[i].value, uint16(i-start)+pos) == -1 {
						miss++
						return true
					}
				}
				m0 = int(pos)
				return false
			})
			if m0 == -1 {
				matchAt = nil
				break
			}
			matchAt = append(matchAt, matchspan{index: m0, last: chars[start+length-1]})
		}

		if len(matchAt) > 0 {
			if len(res) >= n {
				return res[:n], append([]byte(nil), cursors[0].key...), nil
			}

			indexBuf := cursors[0].key[4:]
			index, _ := SortedUvarint(indexBuf)
			score := binary.BigEndian.Uint32(cursors[0].key[:4])
			docId := bkIndex.Get(indexBuf)

			content := bkId.Get(docId)
			if len(content) > 0 {
				_, w := SortedUvarint(content)
				content = content[w+4:]

				runes1Len, w := binary.Uvarint(content)
				content = content[w:]
				content = content[runes1Len:]

				runes2Len, w := binary.Uvarint(content)
				content = content[w:]
				content = content[runes2Len*3:]
			}

			res = append(res, Document{
				Index:   index,
				ID:      append([]byte(nil), docId...),
				Score:   score,
				Content: string(content),
				matchAt: matchAt,
			})
		}

		head.key, head.value = head.Prev()
	}
	return
}
