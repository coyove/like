package like

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/coyove/bbolt"
	"github.com/coyove/like/array16"
)

type Document struct {
	Index uint64
	ID    []byte
	Score uint32
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
	p += fmt.Sprintf(", %d", d.Score)
	return p + ")"
}

func (d SearchMetrics) Highlight(content string, left, right string) (out string) {
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

type cursor struct {
	*bbolt.Cursor
	key, value []byte
}

type SearchMetrics struct {
	Chars          [][]rune
	Err            error
	Seek           int
	Scan           int
	SwitchHead     int
	FastSwitchHead int
	Miss           int
}

func (db *DB) Search(query string, start []byte, n int, metrics *SearchMetrics) (res []Document, next []byte) {
	var chars []rune
	var segs [][2]int
	for query = strings.TrimSpace(query); len(query) > 0; {
		var term string
		if query[0] == '"' {
			if q := strings.IndexByte(query[1:], '"'); q > -1 {
				term = query[1 : 1+q]
				query = query[1+q+1:]
				goto OK
			}
		}
		if i := strings.IndexFunc(query, unicode.IsSpace); i == -1 {
			term, query = query, ""
		} else {
			term, query = query[:i], strings.TrimSpace(query[i+1:])
		}
	OK:
		old := len(chars)
		left := int(db.MaxChars) - old
		if left < 0 {
			break
		}
		chars = append(chars, CollectChars(term, uint16(left))...)
		if len(chars) > old {
			segs = append(segs, [2]int{old, len(chars) - old})
			metrics.Chars = append(metrics.Chars, chars[old:])
		}
	}
	if len(chars) == 0 {
		return
	}

	cursors := make([]*cursor, len(chars))

	tx, err := db.Store.Begin(false)
	if err != nil {
		metrics.Err = err
		return
	}
	defer tx.Rollback()

	bkId := tx.Bucket([]byte(db.Namespace))
	bkIndex := tx.Bucket([]byte(db.Namespace + "index"))
	if bkId == nil || bkIndex == nil {
		return
	}

	for i, r := range chars {
		bk := tx.Bucket(binary.BigEndian.AppendUint32([]byte(db.Namespace), uint32(r)))
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

			// Try fast path to avoid seek
			var same bool
		FAST:
			if cur.key, cur.value, same = cur.PrevSamePage(); same {
				switch bytes.Compare(cur.key, head.key) {
				case 1:
					goto FAST
				case 0:
					continue
				case -1:
					head = cur
					metrics.FastSwitchHead++
					continue SWITCH_HEAD
				}
			}

			metrics.Seek++

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
			metrics.SwitchHead++
			continue SWITCH_HEAD
		}

		// 	for i, c := range cursors {
		// 		array16.Foreach(c.value, func(pos uint16) bool {
		// 			fmt.Println(string(chars[i]), pos)
		// 			return true
		// 		})
		// 	}
		metrics.Scan++

		match := false
		for _, seg := range segs {
			start, length := seg[0], seg[1]
			array16.Foreach(cursors[start].value, func(pos uint16) bool {
				match = false
				for i := start + 1; i < start+length; i++ {
					if array16.Contains(cursors[i].value, uint16(i-start)+pos) == -1 {
						metrics.Miss++
						return true
					}
				}
				match = true
				return false
			})
			if !match {
				break
			}
		}

		if match {
			if len(res) >= n {
				return res[:n], append([]byte(nil), cursors[0].key...)
			}

			indexBuf := cursors[0].key[4:]
			index, _ := SortedUvarint(indexBuf)
			score := binary.BigEndian.Uint32(cursors[0].key[:4])
			docId := bkIndex.Get(indexBuf)

			res = append(res, Document{
				Index: index,
				ID:    append([]byte(nil), docId...),
				Score: score,
			})
		}

		head.key, head.value = head.Prev()
	}
	return
}
