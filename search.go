package like

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode"
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

type cursor struct {
	*bbolt.Cursor
	key, value []byte
}

func (db *DB) Search(query string, start []byte, n int, metrics *SearchMetrics) (res []Document, next []byte) {
	if metrics == nil {
		metrics = &SearchMetrics{}
	}
	metrics.Query = query

	var chars []rune
	var charsEx [][]rune
	var segs [][2]int

	for query = strings.TrimSpace(query); len(query) > 0; {
		var term string
		var exclude bool
		if query[0] == '-' {
			exclude = true
			query = query[1:]
		}
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
		if len(term) == 0 {
			continue
		}

		// Start collecting chars.
		var parts []rune
		CollectFunc(term, func(i int, off [2]int, r rune, gram []rune) bool {
			if i >= int(db.MaxChars) {
				return false
			}
			parts = append(parts, r)
			switch len(gram) {
			case 2, 3:
				metrics.Collected = append(metrics.Collected, string(gram))
			default:
				metrics.Collected = append(metrics.Collected, string(r))
			}
			return true
		})

		if len(parts) == 0 {
			continue
		}

		if exclude {
			charsEx = append(charsEx, parts)
			metrics.CharsEx = append(metrics.CharsEx, parts)
		} else {
			segs = append(segs, [2]int{len(chars), len(parts)})
			chars = append(chars, parts...)
			metrics.Chars = append(metrics.Chars, parts)
		}
	}

	if len(chars) == 0 {
		return
	}

	tx, err := db.Store.Begin(false)
	if err != nil {
		metrics.Error = err.Error()
		return
	}
	defer tx.Rollback()

	bkIndex := tx.Bucket([]byte(db.Namespace + "index"))
	if bkIndex == nil {
		return
	}

MORE:
	db.marchSearch(tx, chars, segs, start, n, metrics, func(key []byte) bool {
		if len(res) >= n {
			res = res[:n]
			next = append([]byte(nil), key...)
			return false
		}

		indexBuf := key[4:]
		index, _ := SortedUvarint(indexBuf)
		score := binary.BigEndian.Uint32(key[:4])
		docId := bkIndex.Get(indexBuf)

		res = append(res, Document{
			Index: index,
			ID:    append([]byte(nil), docId...),
			Score: score,
		})
		return true
	})

	if len(res) > 0 && len(charsEx) > 0 {
		for _, ex := range charsEx {
			if len(res) == 0 {
				break
			}
			last := res[len(res)-1]
			boundKey := AppendSortedUvarint(binary.BigEndian.AppendUint32(nil, last.Score), last.Index)
			db.marchSearch(tx, ex, [][2]int{{0, len(ex)}}, start, n, metrics, func(key []byte) bool {
				if bytes.Compare(key, boundKey) < 0 {
					return false
				}
				indexBuf := key[4:]
				index, _ := SortedUvarint(indexBuf)
				for i := len(res) - 1; i >= 0; i-- {
					if res[i].Index == index {
						res = append(res[:i], res[i+1:]...)
						break
					}
				}
				return true
			})
		}

		if metrics.Query == "-0" {
			fmt.Println(111, res)
		}

		if len(res) < n && len(next) > 0 {
			start = next
			next = nil
			goto MORE
		}
	}

	return
}

func (db *DB) marchSearch(tx *bbolt.Tx, chars []rune, segs [][2]int,
	start []byte, n int, metrics *SearchMetrics, f func([]byte) bool) {
	cursors := make([]*cursor, len(chars))

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
			cmp := bytes.Compare(cur.key, head.key)
			if cmp == 0 {
				continue
			}

			if cmp < 0 {
				head = cur
				metrics.FastSwitchHead++
				continue SWITCH_HEAD
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

			cmp = bytes.Compare(cur.key, head.key)
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
			if !f(cursors[0].key) {
				return
			}
		}

		head.key, head.value = head.Prev()
	}
	return
}
