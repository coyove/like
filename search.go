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
		metrics.Error = err.Error()
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
