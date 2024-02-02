package like

import (
	"bytes"
	"encoding/binary"
	"sort"
	"strings"
	"unicode"

	"github.com/coyove/bbolt"
	"github.com/coyove/like/array16"
)

type segchars struct {
	chars []rune
	segs  [][2]int
}

type cursor struct {
	*bbolt.Cursor
	key, value []byte
	index      int
}

func (db *DB) Search(query string, start []byte, n int, metrics *Metrics) (res []Document, next []byte) {
	if metrics == nil {
		metrics = &Metrics{}
	}
	metrics.Query = query

	var sc segchars
	var charsEx []segchars

	for query = strings.TrimSpace(query); len(query) > 0; {
		var exclude bool
		if query[0] == '-' {
			exclude = true
			query = query[1:]
		}

		var term string
		if q := strings.IndexByte(query[1:], '"'); q > -1 && query[0] == '"' {
			// Quoted term.
			term, query = query[1:1+q], query[1+q+1:]
		} else if i := strings.IndexFunc(query, unicode.IsSpace); i == -1 {
			// End of query.
			term, query = query, ""
		} else {
			// Normal term.
			term, query = query[:i], strings.TrimSpace(query[i+1:])
		}

		if terms := strings.Split(term, "|"); len(terms) > 1 {
			if exclude {
				metrics.Error = "mixed '|' and '-' operators"
				return
			}
			if len(metrics.CharsOr) > 0 {
				metrics.Error = "multiple '|' operators"
				return
			}

			for _, term := range terms {
				parts := metrics.Collect(term, db.MaxChars)
				if len(parts) > 0 {
					metrics.CharsOr = append(metrics.CharsOr, parts)
				}
			}
			if len(metrics.CharsOr) <= 1 {
				metrics.CharsOr = metrics.CharsOr[:0]
			}
		} else {
			parts := metrics.Collect(term, db.MaxChars)
			if len(parts) == 0 {
				continue
			}

			if exclude {
				charsEx = append(charsEx, segchars{parts, [][2]int{{0, len(parts)}}})
				metrics.CharsEx = append(metrics.CharsEx, parts)
			} else {
				sc.segs = append(sc.segs, [2]int{len(sc.chars), len(parts)})
				sc.chars = append(sc.chars, parts...)
				metrics.Chars = append(metrics.Chars, parts)
			}
		}
	}

	if len(metrics.CharsOr) == 0 {
		if len(sc.chars) == 0 {
			return
		}

		return db.oneSearch(sc, charsEx, start, n, metrics)
	}

	// return db.multiSearch(sc, charsEx, start, n, metrics)
	// 	for {
	// 		docs, next := db.multiSearch(sc, charsEx, start, n, metrics)
	// 		res = append(res, docs...)
	// 		if len(res) < n {
	// 			if len(next) == 0 {
	// 				return res, next
	// 			}
	// 			start = next
	// 			continue
	// 		}
	// 		if len(res) > n {
	// 			next = res[n].boundKey(nil)
	// 			res = res[:n]
	// 		}
	// 		return res, next
	// 	}
	// }
	//
	// func (db *DB) multiSearch(sc segchars, charsEx []segchars, start []byte, n int, metrics *Metrics) (docs []Document, next []byte) {
	var docs []Document
	for _, or := range metrics.CharsOr {
		resA, nextA := db.oneSearch(segchars{
			append(sc.chars, or...),
			append(sc.segs, [2]int{len(sc.chars), len(or)}),
		}, charsEx, start, n, metrics)

		if bytes.Compare(nextA, next) > 0 {
			next = nextA
		}
		docs = append(docs, resA...)
	}

	sort.Slice(docs, func(i, j int) bool {
		if docs[i].Score == docs[j].Score {
			return docs[i].Index > docs[j].Index
		}
		return docs[i].Score > docs[j].Score
	})
	for i := len(docs) - 1; i > 0; i-- {
		if bytes.Equal(docs[i].ID, docs[i-1].ID) {
			docs = append(docs[:i], docs[i+1:]...)
		}
	}

	if len(docs) > n {
		tmp := docs[n].boundKey(nil)
		if bytes.Compare(tmp, next) > 0 {
			next = tmp
		}
		docs = docs[:n]
	}
	return docs, next
}

func (db *DB) oneSearch(sc segchars, charsEx []segchars, start []byte, n int, metrics *Metrics) (res []Document, next []byte) {
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
	db.marchSearch(tx, sc, start, metrics, func(key []byte) bool {
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
			boundKey := res[len(res)-1].boundKey(nil)
			db.marchSearch(tx, ex, start, metrics, func(key []byte) bool {
				if bytes.Compare(key, boundKey) < 0 {
					return false
				}
				indexBuf := key[4:]
				index, _ := SortedUvarint(indexBuf)
				for i := range res {
					if res[i].Index == index {
						res = append(res[:i], res[i+1:]...)
						break
					}
				}
				if len(res) == 0 {
					return false
				}
				return true
			})
		}

		if len(res) < n && len(next) > 0 {
			start = next
			next = nil
			goto MORE
		}
	}

	return
}

func (db *DB) marchSearch(tx *bbolt.Tx, sc segchars, start []byte, metrics *Metrics, f func([]byte) bool) {
	cursors := make([]*cursor, len(sc.chars))

	for i, r := range sc.chars {
		bk := tx.Bucket(binary.BigEndian.AppendUint32([]byte(db.Namespace), uint32(r)))
		if bk == nil {
			return
		}
		c := bk.Cursor()
		k, v := c.Last()
		if len(start) > 0 {
			k, v = c.Seek(start)
		}
		cursors[i] = &cursor{c, k, v, 0}
	}

	for _, seg := range sc.segs {
		for i := seg[0]; i < seg[0]+seg[1]; i++ {
			cursors[i].index = i - seg[0]
		}
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
		for _, seg := range sc.segs {
			start, length := seg[0], seg[1]

			cc := cursors[start : start+length]
			sort.Slice(cc, func(i, j int) bool {
				return len(cc[i].value) < len(cc[j].value)
			})

			array16.Foreach(cc[0].value, func(pos uint16) bool {
				match = false
				for i := 1; i < len(cc); i++ {
					_, ok := array16.Contains(cc[i].value, uint16(int(pos)-cc[0].index+cc[i].index))
					if !ok {
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

		// === NOTE: cursors[*].value has been invalidated, don't use ===

		if match {
			if !f(cursors[0].key) {
				return
			}
		}

		head.key, head.value = head.Prev()
	}
	return
}
