package like

import (
	"bytes"
	"encoding/binary"
	"sort"
	"strings"
	"time"
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
}

func (db *DB) Search(query string, start []byte, n int, metrics *Metrics) (res []Document, next []byte) {
	if metrics == nil {
		metrics = &Metrics{}
	}
	metrics.Query = query

	var sc segchars
	var charsEx []segchars

	query = strings.TrimSpace(query)
	for len(query) > 0 {
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

	if len(sc.chars) == 0 {
		sc.chars = []rune{0}
		sc.segs = [][2]int{{0, 1}}
	}

	if len(metrics.CharsOr) == 0 {
		if len(sc.chars) == 0 {
			return
		}

		return db.oneSearch(sc, charsEx, start, n, metrics)
	}

	var dm DocumentsMerger
	for _, or := range metrics.CharsOr {
		dm.Merge(db.oneSearch(segchars{
			append(sc.chars, or...),
			append(sc.segs, [2]int{len(sc.chars), len(or)}),
		}, charsEx, start, n, metrics))
	}
	return dm.Peek(n)
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

	var ddl int64
	if db.SearchTimeout > 0 {
		ddl = time.Now().Add(db.SearchTimeout).UnixNano()
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
	}, ddl)

	if len(res) > 0 && len(charsEx) > 0 {
		// fmt.Println("=========== START ", sc.chars, charsEx, start, res)
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
			}, ddl)
		}

		// fmt.Println("=========== END ", res, n, next)
		if len(res) < n && len(next) > 0 {
			start = next
			next = nil
			goto MORE
		}
	}

	return
}

func (db *DB) marchSearch(tx *bbolt.Tx, sc segchars, start []byte, metrics *Metrics, f func([]byte) bool, ddl int64) {
	cursors := make([]*cursor, len(sc.chars))

	for i, r := range sc.chars {
		bk := tx.Bucket(binary.BigEndian.AppendUint32([]byte(db.Namespace), uint32(r)))
		if bk == nil {
			return
		}
		c := bk.Cursor()
		var k, v []byte
		if len(start) > 0 {
			k, v = c.Seek(start)
			if len(k) == 0 {
				k, v = c.Last()
			} else if bytes.Compare(k, start) > 0 {
				k, v = c.Prev()
			}
		} else {
			k, v = c.Last()
		}
		cursors[i] = &cursor{c, k, v}
		if i == 0 || int(c.Bucket().Sequence()) < metrics.EstimatedCount {
			metrics.EstimatedCount = int(c.Bucket().Sequence())
		}
	}

	if len(sc.chars) == 1 && sc.chars[0] == 0 {
		c := cursors[0]
		for len(c.key) > 0 {
			metrics.Scan++
			if !f(c.key) {
				return
			}
			c.key, c.value = c.Prev()
		}
		metrics.EstimatedCount = int(c.Bucket().Sequence())
		return
	}

	var matches, slowNow int

SWITCH_HEAD:
	for head := cursors[0]; len(head.key) > 0; {
		if ddl > 0 {
			slowNow++
			if slowNow%100 == 0 && time.Now().UnixNano() > ddl {
				metrics.Timeout = true
				break
			}
		}

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

			array16.Foreach(cc[0].value, func(pos uint16) bool {
				match = false
				for i := 1; i < len(cc); i++ {
					_, ok := array16.Contains(cc[i].value, uint16(int(pos)+i))
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
				break
			}
			matches++
		}

		head.key, head.value = head.Prev()
	}

	if metrics.EstimatedCount == -1 {
		metrics.EstimatedCount = matches
	}
	return
}

type DocumentsMerger struct {
	docs []Document
	next []byte
}

func (dm *DocumentsMerger) Merge(docs []Document, next []byte) {
	if bytes.Compare(next, dm.next) > 0 {
		dm.next = next
	}
	dm.docs = append(dm.docs, docs...)
}

func (dm *DocumentsMerger) Peek(n int) ([]Document, []byte) {
	docs := dm.docs
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
		if bytes.Compare(tmp, dm.next) > 0 {
			dm.next = tmp
		}
		docs = docs[:n]
	}
	return docs, dm.next
}
