package like

import (
	"bytes"
	"encoding/binary"
	"strings"
	"time"
	"unicode"

	"github.com/coyove/bbolt"
	"github.com/coyove/like/array16"
)

type segchars struct {
	Chars []rune `json:"chars"`
	Fuzzy bool   `json:"fuzzy"`

	cursors []*cursor
}

type cursor struct {
	*bbolt.Cursor
	key, value []byte
}

func (db *DB) Search(query string, start []byte, n int, metrics *Metrics) (res []Document, next []byte) {
	metrics.Query = query

	var chars, charsEx []*segchars

	query = strings.TrimSpace(query)
	for len(query) > 0 {
		var exclude bool
		if query[0] == '-' {
			exclude = true
			query = query[1:]
		}

		var term string
		var fuzzy bool = true
		if q := strings.IndexByte(query[1:], '"'); q > -1 && query[0] == '"' {
			// Quoted term.
			term, query = query[1:1+q], query[1+q+1:]
			fuzzy = false
		} else if i := strings.IndexFunc(query, unicode.IsSpace); i == -1 {
			// End of query.
			term, query = query, ""
		} else {
			// Normal term.
			term, query = query[:i], strings.TrimSpace(query[i+1:])
		}

		parts := metrics.Collect(term, db.MaxChars)
		if len(parts) == 0 {
			continue
		}

		if exclude {
			s := &segchars{Chars: parts}
			charsEx = append(charsEx, s)
			metrics.CharsEx = append(metrics.CharsEx, s)
		} else {
			s := &segchars{Chars: parts, Fuzzy: fuzzy}
			chars = append(chars, s)
			metrics.Chars = append(metrics.Chars, s)
		}
	}

	if len(chars) == 0 {
		chars = []*segchars{{Chars: []rune{0}}}
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

	var ddl int64
	if db.SearchTimeout > 0 {
		ddl = time.Now().Add(db.SearchTimeout).UnixNano()
	}

MORE:
	db.marchSearch(tx, chars, start, metrics, func(key []byte, segs [][2]uint16) bool {
		if len(res) >= n {
			res = res[:n]
			next = append([]byte(nil), key...)
			return false
		}

		indexBuf := key[4:]
		index, _ := SortedUvarint(indexBuf)
		score := binary.BigEndian.Uint32(key[:4])
		docId := bkIndex.Get(indexBuf)

		doc := Document{
			Index: index,
			ID:    append([]byte(nil), docId...),
			Score: score,
			Segs:  append([][2]uint16(nil), segs...),
			db:    db,
		}
		if metrics.Deduplicator == nil || !metrics.Deduplicator(doc) {
			res = append(res, doc)
		}
		// fmt.Println(res)
		return true
	}, ddl)

	if len(res) > 0 && len(charsEx) > 0 {
		// fmt.Println("=========== START ", sc.chars, charsEx, start, res)
		for i := range charsEx {
			if len(res) == 0 {
				break
			}
			boundKey := res[len(res)-1].boundKey(nil)
			db.marchSearch(tx, charsEx[i:i+1], start, metrics, func(key []byte, _ [][2]uint16) bool {
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
	}

	if len(res) < n && len(next) > 0 && !metrics.Timeout {
		start = next
		next = nil
		goto MORE
	}

	return
}

func (db *DB) marchSearch(tx *bbolt.Tx, chars []*segchars, start []byte, metrics *Metrics, f func([]byte, [][2]uint16) bool, ddl int64) {
	var cursors []*cursor

	for _, sc := range chars {
		sc.cursors = sc.cursors[:0]
		for _, r := range sc.Chars {
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
			cur := &cursor{c, k, v}
			cursors = append(cursors, cur)
			sc.cursors = append(sc.cursors, cur)
		}
	}

	if len(chars) == 1 && len(chars[0].Chars) == 1 && chars[0].Chars[0] == 0 {
		c := cursors[0]
		for len(c.key) > 0 {
			metrics.Scan++
			if !f(c.key, nil) {
				return
			}
			c.key, c.value = c.Prev()
		}
		return
	}

	var slowNow int
	var segs [][2]uint16

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
		segs = segs[:0]
		for _, seg := range chars {
			cc := seg.cursors
			missThreshold := metrics.FuzzyMiss
			dist := metrics.FuzzyDist
			if missThreshold >= len(cc)/2 {
				missThreshold = len(cc) / 2
			}
			if len(cc) <= 4 {
				missThreshold = 0
			}
			if !seg.Fuzzy {
				missThreshold = 0
				dist = 0
			}

			array16.Foreach(cc[0].value, func(pos uint16) bool {
				return true
			})

			array16.Foreach(cc[0].value, func(pos uint16) bool {
				misses := 0
				match = false
				minPos, maxPos := pos, array16.AddSat(pos, uint16(len(cc))-1)
				for i := 1; i < len(cc); i++ {
					pos := array16.AddSat(pos, uint16(i))
					realPos, ok := array16.Contains(cc[i].value, array16.SubSat(pos, dist), array16.AddSat(pos, dist))
					if !ok {
						misses++
						if misses > missThreshold {
							return true
						}
						continue
					}
					if realPos > maxPos {
						maxPos = realPos
					}
					if realPos < minPos {
						minPos = realPos
					}
				}

				// Found
				match = true
				segs = append(segs, [2]uint16{minPos, maxPos})
				return false
			})

			if !match {
				metrics.Miss++
				break
			}
		}

		// === NOTE: cursors[*].value has been invalidated, don't use ===

		if match {
			if !f(cursors[0].key, segs) {
				break
			}
		}

		head.key, head.value = head.Prev()
	}

	return
}
