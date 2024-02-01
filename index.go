package like

import (
	"encoding/binary"
	"fmt"

	"github.com/coyove/bbolt"
	"github.com/coyove/like/array16"
)

const (
	IndexOrderRandom byte = iota
	IndexOrderIncr
	IndexOrderDecr
)

type IndexDocument struct {
	ID      []byte
	Rescore bool
	Order   byte
	Score   uint32
	Content string
}

func (d IndexDocument) SetID(v []byte) IndexDocument {
	d.ID = v
	return d
}

func (d IndexDocument) SetIntID(v uint64) IndexDocument {
	d.ID = binary.BigEndian.AppendUint64(nil, v)
	return d
}

func (d IndexDocument) SetStringID(v string) IndexDocument {
	d.ID = append(d.ID[:0], v...)
	return d
}

func (db *DB) Index(doc IndexDocument) error {
	return db.BatchIndex([]IndexDocument{doc})
}

func (db *DB) BatchIndex(docs []IndexDocument) error {
	if len(docs) == 0 {
		return nil
	}

	tx, err := db.Store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, doc := range docs {
		chars, _ := Collect(doc.Content, db.MaxChars)
		if len(doc.ID) == 0 {
			return fmt.Errorf("empty document")
		}
		if len(chars) == 0 && !doc.Rescore {
			return fmt.Errorf("empty document")
		}

		if doc.Rescore {
			deleteTx(tx, db.Namespace, doc.ID, "rescore", doc.Score)
			continue
		}

		bkId, index, oldExisted := deleteTx(tx, db.Namespace, doc.ID, "index", 0)
		if err != nil {
			return err
		}

		var tmp []byte

		newScore := binary.BigEndian.AppendUint32(nil, doc.Score)

		var runes1 []uint16
		var runes2 []uint32

		for k, v := range chars {
			k := uint32(k)
			if k < 0xFFFF {
				runes1 = append(runes1, uint16(k))
			} else {
				runes2 = append(runes2, k)
			}
			// if len(v) > 1000 {
			// 	fmt.Println(string(k), len(v), array16.Len(v))
			// }
			tmp = binary.BigEndian.AppendUint32(append(tmp[:0], db.Namespace...), k)
			bk, _ := tx.CreateBucketIfNotExists(tmp)
			bk.SetSequence(bk.Sequence() + 1)
			if !oldExisted { // this is a new document
				switch doc.Order {
				case IndexOrderIncr:
					bk.FillPercent = 0.9
				case IndexOrderDecr:
					bk.FillPercent = 0.1
				}
			}
			bk.Put(AppendSortedUvarint(newScore, index), v)
		}

		payload := AppendSortedUvarint(nil, index)
		payload = binary.BigEndian.AppendUint32(payload, doc.Score)

		buf1 := array16.CompressFull(runes1)
		payload = binary.AppendUvarint(payload, uint64(len(buf1)))
		payload = append(payload, buf1...)
		payload = binary.AppendUvarint(payload, uint64(len(runes2)))
		for _, k := range runes2 {
			k -= 0x10000
			if k > 0xFFFFFF {
				return fmt.Errorf("invalid unicode %x", k+0x10000)
			}
			payload = append(payload, byte(k>>16), byte(k>>8), byte(k))
		}

		bkId.Put(doc.ID, payload)
	}

	return tx.Commit()
}

func (db *DB) Delete(doc IndexDocument) error {
	tx, err := db.Store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	deleteTx(tx, db.Namespace, doc.ID, "delete", 0)
	return tx.Commit()
}

func deleteTx(tx *bbolt.Tx, ns string, id8 []byte, action string, rescore uint32) (*bbolt.Bucket, uint64, bool) {
	bkId, _ := tx.CreateBucketIfNotExists([]byte(ns))
	bkIndex, _ := tx.CreateBucketIfNotExists([]byte(ns + "index"))
	bkIndex.FillPercent = 0.95

	var tmp []byte
	var deletes int
	var oldScore []byte
	var index uint64

	oldPayload := bkId.Get(id8)

	if len(oldPayload) > 0 {
		var w int
		index, w = SortedUvarint(oldPayload)
		oldScore = oldPayload[w : w+4 : w+4]
		oldPayload = oldPayload[w+4:]
	} else {
		if action == "delete" || action == "rescore" {
			return nil, 0, false
		}
		index = bkId.Sequence()
		bkId.SetSequence(bkId.Sequence() + 1)
	}

	if action == "delete" {
		bkIndex.Delete(AppendSortedUvarint(nil, index))
		bkIndex.SetSequence(bkIndex.Sequence() - 1)
		bkId.Delete(id8)
	} else {
		bkIndex.Put(AppendSortedUvarint(nil, index), id8)
		if len(oldPayload) == 0 { // new doc
			bkIndex.SetSequence(bkIndex.Sequence() + 1)
		}
	}

	if len(oldPayload) > 0 {
		work := func(v uint32) {
			tmp = binary.BigEndian.AppendUint32(append(tmp[:0], ns...), v)
			bk := tx.Bucket(tmp)
			if action == "rescore" {
				prev, _ := bk.TestDelete(AppendSortedUvarint(oldScore, index))
				bk.Put(AppendSortedUvarint(binary.BigEndian.AppendUint32(nil, rescore), index), prev)
			} else {
				bk.SetSequence(bk.Sequence() - 1)
				bk.Delete(AppendSortedUvarint(oldScore, index))
			}
			deletes++
		}

		runes1Len, w := binary.Uvarint(oldPayload)
		oldPayload = oldPayload[w:]

		array16.ForeachBlock(oldPayload[:runes1Len], func(r uint16) bool {
			work(uint32(r))
			return true
		})

		oldPayload = oldPayload[runes1Len:]
		runes2Len, w := binary.Uvarint(oldPayload)
		oldPayload = oldPayload[w:]

		for i := 0; i < len(oldPayload[:runes2Len*3]); i += 3 {
			r := uint32(oldPayload[i])<<16 + uint32(oldPayload[i+1])<<8 + uint32(oldPayload[i+2])
			work(r + 0x10000)
		}
	}

	if action == "rescore" {
		old := bkId.Get(id8)
		index, w := SortedUvarint(old)
		buf := AppendSortedUvarint(nil, index)
		buf = binary.BigEndian.AppendUint32(buf, rescore)
		buf = append(buf, old[w+4:]...)
		bkId.Put(id8, buf)
	}

	return bkId, index, len(oldScore) > 0
}

func (db *DB) Count() (int, int, error) {
	tx, err := db.Store.Begin(false)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	bkId := tx.Bucket([]byte(db.Namespace))
	bkIndex := tx.Bucket([]byte(db.Namespace + "index"))
	if bkIndex != nil {
		return int(bkIndex.Sequence()), int(bkId.Sequence()), nil
	}
	return 0, 0, nil
}

func TopK(db *bbolt.DB, ns string, n int) {
	tx, _ := db.Begin(false)
	defer tx.Rollback()

	var h [][2]int32
	c := tx.Cursor()
	for k, _ := c.Seek(append([]byte(ns), 0)); len(k) > 0; k, _ = c.Next() {
		r := rune(binary.BigEndian.Uint32(k[len(ns):]))
		count := int32(tx.Bucket(k).Sequence())
		h = append(h, [2]int32{int32(r), count})
		if len(h) > n {
			j := len(h) - 1
			for {
				i := (j - 1) / 2 // parent
				if i == j || h[j][1] >= h[i][1] {
					break
				}
				h[i], h[j] = h[j], h[i]
				j = i
			}
			h = h[1:]
		}
	}

	for i := range h {
		fmt.Println(string(rune(h[i][0])), h[i][1])
	}
}
