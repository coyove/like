package like

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/coyove/bbolt"
	"github.com/coyove/like/array16"
	"github.com/dop251/scsu"
)

type IndexDocument struct {
	ID      []byte
	Rescore bool
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

func (d IndexDocument) String() string {
	if d.Rescore {
		return fmt.Sprintf("RescoreDocument(%x, %d)", d.ID, d.Score)
	}
	return fmt.Sprintf("IndexDocument(%x, %d, %db)", d.ID, d.Score, len(d.Content))
}

func (db *DB) Index(doc IndexDocument) error {
	return db.BatchIndex([]IndexDocument{doc}, false)[0]
}

func (db *DB) BatchIndex(docs []IndexDocument, sortInsert bool) []error {
	if len(docs) == 0 {
		return nil
	}
	if sortInsert {
		sort.Slice(docs, func(i, j int) bool {
			return bytes.Compare(docs[i].ID, docs[j].ID) < 0
		})
	}

	errs := make([]error, len(docs))
	tx, err := db.Store.Begin(true)
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	defer tx.Rollback()

	for i, doc := range docs {
		contentBytes, err := scsu.Encode(doc.Content, nil)
		if err != nil {
			errs[i] = fmt.Errorf("invalid document content: %v", err)
			continue
		}
		chars, _ := Collect(doc.Content, db.MaxChars)
		if len(doc.ID) == 0 {
			errs[i] = fmt.Errorf("empty document ID")
			continue
		}
		if len(doc.ID) > bbolt.MaxKeySize {
			errs[i] = fmt.Errorf("document ID too large")
			continue
		}
		if len(chars) == 0 && !doc.Rescore {
			errs[i] = fmt.Errorf("empty document")
			continue
		}
		if _, ok := chars[0]; ok {
			panic("BUG")
		}

		if doc.Rescore {
			deleteTx(tx, db.Namespace, doc.ID, "rescore", doc.Score)
			continue
		}

		bkId, index := deleteTx(tx, db.Namespace, doc.ID, "index", 0)

		newScore := binary.BigEndian.AppendUint32(nil, doc.Score)

		var tmp []byte
		var runes1 []uint16
		var runes2 []uint32

		chars[0] = nil
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
			bk.Put(AppendSortedUvarint(newScore, index), v)
		}

		payload := AppendSortedUvarint(nil, index)
		payload = binary.BigEndian.AppendUint32(payload, doc.Score)

		buf1 := array16.Compress(runes1)
		payload = binary.AppendUvarint(payload, uint64(len(buf1)))
		payload = append(payload, buf1...)
		payload = binary.AppendUvarint(payload, uint64(len(runes2)))
		for _, k := range runes2 {
			k -= 0x10000
			if k > 0xFFFFFF {
				errs[i] = fmt.Errorf("invalid unicode %x", k+0x10000)
				continue
			}
			payload = append(payload, byte(k>>16), byte(k>>8), byte(k))
		}

		bkContent, _ := tx.CreateBucketIfNotExists([]byte(db.Namespace + "content"))

		if sortInsert {
			bkId.FillPercent = 95
			bkContent.FillPercent = 95
		}

		bkId.Put(doc.ID, payload)
		bkContent.Put(doc.ID, contentBytes)
	}

	if db.maxDocsTest > 0 {
		bkIndex, _ := tx.CreateBucketIfNotExists([]byte(db.Namespace + "index"))
		if diff := bkIndex.Sequence() - db.maxDocsTest; diff > 0 {
			db.evict(tx, int(diff))
		}
	}

	if db.FreelistRange != [2]int{} {
		stats := db.Store.Stats()
		size := stats.FreePageN + stats.PendingPageN

		if db.cfls > 0 {
			if size > db.FreelistRange[1] {
				db.cfls = 0
			} else {
				db.cfls = db.cfls/2 + len(docs)
				db.evict(tx, db.cfls)
			}
		} else {
			if size < db.FreelistRange[0] {
				db.cfls = len(docs) * 2
				db.evict(tx, db.cfls)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		for i := range errs {
			errs[i] = err
		}
	}
	return errs
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

func (db *DB) GetIndexAndScore(docID []byte) (uint64, uint32, error) {
	tx, err := db.Store.Begin(false)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	bkId := tx.Bucket([]byte(db.Namespace))
	if bkId == nil {
		return 0, 0, nil
	}
	payload := bkId.Get(docID)
	if len(payload) == 0 {
		return 0, 0, nil
	}
	index, w := SortedUvarint(payload)
	score := binary.BigEndian.Uint32(payload[w:])
	return index, score, nil
}

func deleteTx(tx *bbolt.Tx, ns string, id8 []byte, action string, rescore uint32) (*bbolt.Bucket, uint64) {
	bkId, _ := tx.CreateBucketIfNotExists([]byte(ns))
	bkIndex, _ := tx.CreateBucketIfNotExists([]byte(ns + "index"))
	bkContent, _ := tx.CreateBucketIfNotExists([]byte(ns + "content"))

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
			return nil, 0
		}
		index = bkId.Sequence()
		bkId.SetSequence(bkId.Sequence() + 1)
	}

	if action == "delete" {
		bkIndex.Delete(AppendSortedUvarint(nil, index))
		bkIndex.SetSequence(bkIndex.Sequence() - 1)
		bkId.Delete(id8)
		bkContent.Delete(id8)
	} else {
		bkIndex.Put(AppendSortedUvarint(nil, index), id8)
		if len(oldPayload) == 0 { // new doc
			// bkIndex.FillPercent = 0.95
			bkIndex.SetSequence(bkIndex.Sequence() + 1)
		}
	}

	foreachPayload(true, oldPayload, func(v uint32) {
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
	})

	if action == "rescore" {
		old := bkId.Get(id8)
		index, w := SortedUvarint(old)
		buf := AppendSortedUvarint(nil, index)
		buf = binary.BigEndian.AppendUint32(buf, rescore)
		buf = append(buf, old[w+4:]...)
		bkId.Put(id8, buf)
	}

	return bkId, index
}

func foreachPayload(zero bool, buf []byte, work func(uint32)) {
	if len(buf) == 0 {
		return
	}

	runes1Len, w := binary.Uvarint(buf)
	buf = buf[w:]

	array16.Foreach(buf[:runes1Len], func(r uint16) bool {
		work(uint32(r))
		return true
	})

	buf = buf[runes1Len:]
	runes2Len, w := binary.Uvarint(buf)
	buf = buf[w:]

	for i := 0; i < len(buf[:runes2Len*3]); i += 3 {
		r := uint32(buf[i])<<16 + uint32(buf[i+1])<<8 + uint32(buf[i+2])
		work(r + 0x10000)
	}

	if zero {
		work(0)
	}
}

func (db *DB) Count() (total int, watermark int, err error) {
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

func (db *DB) evict(tx *bbolt.Tx, diff int) {
	bkIndex, _ := tx.CreateBucketIfNotExists([]byte(db.Namespace + "index"))

	var toDeletes [][]byte
	bk := tx.Bucket([]byte(db.Namespace + "\x00\x00\x00\x00"))
	c := bk.Cursor()
	k, _ := c.First()
	for i := 0; i < int(diff) && len(k) > 0; i++ {
		id := bkIndex.Get(k[4:])
		if db.OnEvict != nil {
			db.OnEvict(id)
		}
		toDeletes = append(toDeletes, id)
		k, _ = c.Next()
	}
	for _, d := range toDeletes {
		deleteTx(tx, db.Namespace, d, "delete", 0)
	}
}
