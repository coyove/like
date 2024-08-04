package like

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
	"unicode"
	"unsafe"

	"github.com/coyove/bbolt"
	"github.com/pierrec/lz4/v4"
)

var shortDocString bool

type DB struct {
	Store         *bbolt.DB
	Namespace     string
	MaxChars      uint16
	OnEvict       func([]byte)
	SearchTimeout time.Duration
	FreelistRange [2]int

	maxDocsTest uint64
	cfls        int
}

func (db *DB) OpenDefault(path string) (err error) {
	db.Store, err = bbolt.Open(path, 0644, &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		NoSync:       true,
	})
	db.MaxChars = 65535
	return
}

type Document struct {
	Index uint64
	Segs  [][2]uint16
	ID    []byte
	Score uint32
	db    *DB
}

func (d Document) boundKey(in []byte) []byte {
	return AppendSortedUvarint(binary.BigEndian.AppendUint32(in, d.Score), d.Index)
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

func (d Document) Content() (v []byte, err error) {
	tx, err := d.db.Store.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	bk := tx.Bucket([]byte(d.db.Namespace + "content"))
	if bk == nil {
		return nil, nil
	}
	comp := bk.Get(d.ID)
	if len(comp) == 0 {
		return nil, nil
	}
	rd := lz4.NewReader(bytes.NewReader(comp))
	return io.ReadAll(rd)
}

func (d Document) String() string {
	if shortDocString {
		return fmt.Sprintf("#%d", d.Index)
	}

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
