package like

import (
	"encoding/binary"
	"fmt"
	"time"
	"unicode"
	"unsafe"

	"github.com/coyove/bbolt"
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
	Segs  []uint16
	ID    []byte
	Score uint32
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
