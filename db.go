package like

import "github.com/coyove/bbolt"

type DB struct {
	Store     *bbolt.DB
	Namespace string
	MaxChars  uint16
}

func (db *DB) OpenDefault(path string) (err error) {
	db.Store, err = bbolt.Open(path, 0644, &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		NoSync:       true,
	})
	db.MaxChars = 65535
	return
}
