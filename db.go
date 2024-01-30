package like

import "github.com/coyove/bbolt"

type DB struct {
	Store     *bbolt.DB
	Namespace string
	MaxChars  uint16
}
