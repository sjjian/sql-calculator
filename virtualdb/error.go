package virtualdb

import "errors"

const (
	NotExistSchemaErrorPattern  = "not exist schema: %s"
	DuplicateSchemaErrorPattern = "duplicate schema: %s"
	NotExistTableErrorPattern   = "not exist table: %s.%s"
	DuplicateTableErrorPattern  = "duplicate table: %s.%s"
	NotExistColumnErrorPattern  = "not exist column %s in %s.%s"
	DuplicateColumnErrorPattern = "duplicate column %s in %s.%s"
	NotExistIndexErrorPattern   = "not exist index %s in %s.%s"
	DuplicateIndexErrorPattern  = "duplicate index %s in %s.%s"
)

var (
	NoPrimaryKeyError    = errors.New("no primary key")
	ExistPrimaryKeyError = errors.New("primary key exists")
)
