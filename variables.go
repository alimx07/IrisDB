package irisdb

import (
	"os"
	"time"
)

var (
	Fsync            = true
	SyncInterval     = 100 * time.Millisecond
	PageSize         = 4096
	Compression      = true
	Wal              = true
	Flag             = os.O_CREATE | os.O_RDWR
	Permission       = 0644
	FalsePostiveProb = 0.01
	DBName           = "irisdb"
	MemTableSize     = 64 * 1024
	AvgKeySize       = 16
)

const (
	KeyExtension     = ".key"
	ValExtension     = ".val"
	BloomExtension   = ".bf"
	WalExtension     = ".wal"
	SSTABLEExtesnion = ".sst"
	DBExtension      = ".irisdb"
)
