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
	SstableSize      = 128 * 1024 // size of sstable in level 0
	SizeMultiple     = 5          // SizeLevel(i) = Multiple * SizeLevel(i-1)
	TOMPOSTONE       = []byte{0xFD, 0xFE, 0xFA, 0xF9}
	MaxLevels        = 6
)

const (
	KeyExtension     = ".key"
	ValExtension     = ".val"
	BloomExtension   = ".bf"
	WalExtension     = ".wal"
	SSTABLEExtesnion = ".sst"
	DBExtension      = ".irisdb"
	MagicNumber      = 0xAB75DE95
)
