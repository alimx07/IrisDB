# IrisDB

An LSM-tree key-value store written in Go. Every write is timestamped, enabling snapshot reads and time-range queries.

## Roadmap

- [ ] **v1** : LSM database :)
- [ ] **v1.x** : Expose Prometheus metrics
- [ ] **v2** : Let`s see K8s | add Kubernetes operator
- [ ] **v3** :  Maybe MCP integration


---
STILL WORKING ON IT //
---

---

## API

```go
import irisdb "github.com/alimx07/IrisDB"

db, err := irisdb.OpenDB("/path/to/data")
defer db.Close()

// Write
db.Put([]byte("key"), []byte("value"))
db.Delete([]byte("key"))

// Seq bulk-write
hint := irisdb.NewHint()
db.PutSeq([]byte("key001"), []byte("v1"), hint)
db.PutSeq([]byte("key002"), []byte("v2"), hint)

// Read
val, err := db.Get([]byte("key"))        // latest value, nil if not found

// Time-based reads
entry, err  := db.GetBefore([]byte("key"), t)       // latest value  <= t
entries, err := db.GetRange([]byte("key"), t1, t2)  // all values [t1, t2]


// Scan a key range 
entries, err := db.Scan([]byte("key001"), []byte("key999"))
```

## Configuration (`variables.go`)

| Variable | Default | Description |
|---|---|---|
| `MemTableSize` | 64 KB | Memtable capacity before flush |
| `SstableSize` | 128 KB | Level-0 SSTable size target |
| `MaxLevels` | 6 | Compaction levels |
| `SizeMultiple` | 5 | Level size multiplier |
| `PageSize` | 4096 B | Page size for all files |
| `Fsync` | true | Force-sync on write |
| `Compression` | true | Snappy block compression |
| `FalsePostiveProb` | 0.01 | Bloom filter false-positive rate |

## Tests

```bash
go test ./...
```

