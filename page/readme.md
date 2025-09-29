## Benchmark Results

Machine: **AMD Ryzen 5 5600H with Radeon Graphics**  
OS/Arch: **linux/amd64**  

```text
pkg: github.com/alimx07/IrisDB/page
cpu: AMD Ryzen 5 5600H with Radeon Graphics         
BenchmarkWrite-12                  99931             11936 ns/op            4096 B/op          1 allocs/op
BenchmarkRead-12                  315517              3821 ns/op           21760 B/op          4 allocs/op
BenchmarkReadWrite-12             399576              2667 ns/op            3074 B/op          1 allocs/op
PASS
ok      github.com/alimx07/IrisDB/page  3.711s

PASS
ok  	irisdb/skiplist	30.563s
