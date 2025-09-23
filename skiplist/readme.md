## Benchmark Results

Machine: **AMD Ryzen 5 5600H with Radeon Graphics**  
OS/Arch: **linux/amd64**  

```text
pkg: irisdb/skiplist

BenchmarkInsert/Value-16B-12         	 5758156	       290.1 ns/op
BenchmarkInsert/Value-128B-12        	 5229015	       309.0 ns/op
BenchmarkInsert/Value-512B-12        	 4541200	       334.2 ns/op

BenchmarkGet/Value-16B-12            	37226019	        34.52 ns/op
BenchmarkGet/Value-128B-12           	31623740	        36.27 ns/op
BenchmarkGet/Value-512B-12           	30642248	        35.68 ns/op

BenchmarkMixed/Write-Heavy-12        	 7083886	       250.4 ns/op
BenchmarkMixed/Read-Heavy-12         	11627131	       157.0 ns/op
BenchmarkMixed/Write-Read-12         	 8928442	       204.2 ns/op

PASS
ok  	irisdb/skiplist	30.563s
