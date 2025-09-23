## Benchmark Results

Machine: **AMD Ryzen 5 5600H with Radeon Graphics**  
OS/Arch: **linux/amd64**  

```text
pkg: irisdb/skiplist

BenchmarkInsert/Value-16B-12         	 4105694	       366.3 ns/op
BenchmarkInsert/Value-128B-12        	 3670724	       388.4 ns/op
BenchmarkInsert/Value-512B-12        	 3413232	       427.1 ns/op

BenchmarkGet/Value-16B-12            	23967844	        56.55 ns/op
BenchmarkGet/Value-128B-12           	24281823	        48.33 ns/op
BenchmarkGet/Value-512B-12           	25544679	        52.92 ns/op

BenchmarkMixed/Write-Heavy-12        	 4819551	       312.1 ns/op
BenchmarkMixed/Read-Heavy-12         	 8378656	       207.7 ns/op
BenchmarkMixed/Write-Read-12         	 5875960	       261.2 ns/op

PASS
ok  	irisdb/skiplist	30.563s
