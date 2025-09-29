package page

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestInitPage(t *testing.T) {
	tempFile := "test.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 4096, false, 0)
	if err != nil {
		t.Errorf("Failed to init page: %v", err)
	}
	defer pg.Close()

	if pg.pageSize != 4096 {
		t.Errorf("Expected pageSize 4096, got %d", pg.pageSize)
	}
}

func TestWriteData(t *testing.T) {
	tempFile := "test.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 1024, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = pg.Write(data)
	if err != nil {
		t.Errorf("Write failed: %v", err.Error())
	}
	otherData := make([]byte, 1000)
	for i := range otherData {
		otherData[i] = byte(i % 256)
	}
	pageNum, err := pg.Write(otherData)
	if err != nil {
		t.Errorf("Write failed: %v", err.Error())
	}
	if pageNum != 5 {
		t.Errorf("Expected pageNum == 5 for data, got %d", pageNum)
	}
}

func TestReadWrite(t *testing.T) {
	tempFile := "test.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 512, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	original := make([]byte, 2000)
	for i := range original {
		original[i] = byte(i % 256)
	}

	pageNum, err := pg.Write(original)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}

	readData, _, err := pg.Read(uint16(pageNum))
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}

	if !bytes.Equal(original, readData) {
		t.Errorf("data mismatch")
	}
}

func TestEmptyData(t *testing.T) {
	tempFile := "test.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 4096, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	pageNum, err := pg.Write([]byte{})
	if err != nil {
		t.Fatalf("Write data failed: %v", err)
	}

	readData, _, err := pg.Read(uint16(pageNum))
	if err != nil {
		t.Fatalf("Read data failed: %v", err)
	}

	if len(readData) != 0 {
		t.Errorf("Expected empty data, got %d bytes", len(readData))
	}
}

func TestCloseMultipleTimes(t *testing.T) {
	tempFile := "test.db"
	defer os.Remove(tempFile)

	pg, _ := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 4096, false, 0)

	err := pg.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	err = pg.Close()
	if err != nil {
		t.Errorf("Second close should not fail: %v", err)
	}
}

func TestIterator(t *testing.T) {
	tempFile := "test.db"
	defer os.Remove(tempFile)

	pg, _ := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 4096, false, 0)
	defer pg.Close()

	data := []byte("IRISDB_ITERATOR")
	pageNum, _ := pg.Write(data)

	it := Newiterator(pg)
	if !it.Valid() {
		t.Error("Iterator should be valid initially")
	}

	readData, err := it.Get(uint16(pageNum))
	if err != nil {
		t.Errorf("Iterator Get failed: %v", err)
	}

	if !bytes.Equal(data, readData) {
		t.Error("Iterator data mismatch")
	}
}

func TestInvalidPageRead(t *testing.T) {
	tempFile := "test.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 4096, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	_, _, err = pg.Read(999)
	if err == nil {
		t.Error("Expected error when reading invalid page")
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	tempFile := "test.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 1024, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	numOps := 100
	var wg sync.WaitGroup
	wg.Add(numOps)

	type writeResult struct {
		pageNum uint32
		data    []byte
	}

	results := make(chan writeResult, numOps)

	// Concurrent writes
	for i := 0; i < numOps; i++ {
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("concurrent data %d", i))
			pageNum, err := pg.Write(data)
			if err != nil {
				t.Errorf("Concurrent write failed: %v", err)
				return
			}
			results <- writeResult{pageNum: pageNum, data: data}
		}(i)
	}

	wg.Wait()
	close(results)

	// Concurrent reads
	wg.Add(numOps)
	for res := range results {
		go func(res writeResult) {
			defer wg.Done()
			readData, _, err := pg.Read(uint16(res.pageNum))
			if err != nil {
				t.Errorf("Concurrent read failed for page %d: %v", res.pageNum, err)
				return
			}
			if !bytes.Equal(res.data, readData) {
				t.Errorf("Data mismatch on page %d", res.pageNum)
			}
		}(res)
	}
	wg.Wait()
}

func BenchmarkWrite(b *testing.B) {
	tempFile := "bench.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 4096, false, 0)
	if err != nil {
		b.Error(err)
	}
	defer pg.Close()

	data := make([]byte, 8000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(4)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := pg.Write(data)
			if err != nil {
				b.Errorf("Write failed: %v", err)
			}
		}
	})
}

func BenchmarkRead(b *testing.B) {
	tempFile := "bench.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 4096, false, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer pg.Close()

	numPages := 1000
	pageNums := make([]uint32, numPages)
	data := make([]byte, 8000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	for i := range pageNums {
		pageNum, err := pg.Write(data)
		if err != nil {
			b.Error(err)
		}
		pageNums[i] = pageNum
	}

	i := 0
	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(4)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := pg.Read(uint16(pageNums[i%numPages]))
			if err != nil {
				b.Errorf("Read failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkReadWrite(b *testing.B) {
	tempFile := "bench.db"
	defer os.Remove(tempFile)

	pg, err := InitPage(tempFile, os.O_CREATE|os.O_RDWR, 0644, 4096, false, 0)
	if err != nil {
		b.Error(err)
	}
	defer pg.Close()

	numPages := 1000
	pageNums := make([]uint32, numPages)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	i := 0
	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(4)
	// 50% read + 50% write
	b.RunParallel(func(pb *testing.PB) {

		for pb.Next() {
			if i%2 == 0 {
				pageNums[i%numPages], err = pg.Write(data)
				if err != nil {
					b.Errorf("Write failed: %v", err)
				}
			} else {
				// if i%numPages is not intiallized. it will read page 0
				_, _, err := pg.Read(uint16(pageNums[i%numPages]))
				if err != nil {
					b.Errorf("Read failed: %v", err)
				}
			}
			i++
		}
	})
}
