package datafile

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDataFile(t *testing.T) {
	t.Run("all", func(t *testing.T) {
		dataLen := uint32(3)
		path1 := filepath.Join(os.TempDir(), "data_file_test_new.txt")
		defer func() {
			if err := removeFile(path1); err != nil {
				t.Errorf("Open file error: %s\n", err)
			}
		}()
		t.Run("New", func(t *testing.T) {
			testNew(path1, dataLen, t)
		})
		path2 := filepath.Join(os.TempDir(), "data_file_test.txt")
		defer func() {
			if err := removeFile(path2); err != nil {
				t.Fatalf("Open file error: %s\n", err)
			}
		}()
		max := 100000
		t.Run("RW", func(t *testing.T) {
			testRW(path2, dataLen, max, t)
		})
	})
}

func testNew(path string, dataLen uint32, t *testing.T) {
	t.Logf("New a data file (path: %s, dataLen: %d)...\n", path, dataLen)
	df, err := NewDataFile(path, dataLen)
	if err != nil {
		t.Logf("Couldn't new a data file: %s", err)
		t.FailNow()
	}
	if df == nil {
		t.Log("Unmoral data file!")
		t.FailNow()
	}
	defer df.Close()
	if df.DataLen() != dataLen {
		t.Fatalf("Incorrect data length!")
	}
}

func removeFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	_ = file.Close()
	return os.Remove(path)
}

func testRW(path string, dataLen uint32, max int, t *testing.T) {
	t.Logf("New a data file (path: %s, dataLen: %d)...\n", path, dataLen)
	df, err := NewDataFile(path, dataLen)
	if err != nil {
		t.Logf("Couldn't new a data file: %s", err)
		t.FailNow()
	}
	defer df.Close()
	var wg sync.WaitGroup
	wg.Add(5)
	//写入
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			var preWSN int64 = -1
			for j := 0; j < max; j++ {
				data := Data{byte(rand.Int31n(256)), byte(rand.Int31n(256)), byte(rand.Int31n(256))}
				wsn, err := df.Write(data)
				if err != nil {
					t.Fatalf("Unexpected writing error:%s\n", err)
				}
				if preWSN >= 0 && wsn <= preWSN {
					t.Fatalf("Incorrect WSN %d! (lt %d)\n", wsn, preWSN)
				}
				preWSN = wsn
			}
		}()
	}
	//读取
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			var preRSN int64 = -1
			for j := 0; j < max; j++ {
				rsn, data, err := df.Read()
				if err != nil {
					t.Fatalf("Unexpected writing error: %s\n", err)
				}
				if data == nil {
					t.Fatalf("Unormal data!")
				}
				if preRSN >= 0 && rsn <= preRSN {
					t.Fatalf("Incorrect RSN %d! (lt %d)\n", rsn, preRSN)
				}
				preRSN = rsn
			}
		}()
	}
	wg.Wait()
}
