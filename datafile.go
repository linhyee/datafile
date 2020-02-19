package datafile

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// Data 代表数据的类型
type Data []byte

// DataFile 代表数据文件的接口类型
type DataFile interface {
	// Read 读取一个数据块
	Read() (rsn int64, d Data, err error)
	// Write 写入一个数据块
	Write(d Data) (wsn int64, err error)
	// RSN 获取最后读取的数据块的序列号
	RSN() int64
	// WSN 获取最后写入的数据块
	WSN() int64
	// DataLen 获取数据块的长度
	DataLen() uint32
	// Close 关闭数据文件
	Close() error
}

// myDataFile 代表数据文件的实现类型
type myDataFile struct {
	f       *os.File     // 文件
	fMutex  sync.RWMutex // 被用于文件的读写锁
	rCond   *sync.Cond   // 读操作需要用到的条件变量
	wOffset int64        // 写操作需要用到的偏移量
	rOffset int64        // 读操作需要用到的偏移量
	dataLen uint32       // 数据块的长度
}

// NewDataFile 新建一个数据文件的实例
func NewDataFile(path string, dataLen uint32) (DataFile, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if dataLen == 0 {
		return nil, errors.New("invalid data length")
	}
	df := &myDataFile{f: f, dataLen: dataLen}
	df.rCond = sync.NewCond(df.fMutex.RLocker())
	return df, nil
}

// Read 读取一个数据块
func (df *myDataFile) Read() (rsn int64, d Data, err error) {
	//读取并更新偏移量
	var offset int64
	for {
		offset = atomic.LoadInt64(&df.rOffset)
		if atomic.CompareAndSwapInt64(&df.rOffset, offset, offset+int64(df.dataLen)) {
			break
		}
	}
	//读取一个数据块
	rsn = offset / int64(df.dataLen)
	nBytes := make([]byte, df.dataLen)
	df.fMutex.RLock()
	defer df.fMutex.RUnlock()
	for {
		_, err = df.f.ReadAt(nBytes, offset)
		if err != nil {
			//Read遇到EOF时,要作确保能返回data,等待写Goroutine写入数据
			if err == io.EOF {
				df.rCond.Wait()
				continue
			}
			return
		}
		d = nBytes
		return
	}
}

// Write 写入一个数据块
func (df *myDataFile) Write(d Data) (wsn int64, err error) {
	//读取并更新写偏移量
	var offset int64
	for {
		offset = atomic.LoadInt64(&df.wOffset)
		if atomic.CompareAndSwapInt64(&df.wOffset, offset, offset+int64(df.dataLen)) {
			break
		}
	}
	//写入一个数据块
	wsn = offset / int64(df.dataLen)
	var nBytes []byte
	if len(d) > int(df.dataLen) {
		nBytes = d[0:df.dataLen]
	} else {
		//这里有个问题,当Data小于dataLen时,数据块会出现空洞
		nBytes = d
	}
	df.fMutex.Lock()
	defer df.fMutex.Unlock()
	_, err = df.f.Write(nBytes)
	//通知等待的读Goroutine
	df.rCond.Signal()
	return
}

// RSN 获取最后读取的数据块的序列号
func (df *myDataFile) RSN() int64 {
	offset := atomic.LoadInt64(&df.rOffset)
	return offset / int64(df.dataLen)
}

// WSN 获取最后写入的数据块
func (df *myDataFile) WSN() int64 {
	offset := atomic.LoadInt64(&df.wOffset)
	return offset / int64(df.dataLen)
}

// DataLen 获取数据块的长度
func (df *myDataFile) DataLen() uint32 {
	return df.dataLen
}

// Close 关闭数据文件
func (df *myDataFile) Close() error {
	if df.f == nil {
		return nil
	}
	return df.f.Close()
}
