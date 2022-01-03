package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sort"

	"github.com/edsrzf/mmap-go"
)

const (
	FilePerm = 0644
	PathSeparator = string(os.PathSeparator)
)

var (
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.hash",
	}

	DBFileSuffixName = []string{"hash"}
)

var (
	// ErrEmptyEntry the entry is empty.
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

type FileRWMethod uint8

const (
	FileIO FileRWMethod = iota
	MMap
)

type DBFile struct {
	Id     uint32
	path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}

func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, path: path, Offset: stat.Size(), method: method}
	
	if method == FileIO {
		df.File = file
	} else {
		if err = file.Truncate(blockSize); err != nil {
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil
}

func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte

	if buf, err = df.ReadBuf(offset, int64(entryHeaderSize)); err != nil {
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.ReadBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}

	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var val []byte
		if val, err = df.ReadBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = val
	}

	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var val []byte
		if val, err = df.ReadBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = val
	}

	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.Crc32 {
		return nil, ErrInvalidCrc
	}

	return
}

func (df *DBFile) ReadBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if df.method == FileIO {
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap && offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil
}

func (df *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := df.method
	writeOff := df.Offset
	encVal, err := e.Encode()
	if err != nil {
		return err
	}

	if method == FileIO {
		if _, err := df.File.WriteAt(encVal, writeOff); err != nil {
			return err
		}
	}
	if method == MMap {
		copy(df.mmap[writeOff:], encVal)
	}
	df.Offset += int64(e.Size())
	return nil
}

func (df *DBFile) Close(sync bool) (err error) {
	if sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}
	if df.mmap != nil {
		err = df.mmap.Unmap()
	}
	return
}

func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}

	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}

// Build load all db files from disk.
// 返回的第一个参数是每种数据类型对应的文件id对应的文件，返回的第二个参数是每种数据类型对应的active file id
func Build(path string, method FileRWMethod, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	// 读取目录中的文件
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}

	// 取出每一种数据类型存储的文件的所有文件id
	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			}
		}
	}

	// load all the db files.
	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	fileIDs := fileIdsMap[dataType]
	sort.Ints(fileIDs)
	files := make(map[uint32]*DBFile)
	var activeFileId uint32 = 0

	if len(fileIDs) > 0 {
		// active fileid 是最大的
		activeFileId = uint32(fileIDs[len(fileIDs)-1])

		for i := 0; i < len(fileIDs)-1; i++ {
			id := fileIDs[i]

			file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
			if err != nil {
				return nil, nil, err
			}
			files[uint32(id)] = file
		}
	}
	archFiles[dataType] = files
	activeFileIds[dataType] = activeFileId
	return archFiles, activeFileIds, nil
}