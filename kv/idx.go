package kv

import (
	"MetaDB/kv/storage"
	"MetaDB/kv/index"

	"sync"
	"io"
	"log"
	"time"
	"sort"
)

type DataType = uint16

const (
	Hash DataType = iota
)

// The operation of Hash
const (
	HashHSet uint16 = iota
	HashHDel
	HashHClear
	HashHExpire
)

func (db *KVDB) buildHashIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}
	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case HashHSet:
		db.hashIndex.indexes.HSet(key, string(entry.Meta.Extra), entry.Meta.Value)
	case HashHDel:
		db.hashIndex.indexes.HDel(key, string(entry.Meta.Extra))
	case HashHClear:
		db.hashIndex.indexes.HClear(key)
	case HashHExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.hashIndex.indexes.HClear(key)
		} else {
			db.expires[Hash][key] = int64(entry.Timestamp)
		}
	}
}

// 把磁盘中的所有文件读到内存中
// load Hash from db files.
func (db *KVDB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	dataType := 0
	go func(dType uint16) {
		defer wg.Done()

		// archived files
		var fileIds []int
		dbFile := make(map[uint32]*storage.DBFile)
		for k, v := range db.archFiles[dType] {
			dbFile[k] = v
			fileIds = append(fileIds, int(k))
		}
		
		// active file
		activeFile, err := db.getActiveFile(dType)
		if err != nil {
			log.Fatalf("active file is nil, the db can not open.[%+v]", err)
			return
		}
		dbFile[activeFile.Id] = activeFile
		fileIds = append(fileIds, int(activeFile.Id))
		
		// load the db files in a specified order.
		sort.Ints(fileIds)
		for i := 0; i < len(fileIds); i++ {
			fid := uint32(fileIds[i])
			df := dbFile[fid]
			var offset int64 = 0
		
			for offset <= db.config.BlockSize {
				if e, err := df.Read(offset); err == nil {
					idx := &index.Indexer{
						Meta:   e.Meta,
						FileId: fid,
						Offset: offset,
					}
					offset += int64(e.Size())
		
					if len(e.Meta.Key) > 0 {
						// 核心在于调用buildIndex
						if err := db.buildIndex(e, idx); err != nil {
							log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
						}
					}
				} else {
					if err == io.EOF {
						break
					}
					log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
				}
			}
		}
	} (uint16(dataType))
	wg.Wait()
	return nil
}