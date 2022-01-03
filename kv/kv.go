package kv

import (
	"MetaDB/kv/storage"
	"MetaDB/kv/index"
	"MetaDB/kv/utils"

	"sync"
	"errors"
	"time"
	"log"
	"os"
	"io/ioutil"
	"io"
	"encoding/json"
	"sync/atomic"
	"sort"
	"fmt"
)

var (
	// ErrEmptyKey the key is empty
	ErrEmptyKey = errors.New("rosedb: the key is empty")

	// ErrKeyNotExist key not exist
	ErrKeyNotExist = errors.New("rosedb: key not exist")

	// ErrKeyTooLarge the key too large
	ErrKeyTooLarge = errors.New("rosedb: key exceeded the max length")

	// ErrValueTooLarge the value too large
	ErrValueTooLarge = errors.New("rosedb: value exceeded the max length")

	// ErrNilIndexer the indexer is nil
	ErrNilIndexer = errors.New("rosedb: indexer is nil")

	// ErrCfgNotExist the config is not exist
	ErrCfgNotExist = errors.New("rosedb: the config file not exist")

	// ErrReclaimUnreached not ready to reclaim
	ErrReclaimUnreached = errors.New("rosedb: unused space not reach the threshold")

	// ErrExtraContainsSeparator extra contains separator
	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")

	// ErrInvalidTTL ttl is invalid
	ErrInvalidTTL = errors.New("rosedb: invalid ttl")

	// ErrKeyExpired the key is expired
	ErrKeyExpired = errors.New("rosedb: key is expired")

	// ErrDBisReclaiming reclaim and single reclaim can`t execute at the same time.
	ErrDBisReclaiming = errors.New("rosedb: can`t do reclaim and single reclaim at the same time")

	// ErrDBIsClosed db can`t be used after closed.
	ErrDBIsClosed = errors.New("rosedb: db is closed, reopen it")

	// ErrTxIsFinished tx is finished.
	ErrTxIsFinished = errors.New("rosedb: transaction is finished, create a new one")

	// ErrActiveFileIsNil active file is nil.
	ErrActiveFileIsNil = errors.New("rosedb: active file is nil")
)


const (
	// The path for saving rosedb config file.
	configSaveFile = string(os.PathSeparator) + "DB.CFG"

	// rosedb reclaim path, a temporary dir, will be removed after reclaim.
	reclaimPath = string(os.PathSeparator) + "rosedb_reclaim"

	// Separator of the extra info, some commands can`t contains it.
	ExtraSeparator = "\\0"

	// DataStructureNum the num of different data structures, there are five now(string, list, hash, set, zset).
	DataStructureNum = 1
)

type (
	KVDB struct {
		activeFile         *sync.Map
		archFiles          ArchivedFiles
		hashIndex          *HashIdx
		config             Config
		mu                 sync.RWMutex
		expires            Expires
		isReclaiming       bool
		lockMgr            *LockMgr
		closed             uint32
	}

	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

	Expires map[DataType]map[string]int64
)

// Open a rosedb instance. You must call Close after using it.
func Open(config Config) (*KVDB, error) {
	// create the dir path if not exists.
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// load the db files from disk.
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	// set active files for writing.
	activeFiles := new(sync.Map)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles.Store(dataType, file)
	}

	db := &KVDB{
		activeFile: activeFiles,
		archFiles:  archFiles,
		config:     config,
		hashIndex:  newHashIdx(),
		expires:    make(Expires),
	}
	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}
	db.lockMgr = newLockMgr(db)

	// load indexes from db files.
	// 把磁盘中的数据读到内存中建立索引
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Reopen the db according to the specific config path.
func Reopen(path string) (*KVDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config

	b, err := ioutil.ReadFile(path + configSaveFile)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, err
	}
	return Open(config)
}

// Close db and save relative configs.
func (db *KVDB) Close() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err = db.saveConfig(); err != nil {
		return err
	}

	// close and sync the active file.
	db.activeFile.Range(func(key, value interface{}) bool {
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Close(true); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}

	// close the archived files.
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err = file.Sync(); err != nil {
				return err
			}
		}
	}

	atomic.StoreUint32(&db.closed, 1)
	return
}

// Reclaim reclaim db files`s redundant space in disk.
// Reclaim operation will read all archived files, iterate all entries and find the valid.
// Then rewrite the valid entries to new db files.
// So the time required for reclaim operation depend on the number of entries, you`d better execute it in low peak period.
func (db *KVDB) Reclaim() (err error) {
	var reclaimable bool
	for _, archFiles := range db.archFiles {
		if len(archFiles) >= db.config.ReclaimThreshold {
			reclaimable = true
			break
		}
	}
	if !reclaimable {
		return ErrReclaimUnreached
	}

	// create a temporary directory for storing the new db files.
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer func() {
		db.isReclaiming = false
		db.mu.Unlock()
	}()
	db.isReclaiming = true

	// processing the different types of files in different goroutines.
	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}

	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for i := 0; i < DataStructureNum; i++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()

			if len(db.archFiles[dType]) < db.config.ReclaimThreshold {
				newArchivedFiles.Store(dType, db.archFiles[dType])
				return
			}

			var (
				df        *storage.DBFile
				fileId    uint32
				archFiles = make(map[uint32]*storage.DBFile)
				fileIds   []int
			)

			for _, file := range db.archFiles[dType] {
				fileIds = append(fileIds, int(file.Id))
			}
			sort.Ints(fileIds)

			for _, fid := range fileIds {
				file := db.archFiles[dType][uint32(fid)]
				var offset int64 = 0
				var reclaimEntries []*storage.Entry

				// read all entries in db file, and find the valid entry.
				for {
					if e, err := file.Read(offset); err == nil {
						if db.validEntry(e, offset, file.Id) {
							reclaimEntries = append(reclaimEntries, e)
						}
						offset += int64(e.Size())
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("err occurred when read the entry: %+v", err)
						return
					}
				}

				// rewrite the valid entries to new db file.
				for _, entry := range reclaimEntries {
					if df == nil || int64(entry.Size())+df.Offset > db.config.BlockSize {
						df, err = storage.NewDBFile(reclaimPath, fileId, db.config.RwMethod, db.config.BlockSize, dType)
						if err != nil {
							log.Fatalf("err occurred when create new db file: %+v", err)
							return
						}
						archFiles[fileId] = df
						fileId += 1
					}

					if err = df.Write(entry); err != nil {
						log.Fatalf("err occurred when write the entry: %+v", err)
						return
					}
				}
			}
			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)
		}(uint16(i))
	}
	wg.Wait()

	dbArchivedFiles := make(ArchivedFiles)
	for i := 0; i < DataStructureNum; i++ {
		dType := uint16(i)
		value, ok := newArchivedFiles.Load(dType)
		if !ok {
			log.Printf("one type of data(%d) is missed after reclaiming.", dType)
			return
		}
		dbArchivedFiles[dType] = value.(map[uint32]*storage.DBFile)
	}

	// delete the old db files.
	for dataType, files := range db.archFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				// close file before remove it.
				if err = f.File.Close(); err != nil {
					log.Println("close old db file err: ", err)
					return
				}
				if err = os.Remove(f.File.Name()); err != nil {
					log.Println("remove old db file err: ", err)
					return
				}
			}
		}
	}

	// copy the temporary reclaim directory as new db files.
	for dataType, files := range dbArchivedFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(reclaimPath+name, db.config.DirPath+name)
			}
		}
	}

	db.archFiles = dbArchivedFiles
	return
}

// Backup copy the database directory for backup.
func (db *KVDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {
		err = utils.CopyDir(db.config.DirPath, dir)
	}
	return
}

// Persist the db files.
func (db *KVDB) Sync() (err error) {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.activeFile.Range(func(key, value interface{}) bool {
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Sync(); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}
	return
}

// save config before closing db.
func (db *KVDB) saveConfig() error {
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	b, err := json.Marshal(db.config)
	if err != nil {
		return err
	}
	_, err = file.Write(b)
	if err != nil {
		return err
	}
	err = file.Close()
	return err
}

// build the indexes for different data structures.
func (db *KVDB) buildIndex(entry *storage.Entry, idx *index.Indexer) (err error) {
	switch entry.GetType() {
	case storage.Hash:
		db.buildHashIndex(entry)
	}
	return
}

func (db *KVDB) getActiveFile(dType DataType) (file *storage.DBFile, err error) {
	value, ok := db.activeFile.Load(dType)
	if !ok || value == nil {
		return nil, ErrActiveFileIsNil
	}

	var typeOk bool
	if file, typeOk = value.(*storage.DBFile); !typeOk {
		return nil, ErrActiveFileIsNil
	}
	return
}

func (db *KVDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

func (db *KVDB) checkExpired(key []byte, dType DataType) (expired bool) {
	deadline, exist := db.expires[dType][string(key)]
	if !exist {
		return
	}

	if time.Now().Unix() > deadline {
		expired = true

		var e *storage.Entry
		switch dType {
		case Hash:
			e = storage.NewEntryNoExtra(key, nil, Hash, HashHClear)
			db.hashIndex.indexes.HClear(string(key))
		}
		if err := db.store(e); err != nil {
			log.Println("checkExpired: store entry err: ", err)
			return
		}
		// delete the expire info stored at key.
		delete(db.expires[dType], string(key))
	}
	return
}

// validEntry check whether entry is valid(contains add and update types of operations).
// expired entry will be filtered.
func (db *KVDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}

	mark := e.GetMark()
	switch e.GetType() {
	case Hash:
		if mark == HashHExpire {
			deadline, exist := db.expires[Hash][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == HashHSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	}
	return false
}

func (db *KVDB) store(e *storage.Entry) error {
	// sync the db file if file size is not enough, and open a new db file.
	config := db.config
	activeFile, err := db.getActiveFile(e.GetType())
	if err != nil {
		return err
	}

	if activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err := activeFile.Sync(); err != nil {
			return err
		}

		// save the old db file as arched file.
		activeFileId := activeFile.Id
		db.archFiles[e.GetType()][activeFileId] = activeFile

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId+1, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}
		activeFile = newDbFile
	}

	// write entry to db file.
	if err := activeFile.Write(e); err != nil {
		return err
	}
	db.activeFile.Store(e.GetType(), activeFile)

	// persist db file according to the config.
	if config.Sync {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}