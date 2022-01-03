package index

import (
	"MetaDB/kv/storage"
)

// Indexer the data index info, stored in skip list.
type Indexer struct {
	Meta   *storage.Meta // metadata info.
	FileId uint32        // the file id of storing the data.
	Offset int64         // entry data query start position.
}