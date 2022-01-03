package model

import (
	"MetaDB/server/model/queue"
)

type File struct {
	Token    string
	UUID     string
	Key      string
	FileName string
	Path     string
}

type FileQueue struct {
	Queue *queue.BQueue
}

var GFQ FileQueue

func InitGlobalFileQueue() {
	GFQ = FileQueue{}
	GFQ.Queue = queue.NewBQueue()
	go CheckGFQ()
}

// 管理GFQ
func CheckGFQ() {
	
}

func (fq *FileQueue) Push(file File) {
	fq.Queue.Push(file)
}

func (fq *FileQueue) Pushs(files []File) {
	temp := []queue.IData{}
	for file := range files {
		temp = append(temp, file)
	}
	fq.Queue.Pushs(temp)
}

func (fq *FileQueue) Pop() File {
	return fq.Queue.Pop().(File)
}

func (fq *FileQueue) Get(index int) File {
	return fq.Queue.Get(index).(File)
}

func (fq *FileQueue) Set(index int, file File) error {
	return fq.Queue.Set(index, file)
}

func (fq *FileQueue) Remove(index int) File {
	return fq.Queue.Remove(index).(File)
}

func (fq *FileQueue) RemoveAll() {
	fq.Queue.RemoveAll()
}

func (fq *FileQueue) Size() int {
	return fq.Queue.Size()
}

func (fq *FileQueue) FindIndex(call func(item File) bool) int {
	return fq.Queue.FindIndex(func(item queue.IData) bool {
		return call(item.(File))
	})
}