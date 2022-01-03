package queue

import (
	"sync"
	"fmt"
)

type IQueue interface {
	Push(data IData)

	Pushs(data []IData)

	Pop() IData

	Get(index int) IData

	Set(index int, data IData) error

	Remove(index int) IData

	RemoveAll()

	Size() int

	All() []IData

	FindIndex(call func(item IData) bool) int
}

type IData interface{}

type BQueue struct {
	Datas []IData
	mutex sync.Mutex
}

func NewQueueFromDatas(datas []IData) *BQueue {
	return &BQueue{Datas: datas}
}

func NewBQueue() *BQueue {
	return &BQueue{Datas: []IData{}}
}

func (b *BQueue) Push(data IData) {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    b.Datas = append(b.Datas, data)
}

func (b *BQueue) Pushs(data []IData) {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    b.Datas = append(b.Datas, data...)
}

func (b *BQueue) Pop() IData {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    if len(b.Datas) <= 0 {
        return nil
    }

    var data = b.Datas[0]
    b.Datas = b.Datas[1:]
    return data
}

func (b *BQueue) Get(index int) IData {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    if len(b.Datas) <= 0 {
        return nil
    }
    if index < 0 || index >= len(b.Datas) {
        return nil
    }
    return b.Datas[index]
}

func (b *BQueue) Set(index int, data IData) error {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    if index < 0 || index >= len(b.Datas) {
        return fmt.Errorf(`index range of values [0,%v],index=%v`, len(b.Datas), index)
    }
    b.Datas[index] = data
    return nil
}

func (b *BQueue) Remove(index int) IData {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    if len(b.Datas) == 0 {
        return nil
    }
    if index < 0 || index >= len(b.Datas) {
        return nil
    }

    data := b.Datas[index]
    b.Datas = append(b.Datas[:index], b.Datas[index+1:]...)
    return data
}

func (b *BQueue) RemoveAll() {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    b.Datas = make([]IData, 0)
}

func (b *BQueue) Size() int {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    return len(b.Datas)
}

func (b *BQueue) All() []IData {
    return b.Datas
}

func (b *BQueue) FindIndex(call func(item IData) bool) int {
    b.mutex.Lock()
    defer b.mutex.Unlock()

    for i, data := range b.Datas {
        if call(data) {
            return i
        }
    }
    return -1
}