package dedup


import (
    "sync"
)


type Deleter struct {
    queue Queue
    deleteChannel chan string
    wg *sync.WaitGroup
}


func (d *Deleter) SetDeleteChannel(deleteChannel chan string) {
    d.deleteChannel = deleteChannel
}


func (d *Deleter) getBatchOfMessagesToDelete() []string {
    maxMessages := 10
    var messages []string
    for i := 0; i < maxMessages; i++ {
        message, ok := <- d.deleteChannel
        if !ok {
            return messages
        }
        messages = append(messages, message)
    }
    return messages
}


func (d *Deleter) deleteMessages() {
    receiptHandles := d.getBatchOfMessagesToDelete()
    for len(receiptHandles) > 0 {
        d.queue.DeleteMessagesBatch(receiptHandles)
        receiptHandles = d.getBatchOfMessagesToDelete()
    }
}


func (d *Deleter) Start() {
    d.wg.Add(1)
    go func() {
        defer d.wg.Done()
        d.deleteMessages()
    }()
}


func NewDeleter(queue Queue, deleteChannel chan string, wg *sync.WaitGroup) *Deleter {
    return &Deleter{
        queue: queue,
        deleteChannel: deleteChannel,
        wg: wg,
    }
}
