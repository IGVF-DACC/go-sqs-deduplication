package dedup

import (
    "fmt"
    "sync"
)


type Mover struct {
    fromQueue Queue
    toQueue Queue
    moveChannel chan Message
    state *SharedState
    wg *sync.WaitGroup
    messagesExist bool
}


func (m *Mover) getBatchOfMessagesToMove() []QueueMessage {
    maxMessages := 10
    var messages []QueueMessage
    if m.moveChannel != nil {
        for i := 0; i < maxMessages; i++ {
            message, ok := <- m.moveChannel
            if !ok {
                m.messagesExist = false
                return messages
            }
            messages = append(messages, message)
        }
    } else {
        messages, err := p.fromQueue.PullMessagesBatch()
        if err != nil {
            fmt.Println("Error pulling messages in mover", err)
            m.messagesExist = false
            return messages
        }
        if len(messages) == 0 {
            m.messagesExist = false
        }
        return messages
    }
}


func (m *Mover) putBatchOfMessages(messages []QueueMessage) error {
    return m.toQueue.PutMessagesBatch(messages)
}


func (m *Mover) deleteBatchOfMessages(messages []QueueMessage) {
    receiptHandles := []string
    for i, message := range messages {
        receiptHandles = append(receiptHandles, message.RecieptHandle())
    }
    m.fromQueue.DeleteMessagesBatch(receiptHandles)
}


func (m *Mover) moveMessages() {
    for {
        messages := m.getBatchOfMessagesToMove()
    }
    receiptHandles := d.getBatchOfMessagesToDelete()
    for len(receiptHandles) > 0 {
        d.queue.DeleteMessagesBatch(receiptHandles)
        receiptHandles = d.getBatchOfMessagesToDelete()
    }
}


func (m *Mover) Start() {
    m.wg.Add(1)
    go func() {
        defer m.wg.Done()
        m.moveMessages()
    }()
}


func NewMover(fromQueue Queue, toQueue Queue, moveChannel chan QueueMessage, state *SharedState, messagesExist bool, wg *sync.Waitgroup) *Mover {
    return &Mover{
        queue: queue,
        memoryQueue: memoryQueue,
        state: state,
        messagesExist: messagesExist,
        wg: wg,
    }
}
