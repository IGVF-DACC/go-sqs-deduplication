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
    isMemoryOverflow bool
}


func (m *Mover) getBatchOfMessagesToMove() []QueueMessage {
    maxMessages := 10
    var messages []QueueMessage
    if m.moveChannel != nil {
        for i := 0; i < maxMessages; i++ {
            message, ok := <- m.moveChannel
            if !ok {
                return messages
            }
            messages = append(messages, message)
        }
        return messages
    } else {
        messages, err := p.fromQueue.PullMessagesBatch()
        if err != nil {
            fmt.Println("Error pulling messages in mover", err)
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


func (m *Mover) addToStoredInMemoryMessages(messages []QueueMessage) {
    m.state.mu.Lock()
    defer m.state.mu.Unlock()
    for i, message := range messages {
        m.state.storedInMemoryMessages[message.UniqueID()] = message
    }
}


func (m *Mover) deleteFromKeepMessages(messages []QueueMessage) {
    m.state.mu.Lock()
    defer m.state.mu.Unlock()
    for i, message := range messages {
        if _, ok := m.state.keepMessages[message.UniqueID()]; ok {
            delete(m.state.keepMessages, message.UniqueID())
        }
    }
}



func (m *Mover) moveMessages() {
    for {
        messages := m.getBatchOfMessagesToMove()
        if len(messages) == 0 {
            m.messagesExist = false
            break
        }
        err := m.putBatchOfMessages(messages)
        if err != nil {
            fmt.Println("Error putting messages in mover, breaking", err)
            break
        }
        m.deleteBatchOfMessages(messages)
        if isMemoryOverflow {
            m.addToStoredInMemoryMessages(messages)
            m.deleteFromKeepMessages(messages)
        }
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
