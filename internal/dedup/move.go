package dedup

import (
    "fmt"
    "sync"
)


type Mover struct {
    fromQueue Queue
    toQueue Queue
    moveChannel chan QueueMessage // Messages pulled from here if not nil, otherwise uses fromQueue.
    state *SharedState
    wg *sync.WaitGroup
    flushToStorage bool // Is this move part of flushing memory to storage?
}


func (m *Mover) SetMoveChannel(moveChannel chan QueueMessage) {
    m.moveChannel = moveChannel
}


func (m *Mover) getBatchOfMessages() []QueueMessage {
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
        messages, err := m.fromQueue.PullMessagesBatch()
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
    for _, message := range messages {
        receiptHandles = append(receiptHandles, message.RecieptHandle())
    }
    m.fromQueue.DeleteMessagesBatch(receiptHandles)
}


func (m *Mover) addToStoredMessages(messages []QueueMessage) {
    m.state.mu.Lock()
    defer m.state.mu.Unlock()
    for _, message := range messages {
        m.state.storedMessages[message.UniqueID()] = message
    }
}


func (m *Mover) deleteFromKeepMessages(messages []QueueMessage) {
    m.state.mu.Lock()
    defer m.state.mu.Unlock()
    for _, message := range messages {
        if _, ok := m.state.keepMessages[message.UniqueID()]; ok {
            delete(m.state.keepMessages, message.UniqueID())
        }
    }
}



func (m *Mover) moveMessages() {
    for {
        messages := m.getBatchOfMessages()
        if len(messages) == 0 {
            fmt.Println("No messages found to move")
            break
        }
        err := m.putBatchOfMessages(messages)
        if err != nil {
            fmt.Println("Error putting messages in mover, breaking", err)
            break
        }
        m.deleteBatchOfMessages(messages)
        if flushToStorage {
            m.addToStoredMessages(messages)
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


func NewMover(fromQueue Queue, toQueue Queue, moveChannel chan QueueMessage, state *SharedState, flushToStorage bool, wg *sync.Waitgroup) *Mover {
    return &Mover{
        fromQueue: queue,
        toQueue: memoryQueue,
        moveChannel: moveChannel,
        state: state,
        flushToStorage: flushToStorage,
        wg: wg,
    }
}
