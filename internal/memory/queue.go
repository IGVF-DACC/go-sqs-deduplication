package memory


import (
    "sync"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/dedup"
)


// InMemoryQueue for use in tests to simulate real queue (like SQSQueue)
type InMemoryQueue struct {
    messages []dedup.QueueMessage
    deletedMessages []string
    resetMessages []string
    mu sync.Mutex
    nextID int
    maxBatchSize int
}



func NewInMemoryQueue(maxBatchSize int) *InMemoryQueue {
    return &InMemoryQueue{
        messages:    make([]dedup.QueueMessage, 0),
        maxBatchSize: maxBatchSize,
    }
}



func (q *InMemoryQueue) AddMessages(messages []dedup.QueueMessage) {
    q.mu.Lock()
    defer q.mu.Unlock()
    q.messages = append(q.messages, messages...)
}


func (q *InMemoryQueue) PullMessagesBatch() ([]dedup.QueueMessage, error) {
    q.mu.Lock()
    defer q.mu.Unlock()
    var batch []dedup.QueueMessage
    if len(q.messages) > q.maxBatchSize {
        batch, q.messages = q.messages[:q.maxBatchSize], q.messages[q.maxBatchSize:]
    } else {
        batch, q.messages = q.messages, nil
    }
    return batch, nil
}



func (q *InMemoryQueue) DeleteMessagesBatch(receiptHandles []string) {
    q.mu.Lock()
    defer q.mu.Unlock()
    receiptHandleSet := make(map[string]struct{})
    for _, handle := range receiptHandles {
        receiptHandleSet[handle] = struct{}{}
        q.deletedMessages = append(q.deletedMessages, handle)
    }
    filteredMessages := q.messages[:0] // Use the same underlying array
    for _, msg := range q.messages {
        if _, found := receiptHandleSet[msg.ReceiptHandle()]; !found {
            filteredMessages = append(filteredMessages, msg)
        }
    }
    q.messages = filteredMessages
}


func (q *InMemoryQueue) ResetVisibilityBatch(receiptHandles []string) {
    q.mu.Lock()
    defer q.mu.Unlock()
    q.resetMessages = append(q.resetMessages, receiptHandles...)
}


func (q *InMemoryQueue) PutMessagesBatch(messages []dedup.QueueMessage) error {
    q.mu.Lock()
    defer q.mu.Unlock()
    q.messages = append(q.messages, messages...)
    return nil
}


func (q *InMemoryQueue) GetDeletedMessages() []string {
    q.mu.Lock()
    defer q.mu.Unlock()
    return q.deletedMessages
}


func (q *InMemoryQueue) GetResetMessages() []string {
    q.mu.Lock()
    defer q.mu.Unlock()
    return q.resetMessages
}


func (q *InMemoryQueue) MessagesLen() int {
    q.mu.Lock()
    defer q.mu.Unlock()
    return len(q.messages)
}
