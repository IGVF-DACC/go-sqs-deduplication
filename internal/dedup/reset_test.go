package dedup_test

import (
    "sync"
    "testing"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/memory"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/dedup"
)


func TestReseter(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(100)
    inMemoryQueue.AddMessages(generatedMessages)
    if inMemoryQueue.MessagesLen() != 100 {
        t.Errorf("Unexpected number of messages in queue %d", inMemoryQueue.MessagesLen())
    }
    keepChannel := make(chan string, 100)
    for i := 0; i < 3; i++ {
        pulledMessages, _ := inMemoryQueue.PullMessagesBatch()
        for _, pulledMessage := range pulledMessages {
            keepChannel <- pulledMessage.ReceiptHandle()
        }
    }
    close(keepChannel)
    wg := &sync.WaitGroup{}
    reseter := dedup.NewReseter(inMemoryQueue, keepChannel, wg)
    reseter.Start()
    wg.Wait()
    if len(inMemoryQueue.GetResetMessages()) != 30 {
        t.Errorf("Expected 30 messages to be reset, got %d", len(inMemoryQueue.GetResetMessages()))
    }
}
