package dedup_test


import (
    "sync"
    "testing"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/memory"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/dedup"
)


func TestDeleter(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(100)
    inMemoryQueue.AddMessages(generatedMessages)
    deleteChannel := make(chan string, 20)
    pulledMessages, _ := inMemoryQueue.PullMessagesBatch()
    for _, pulledMessage := range pulledMessages {
        deleteChannel <- pulledMessage.ReceiptHandle()
    }
    close(deleteChannel)
    wg := &sync.WaitGroup{}
    deleter := dedup.NewDeleter(inMemoryQueue, deleteChannel, wg)
    deleter.Start()
    wg.Wait()
    if len(inMemoryQueue.GetDeletedMessages()) != 10 {
        t.Errorf("Expected 10 messages to be deleted, got %d", len(inMemoryQueue.GetDeletedMessages()))
    }
    if inMemoryQueue.MessagesLen() != 90 {
        t.Error("Unexpected number of messages in queue")
    }
}
