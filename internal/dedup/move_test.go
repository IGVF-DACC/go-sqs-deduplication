package dedup_test



import (
    "sync"
    "testing"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/memory"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/dedup"
    "fmt"
)


func TestMover(t *testing.T) {
    fromQueue := memory.NewInMemoryQueue(10)
    toQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(100)
    fromQueue.AddMessages(generatedMessages)
    state := dedup.NewSharedState(
        make(map[string]dedup.QueueMessage),
        make(map[string]struct{}),
        make(map[string]dedup.QueueMessage),
    )
    wg := &sync.WaitGroup{}
    mover := dedup.NewMover(fromQueue, toQueue, nil, state, false, wg)
    mover.Start()
    wg.Wait()
    if fromQueue.MessagesLen() != 0 {
        t.Errorf("Expected 0 messages to be on fromQueue, got %d", fromQueue.MessagesLen())
    }
    if len(fromQueue.GetResetMessages()) != 0 {
        t.Errorf("Expected 0 messages to be reset on fromQueue, got %d", len(fromQueue.GetResetMessages()))
    }
    if len(fromQueue.GetDeletedMessages()) != 100 {
        t.Errorf("Expected 100 messages to be deleted on fromQueue, got %d", len(fromQueue.GetDeletedMessages()))
    }
    if toQueue.MessagesLen() != 100 {
        t.Errorf("Expected 100 messages to be on to Queue, got %d", toQueue.MessagesLen())
    }
}


func TestMoverFlushToStorage(t *testing.T) {
    wg := &sync.WaitGroup{}
    fromQueue := memory.NewInMemoryQueue(10)
    toQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(100)
    fromQueue.AddMessages(generatedMessages)
    keepMessages := make(map[string]dedup.QueueMessage)
    for _, message := range generatedMessages {
        keepMessages[message.UniqueID()] = message
    }
    state := dedup.NewSharedState(
        keepMessages,
        make(map[string]struct{}),
        make(map[string]dedup.QueueMessage),
    )
    if state.KeepMessagesLen() != 100 {
        t.Error("Expected 100 messages in keepMessages")
    }
    if state.DeleteMessagesLen() != 0 {
        t.Error("Expected 0 messages in deleteMessages")
    }
    if state.StoredMessagesLen() != 0 {
        t.Error("Expected 0 messages in storedMessages")
    }
    if fromQueue.MessagesLen() != 100 {
        t.Error("Expected 100 messages on fromQueue")
    }
    if toQueue.MessagesLen() != 0 {
        t.Error("Expected 0 messages on toQueue")
    }
    moveChannel := make(chan dedup.QueueMessage, 1000)
    wg.Add(1)
    go func() {
        defer wg.Done()
        for _, message := range generatedMessages {
            moveChannel <- message
        }
        close(moveChannel)
    }()
    mover := dedup.NewMover(fromQueue, toQueue, moveChannel, state, true, wg)
    mover.Start()
    wg.Wait()
    fmt.Println(state.KeepMessagesLen())
    fmt.Println(state.DeleteMessagesLen())
    fmt.Println(state.StoredMessagesLen())
    if state.KeepMessagesLen() != 0 {
        t.Error("Expected 0 messages in keepMessages")
    }
    if state.DeleteMessagesLen() != 0 {
        t.Error("Expected 0 messages in deleteMessages")
    }
    if state.StoredMessagesLen() != 100 {
        t.Error("Expected 100 messages in storedMessages")
    }
    if fromQueue.MessagesLen() != 0 {
        t.Errorf("Expected 0 messages to be on fromQueue, got %d", fromQueue.MessagesLen())
    }
    if toQueue.MessagesLen() != 100 {
        t.Errorf("Expected 100 messages to be on to Queue, got %d", toQueue.MessagesLen())
    }
}


func TestMoverFlushToStorageWithDuplicates(t *testing.T) {
    wg := &sync.WaitGroup{}
    fromQueue := memory.NewInMemoryQueue(10)
    toQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(1000)
    duplicateMessages := memory.MakeDuplicateInMemoryMessages("abc", 5000)
    fromQueue.AddMessages(generatedMessages)
    fromQueue.AddMessages(duplicateMessages)
    keepMessages := make(map[string]dedup.QueueMessage)
    for _, message := range generatedMessages {
        keepMessages[message.UniqueID()] = message
    }
    state := dedup.NewSharedState(
        keepMessages,
        make(map[string]struct{}),
        make(map[string]dedup.QueueMessage),
    )
    if state.KeepMessagesLen() != 1000 {
        t.Error("Expected 1000 messages in keepMessages")
    }
    if state.DeleteMessagesLen() != 0 {
        t.Error("Expected 0 messages in deleteMessages")
    }
    if state.StoredMessagesLen() != 0 {
        t.Error("Expected 0 messages in storedMessages")
    }
    if fromQueue.MessagesLen() != 6000 {
        t.Error("Expected 6000 messages on fromQueue")
    }
    if toQueue.MessagesLen() != 0 {
        t.Error("Expected 0 messages on toQueue")
    }
    moveChannel := make(chan dedup.QueueMessage, 10000)
    wg.Add(1)
    go func() {
        defer wg.Done()
        for _, message := range generatedMessages {
            moveChannel <- message
        }
        close(moveChannel)
    }()
    mover := dedup.NewMover(fromQueue, toQueue, moveChannel, state, true, wg)
    mover.Start()
    wg.Wait()
    fmt.Println(state.KeepMessagesLen())
    fmt.Println(state.DeleteMessagesLen())
    fmt.Println(state.StoredMessagesLen())
    if state.KeepMessagesLen() != 0 {
        t.Error("Expected 0 messages in keepMessages")
    }
    if state.DeleteMessagesLen() != 0 {
        t.Error("Expected 0 messages in deleteMessages")
    }
    if state.StoredMessagesLen() != 1000 {
        t.Error("Expected 1000 messages in storedMessages")
    }
    if fromQueue.MessagesLen() != 5000 {
        t.Errorf("Expected 0 messages to be on fromQueue, got %d", fromQueue.MessagesLen())
    }
    if len(fromQueue.GetDeletedMessages()) != 1000 {
        t.Error("Expected 1000 deleted messages in fromQueue")
    }
    if toQueue.MessagesLen() != 1000 {
        t.Errorf("Expected 1000 messages to be on to Queue, got %d", toQueue.MessagesLen())
    }
}
