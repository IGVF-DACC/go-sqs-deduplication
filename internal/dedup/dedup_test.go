package dedup_test

import (
    "testing"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/memory"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/dedup"
)


func TestDeduplicatorHalfDuplicate(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(1000)
    inMemoryQueue.AddMessages(generatedMessages)
    generatedMessages = memory.GenerateInMemoryMessages(1000)
    inMemoryQueue.AddMessages(generatedMessages)
    storageInMemoryQueue := memory.NewInMemoryQueue(10)
    config := &dedup.DeduplicatorConfig{
        Queue: inMemoryQueue,
        StorageQueue: storageInMemoryQueue,
        NumWorkers: 20,
        MaxInflight: 1500,
    }
    deduplicator := dedup.NewDeduplicator(config)
    deduplicator.Run()
    if len(inMemoryQueue.GetDeletedMessages()) != 1000 {
        t.Errorf("Expected 1000 messages to be deleted, got %d", len(inMemoryQueue.GetDeletedMessages()))
    }
    if len(inMemoryQueue.GetResetMessages()) != 1000 {
        t.Errorf("Expected 1000 messages to be reset, got %d", len(inMemoryQueue.GetResetMessages()))
    }
    if inMemoryQueue.MessagesLen() != 0 {
        t.Error("Queue should be empty")
    }
}


func TestDeduplicatorAllUnique(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(3000)
    inMemoryQueue.AddMessages(generatedMessages)
    storageInMemoryQueue := memory.NewInMemoryQueue(10)
    config := &dedup.DeduplicatorConfig{
        Queue: inMemoryQueue,
        StorageQueue: storageInMemoryQueue,
        NumWorkers: 5,
        MaxInflight: 10000,
    }
    deduplicator := dedup.NewDeduplicator(config)
    deduplicator.Run()
    if len(inMemoryQueue.GetDeletedMessages()) != 0 {
        t.Errorf("Expected 0 messages to be deleted, got %d", len(inMemoryQueue.GetDeletedMessages()))
    }
    if len(inMemoryQueue.GetResetMessages()) != 3000 {
        t.Errorf("Expected 3000 messages to be reset, got %d", len(inMemoryQueue.GetResetMessages()))
    }
    if inMemoryQueue.MessagesLen() != 0 {
        t.Error("Queue should be empty")
    }
}


func TestDeduplicatorAllDuplicate(t *testing.T) {
    inMemoryQueue := memory.NewInMemoryQueue(10)
    duplicateMessages := memory.MakeDuplicateInMemoryMessages("abc", 4000)
    inMemoryQueue.AddMessages(duplicateMessages)
    storageInMemoryQueue := memory.NewInMemoryQueue(10)
    config := &dedup.DeduplicatorConfig{
        Queue: inMemoryQueue,
        StorageQueue: storageInMemoryQueue,
        NumWorkers: 10,
        MaxInflight: 10000,
    }
    deduplicator := dedup.NewDeduplicator(config)
    deduplicator.Run()
    if len(inMemoryQueue.GetDeletedMessages()) != 3999 {
        t.Errorf("Expected 3999 messages to be deleted, got %d", len(inMemoryQueue.GetDeletedMessages()))
    }
    if len(inMemoryQueue.GetResetMessages()) != 1 {
        t.Errorf("Expected 1 messages to be reset, got %d", len(inMemoryQueue.GetResetMessages()))
    }
    if inMemoryQueue.MessagesLen() != 0 {
        t.Error("Queue should be empty")
    }
}



func TestDeduplicatorPartialProcessingBecauseOfMaxInflight(t *testing.T) {
    // MaxInflight over total
    inMemoryQueue := memory.NewInMemoryQueue(10)
    generatedMessages := memory.GenerateInMemoryMessages(3000)
    inMemoryQueue.AddMessages(generatedMessages)
    duplicateMessages := memory.MakeDuplicateInMemoryMessages("abc", 5000)
    inMemoryQueue.AddMessages(duplicateMessages)
    storageInMemoryQueue := memory.NewInMemoryQueue(10)
    config := &dedup.DeduplicatorConfig{
        Queue: inMemoryQueue,
        StorageQueue: storageInMemoryQueue,
        NumWorkers: 30,
        MaxInflight: 100000,
    }
    deduplicator := dedup.NewDeduplicator(config)
    deduplicator.Run()
    if len(inMemoryQueue.GetDeletedMessages()) != 4999 {
        t.Errorf("Expected 4999 messages to be deleted, got %d", len(inMemoryQueue.GetDeletedMessages()))
    }
    if len(inMemoryQueue.GetResetMessages()) != 3001 {
        t.Errorf("Expected 3001 messages to be reset, got %d", len(inMemoryQueue.GetResetMessages()))
    }
    if inMemoryQueue.MessagesLen() != 0 {
        t.Error("Queue should be empty")
    }
    // MaxInflight under total unique
    inMemoryQueue = memory.NewInMemoryQueue(10)
    generatedMessages = memory.GenerateInMemoryMessages(3000)
    inMemoryQueue.AddMessages(generatedMessages)
    duplicateMessages = memory.MakeDuplicateInMemoryMessages("abc", 5000)
    inMemoryQueue.AddMessages(duplicateMessages)
    storageInMemoryQueue = memory.NewInMemoryQueue(10)
    config = &dedup.DeduplicatorConfig{
        Queue: inMemoryQueue,
        StorageQueue: storageInMemoryQueue,
        NumWorkers: 30,
        MaxInflight: 500,
    }
    deduplicator = dedup.NewDeduplicator(config)
    deduplicator.Run()
    if len(inMemoryQueue.GetDeletedMessages()) != 8000 {
        t.Errorf("Expected 8000 messages to be deleted, got %d", len(inMemoryQueue.GetDeletedMessages()))
    }
    if len(inMemoryQueue.GetResetMessages()) != 0 {
        t.Errorf("Expected 0 messages to be reset, got %d", len(inMemoryQueue.GetResetMessages()))
    }
    if inMemoryQueue.MessagesLen() != 3001 {
        t.Errorf("Expected 3001 messages to be on queue, got %d", inMemoryQueue.MessagesLen())
    }
    // MaxInflight under total, over total unique
    inMemoryQueue = memory.NewInMemoryQueue(10)
    generatedMessages = memory.GenerateInMemoryMessages(3000)
    inMemoryQueue.AddMessages(generatedMessages)
    duplicateMessages = memory.MakeDuplicateInMemoryMessages("abc", 5000)
    inMemoryQueue.AddMessages(duplicateMessages)
    storageInMemoryQueue = memory.NewInMemoryQueue(10)
    config = &dedup.DeduplicatorConfig{
        Queue: inMemoryQueue,
        StorageQueue: storageInMemoryQueue,
        NumWorkers: 5,
        MaxInflight: 3003,
    }
    deduplicator = dedup.NewDeduplicator(config)
    deduplicator.Run()
    if len(inMemoryQueue.GetDeletedMessages()) != 4999 {
        t.Errorf("Got %d deleted messages", len(inMemoryQueue.GetDeletedMessages()))
    }
    if len(inMemoryQueue.GetResetMessages()) != 3001 {
        t.Errorf("Got %d reset messages", len(inMemoryQueue.GetResetMessages()))
    }
    if inMemoryQueue.MessagesLen() != 0 {
        t.Error("Queue has messages")
    }
}
