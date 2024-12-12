package dedup_test



import (
    "sync"
    "testing"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/memory"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/dedup"
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
    if toQueue.MessagesLen() != 100 {
        t.Errorf("Expected 100 messages to be on to Queue, got %d", toQueue.MessagesLen())
    }
}
