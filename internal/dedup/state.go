package dedup


import (
    "sync"
    "time"
)


type SharedState struct {
    keepMessages map[string]QueueMessage
    deleteMessages map[string]struct{}
    storedMessages map[string]QueueMessage
    startTime time.Time
    mu sync.Mutex
}


func (s *SharedState) KeepMessagesLen() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    return len(s.keepMessages)
}


func (s *SharedState) DeleteMessagesLen() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    return len(s.deleteMessages)
}


func (s *SharedState) StoredMessagesLen() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    return len(s.storedMessages)
}


func NewSharedState(keepMessages map[string]QueueMessage, deleteMessages map[string]struct{}, storedMessages map[string]QueueMessage) *SharedState {
    return &SharedState{
        keepMessages: keepMessages,
        deleteMessages: deleteMessages,
        storedMessages: storedMessages,
        startTime: time.Now(),
    }
}
