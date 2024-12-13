package dedup


import (
    "fmt"
    "sync"
    "time"
)


type Puller struct {
    queue Queue
    state *SharedState
    messagesExist bool
    timedOut bool
    maxInflight int
    timeLimitInSeconds time.Duration
    wg *sync.WaitGroup
}


// Only call with mutex locked.
func (p *Puller) checkIfMessageAlreadyExists(uniqueID string) (QueueMessage, bool) {
    // Message already seen in this round of pulling.
    if keepMessage, exists := p.state.keepMessages[uniqueID]; exists {
        return keepMessage, true
    }
    // Message already seen and persisted to storage queue.
    if storedMessage, exists := p.state.storedMessages[uniqueID]; exists {
        return storedMessage, true
    }
    return nil, false
}


// Only call with mutex locked.
func (p *Puller) processMessages(messages []QueueMessage) {
    for _, message := range messages {
        existingMessage, alreadyExists := p.checkIfMessageAlreadyExists(message.UniqueID())
        if alreadyExists {
            if existingMessage.MessageID() != message.MessageID() {
                // Already seen the UUID, mark message for deletion.
                p.state.deleteMessages[message.ReceiptHandle()] = struct{}{}
            } else {
                // If for some reason the same message is delivered more than once from the queue.
                continue
            }
        } else {
            // Haven't seen it before, add to messages to keep.
            p.state.keepMessages[message.UniqueID()] = message
        }
    }
}


// Only call with mutex locked.
func (p *Puller) atMaxInflight() bool {
    return len(p.state.keepMessages) + len(p.state.deleteMessages) >= p.maxInflight
}


// Only call with mutex locked.
func (p *Puller) atTimeout() bool {
    if time.Since(p.state.startTime) > p.timeLimitInSeconds * time.Second {
        return true
    }
    return false
}


func (p *Puller) getMessagesUntilMaxInflight() {
    for {
        messages, err := p.queue.PullMessagesBatch()
        if err != nil {
            fmt.Println("Error pulling messages", err)
            p.messagesExist = false
            return
        }
        if len(messages) == 0 {
            p.messagesExist = false
            break
        }
        p.state.mu.Lock()
        p.processMessages(messages)
        if p.atMaxInflight() {
            fmt.Println("Reaching max inflight messages from puller")
            p.state.mu.Unlock()
            break
        }
        if p.atTimeout() {
            fmt.Println("Reaching time limit from puller")
            p.timedOut = true
            p.state.mu.Unlock()
            break
        }
        p.state.mu.Unlock()
    }
}


func (p *Puller) Start() {
    p.wg.Add(1)
    go func() {
        defer p.wg.Done()
        p.getMessagesUntilMaxInflight()
    }()
}


func (p *Puller) MessagesExist() bool {
    return p.messagesExist
}


func (p *Puller) TimedOut() bool {
    return p.timedOut
}


func NewPuller(queue Queue, state *SharedState, messagesExist bool, timedOut bool, maxInflight int, timeLimitInSeconds time.Duration, wg *sync.WaitGroup) *Puller {
    return &Puller{
        queue: queue,
        state: state,
        messagesExist: messagesExist,
        maxInflight: maxInflight,
        timedOut: timedOut,
        timeLimitInSeconds: timeLimitInSeconds,
        wg: wg,
    }
}
