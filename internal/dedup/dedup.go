package dedup


import (
    "fmt"
    "sync"
    "time"
)


type Startable interface {
    Start()
}


func startAll[T Startable](actors []T) {
    for _, actor := range actors {
        actor.Start()
    }
}


type DeduplicatorConfig struct {
    Queue Queue
    StorageQueue Queue
    NumWorkers int
    MaxInflight int
    TimeLimitInSeconds int
}


type Deduplicator struct {
    config *DeduplicatorConfig
    wg *sync.WaitGroup
    state *SharedState
    pullers []*Puller
    deleters []*Deleter
    reseters []*Reseter
    flushToStorageMovers []*Mover
    restoreFromStorageMovers []*Mover
    keepChannel chan string
    deleteChannel chan string
    moveChannel chan QueueMessage
    startedFlushToStorage bool
}


func NewDeduplicator(config *DeduplicatorConfig) *Deduplicator {
    return &Deduplicator{
        config: config,
        wg: &sync.WaitGroup{},
        state: &SharedState{
            keepMessages: make(map[string]QueueMessage),
            deleteMessages: make(map[string]struct{}),
            storedMessages: make(map[string]QueueMessage),
            startTime: time.Now(),
        }}
}


func (d *Deduplicator) initPullers() {
    numWorkers := d.config.NumWorkers
    pullers := make([]*Puller, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        puller := &Puller{
            queue: d.config.Queue,
            state: d.state,
            wg: d.wg,
            messagesExist: true,
            timedOut: false,
            maxInflight: d.config.MaxInflight,
            timeLimitInSeconds: d.config.TimeLimitInSeconds,
        }
        pullers = append(pullers, puller)
    }
    d.pullers = pullers
}


func (d *Deduplicator) initDeleters() {
    d.initDeleteChannel()
    numWorkers := d.config.NumWorkers
    deleters := make([]*Deleter, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        deleter := &Deleter{
            queue: d.config.Queue,
            deleteChannel: d.deleteChannel,
            wg: d.wg,
        }
        deleters = append(deleters, deleter)
    }
    d.deleters = deleters
}


func (d *Deduplicator) initReseters() {
    d.initKeepChannel()
    numWorkers := d.config.NumWorkers
    reseters := make([]*Reseter, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        reseter := Reseter{
            queue: d.config.Queue,
            keepChannel: d.keepChannel,
            wg: d.wg,
        }
        reseters = append(reseters, &reseter)
    }
    d.reseters = reseters
}


func (d *Deduplicator) initFlushToStorageMovers() {
    d.initMoveChannel()
    numWorkers := d.config.NumWorkers
    movers := make([]*Mover, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        mover := &Mover{
            fromQueue: d.config.Queue,
            toQueue: d.config.StorageQueue,
            moveChannel: d.moveChannel,
            state: d.state,
            flushToStorage: true,
            wg: d.wg,
        }
        movers = append(movers, mover)
    }
    d.flushToStorageMovers = movers
}


func (d *Deduplicator) initRestoreFromStorageMovers() {
    numWorkers := d.config.NumWorkers
    movers := make([]*Mover, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        mover := &Mover{
            fromQueue: d.config.StorageQueue,
            toQueue: d.config.Queue,
            moveChannel: nil,
            state: d.state,
            flushToStorage: false,
            wg: d.wg,
        }
        movers = append(movers, mover)
    }
    d.restoreFromStorageMovers = movers
}


func (d *Deduplicator) startPullers() {
      startAll(d.pullers)
}


func (d *Deduplicator) startDeleters() {
      startAll(d.deleters)
}


func (d *Deduplicator) startReseters() {
      startAll(d.reseters)
}


func (d *Deduplicator) startFlushToStorageMovers() {
    startAll(d.flushToStorageMovers)
}


func (d *Deduplicator) startRestoreFromStorageMovers() {
    startAll(d.restoreFromStorageMovers)
}


func (d *Deduplicator) initKeepChannel() {
    d.keepChannel = make(chan string, 10000)
}


func (d *Deduplicator) initDeleteChannel() {
    d.deleteChannel = make(chan string, 10000)
}


func (d *Deduplicator) initMoveChannel() {
    d.moveChannel = make(chan QueueMessage, 10000)
}


func (d *Deduplicator) resetDeleteChannel() {
    d.initDeleteChannel()
    for _, deleter := range d.deleters {
        deleter.SetDeleteChannel(d.deleteChannel)
    }
}


func (d *Deduplicator) resetMoveChannel() {
    d.initMoveChannel()
    for _, mover := range d.flushToStorageMovers {
        mover.SetMoveChannel(d.moveChannel)
    }
}


func (d *Deduplicator) queueEmpty() bool {
    for _, puller := range d.pullers {
        if puller.messagesExist {
            return false
        }
    }
    return true
}


func (d *Deduplicator) timedOut() bool {
    for _, puller := range d.pullers {
        if puller.timedOut {
            return true
        }
    }
    return false
}


func (d *Deduplicator) sendMessagesForDeletion() {
    d.wg.Add(1)
    go func() {
        defer d.wg.Done()
        d.state.mu.Lock()
        defer d.state.mu.Unlock()
        for receiptHandle := range d.state.deleteMessages {
            d.deleteChannel <- receiptHandle
        }
        d.state.deleteMessages = make(map[string]struct{})
        close(d.deleteChannel)
    }()
}


func (d *Deduplicator) sendMessagesForVisibilityReset() {
    d.wg.Add(1)
    go func() {
        defer d.wg.Done()
        d.state.mu.Lock()
        defer d.state.mu.Unlock()
        for _, message := range d.state.keepMessages {
            d.keepChannel <- message.ReceiptHandle()
        }
        d.state.keepMessages = make(map[string]QueueMessage)
        close(d.keepChannel)
    }()
}


func (d *Deduplicator) sendMessagesForFlushingToStorage() {
    d.wg.Add(1)
    go func() {
        defer d.wg.Done()
        var keepMessages []QueueMessage
        d.state.mu.Lock()
        // Make local copy so can release lock.
        for _, value := range d.state.keepMessages {
            keepMessages = append(keepMessages, value)
        }
        d.state.mu.Unlock()
        for _, message := range keepMessages {
            d.moveChannel <- message
        }
        close(d.moveChannel)
    }()
}


func (d *Deduplicator) printInfo() {
    d.state.mu.Lock()
    fmt.Println("Keep messages:", len(d.state.keepMessages))
    fmt.Println("Delete messages:", len(d.state.deleteMessages))
    fmt.Println("Stored messages:", len(d.state.storedMessages))
    fmt.Println("Unique messages (keep + stored):", len(d.state.keepMessages) + len(d.state.storedMessages))
    d.state.mu.Unlock()
}


func (d *Deduplicator) atMaxInflight() bool {
    d.state.mu.Lock()
    defer d.state.mu.Unlock()
    if len(d.state.keepMessages) >= d.config.MaxInflight {
       return true
    }
    return false
}


func (d *Deduplicator) waitForWorkToFinish() {
    d.wg.Wait()
}


func (d *Deduplicator) shouldFlushToStorage() bool {
    return d.atMaxInflight() || d.startedFlushToStorage
}


func (d *Deduplicator) pullMessagesAndDeleteDuplicates() {
    d.initPullers()
    d.initDeleters()
    d.initFlushToStorageMovers()
    d.initRestoreFromStorageMovers()
    // Try restoring messages from storage queue in case
    // messages exist from previous run.
    fmt.Println("Restoring messages from storage queue (pre)")
    d.startRestoreFromStorageMovers()
    d.waitForWorkToFinish()
    // Run pull message/delete duplicates loop until no more
    // messages in queue, or max inflight of unique messages reached.
    for {
        fmt.Println("Pulling messages")
        d.startPullers() // Pulls messages until max inflight reached, or no more messages. Determines duplicates.
        d.waitForWorkToFinish()
        d.printInfo()
        fmt.Println("Deleting duplicate messages")
        d.sendMessagesForDeletion()
        d.startDeleters() // Processes messages for deletion.
        d.waitForWorkToFinish()
        if d.queueEmpty() {
            fmt.Println("Pulled all messages from queue")
            break
        }
        if d.shouldFlushToStorage() {
            fmt.Println("Max inflight for keep messages or already flushing to storage")
            fmt.Println("Flushing keep messages to storage")
            d.startedFlushToStorage = true
            d.sendMessagesForFlushingToStorage()
            d.startFlushToStorageMovers()
            d.waitForWorkToFinish()
            d.resetMoveChannel()
        }
        if d.timedOut() {
            fmt.Println("Stopping because of time limit")
            break
        }
        d.resetDeleteChannel() // Give deleters new channel since old one closed.
    }
    // Restore all the messages to keep from storage queue.
    fmt.Println("Restoring messages from storage queue (post)")
    d.startRestoreFromStorageMovers()
    d.waitForWorkToFinish()
}


func (d *Deduplicator) resetVisibilityOnMessagesToKeep() {
    d.initReseters()
    d.sendMessagesForVisibilityReset()
    d.startReseters() // Processes messages for visibility reset.
    d.waitForWorkToFinish()
}


func (d *Deduplicator) Run() {
    fmt.Println("Running deduplicator")
    d.pullMessagesAndDeleteDuplicates()
    fmt.Println("Resetting visibility on messages to keep")
    d.resetVisibilityOnMessagesToKeep()
    fmt.Println("All done")
}


func (d *Deduplicator) Reset() {
    d.state.Reset()
    d.startedFlushToStorage = false
}


func (d *Deduplicator) RunForever(secondsToSleepBetweenRuns int) {
    for {
        d.Run()
        fmt.Println("Sleeping seconds", secondsToSleepBetweenRuns)
        time.Sleep(time.Duration(secondsToSleepBetweenRuns) * time.Second)
        d.Reset()
    }
}
