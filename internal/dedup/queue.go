package dedup


type Queue interface {
    PullMessagesBatch() ([]QueueMessage, error)
    DeleteMessagesBatch(receiptHandles []string)
    ResetVisibilityBatch(receiptHandles []string)
}
