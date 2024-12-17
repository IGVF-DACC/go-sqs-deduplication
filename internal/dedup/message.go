package dedup


type QueueMessage interface {
    UniqueID() string
    MessageID() string
    ReceiptHandle() string
    RawBody() string
}
