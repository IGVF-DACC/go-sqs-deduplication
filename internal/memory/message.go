package memory


import (
    "fmt"
    "math/rand"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/dedup"
)


type InMemoryQueueMessage struct {
    uniqueID      string
    messageID     string
    receiptHandle string
    rawBody string
}


func (m InMemoryQueueMessage) UniqueID() string {
    return m.uniqueID
}


func (m InMemoryQueueMessage) MessageID() string {
    return m.messageID
}


func (m InMemoryQueueMessage) ReceiptHandle() string {
    return m.receiptHandle
}


func (m InMemoryQueueMessage) RawBody() string {
    return m.rawBody
}


func GenerateInMemoryMessages(numMessages int) []dedup.QueueMessage {
    var messages []dedup.QueueMessage
    for i := 1; i <= numMessages; i++ {
        uniqueID := fmt.Sprintf("uuid-%d", i)
        messageID := fmt.Sprintf("msg-%d", rand.Int())
        receiptHandle := fmt.Sprintf("receipt-%d", rand.Int())
        rawBody := ""
        message := InMemoryQueueMessage{
            uniqueID:      uniqueID,
            messageID:     messageID,
            receiptHandle: receiptHandle,
            rawBody: rawBody,
        }
        messages = append(messages, message)
    }
    return messages
}


func MakeDuplicateInMemoryMessages(uniqueID string, numMessages int) []dedup.QueueMessage{
    var messages []dedup.QueueMessage
    for i := 1; i <= numMessages; i++ {
        messageID := fmt.Sprintf("msg-%d", rand.Int())
        receiptHandle := fmt.Sprintf("receipt-%d", rand.Int())
        rawBody := ""
        message := InMemoryQueueMessage{
            uniqueID:      uniqueID,
            messageID:     messageID,
            receiptHandle: receiptHandle,
            rawBody: rawBody,
        }
        messages = append(messages, message)
    }
    return messages
}
