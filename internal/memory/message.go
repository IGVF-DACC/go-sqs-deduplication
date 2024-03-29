package memory


import (
    "fmt"
     "github.com/keenangraham/go-sqs-deduplication/internal/dedup"
)


type InMemoryQueueMessage struct {
	uniqueID      string
	messageID     string
	receiptHandle string
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


func GenerateInMemoryMessages(numMessages int) []dedup.QueueMessage {
    var messages []dedup.QueueMessage
    for i := 1; i <= numMessages; i++ {
        uniqueID := fmt.Sprintf("uuid-%d", i)
        messageID := fmt.Sprintf("msg-%d", i)
        receiptHandle := fmt.Sprintf("receipt-%d", i)
        message := InMemoryQueueMessage{
            uniqueID:      uniqueID,
            messageID:     messageID,
            receiptHandle: receiptHandle,
        }
        messages = append(messages, message)
    }
    return messages
}
