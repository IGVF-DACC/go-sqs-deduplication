package sqs


import (
    "fmt"
    "encoding/json"
    "github.com/aws/aws-sdk-go-v2/service/sqs/types"
    "github.com/keenangraham/go-sqs-deduplication/internal/dedup"
)


type InvalidationQueueMessageData struct {
    uuid string `json:"uuid"`
}


type InvalidationQueueMessage struct {
    data InvalidationQueueMessageData `json:"data"`
    messageID string
    receiptHandle string
}


func (m InvalidationQueueMessage) UniqueID() string {
    return m.data.uuid
}


func (m InvalidationQueueMessage) MessageID() string {
    return m.messageID
}


func (m InvalidationQueueMessage) ReceiptHandle() string {
    return m.receiptHandle
}


type messageParser func(rawMessage types.Message) (dedup.QueueMessage, error)


func InvalidationQueueMessageParser(rawMessage types.Message) (dedup.QueueMessage, error) {
    var message InvalidationQueueMessage
    err := json.Unmarshal([]byte(*rawMessage.Body), &message)
    if err != nil {
        fmt.Println("Error unmarshaling message from JSON:", err)
        return message, err
    }
    message.receiptHandle = *rawMessage.ReceiptHandle
    message.messageID = *rawMessage.MessageId
    return message, err
}
