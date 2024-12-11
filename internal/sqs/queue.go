package sqs


import (
    "fmt"
    "context"
    "os"
    "github.com/aws/aws-sdk-go-v2/aws"
    _sqs "github.com/aws/aws-sdk-go-v2/service/sqs"
    "github.com/aws/aws-sdk-go-v2/service/sqs/types"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/IGVF-DACC/go-sqs-deduplication/internal/dedup"
)


func getClient(profileName string) *_sqs.Client {
    config, err := config.LoadDefaultConfig(
        context.TODO(),
        config.WithSharedConfigProfile(profileName),
    )
    if err != nil {
        fmt.Println("error creating SQS client", err)
        os.Exit(1)
    }
    return _sqs.NewFromConfig(
        config,
        func (o *_sqs.Options) {
            if localstackURL := os.Getenv("LOCALSTACK_ENDPOINT_URL"); localstackURL != "" {
                o.BaseEndpoint = aws.String(localstackURL)
            }
        },
    )
}


type QueueConfig struct {
    QueueUrl *string
    ProfileName string
    MessageParser messageParser
}


func NewQueue(queueConfig *QueueConfig) *Queue {
    client := getClient(queueConfig.ProfileName)
    return &Queue{
        client: client,
        config: queueConfig,
    }
}


type Queue struct {
    client *_sqs.Client
    config *QueueConfig
}


func (q *Queue) PullMessagesBatch() ([]dedup.QueueMessage, error) {
    var messages []dedup.QueueMessage
    result, err := q.client.ReceiveMessage(context.TODO(), &_sqs.ReceiveMessageInput{
        QueueUrl: q.config.QueueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 10,
        VisibilityTimeout: 600,
    })
    if err != nil {
        return messages, err
    }
    for _, rawMessage := range result.Messages {
        message, err := q.config.MessageParser(rawMessage)
        if err != nil {
            fmt.Println("Error parsing message", err)
            continue
        }
        messages = append(messages, message)
    }
    return messages, err
}


func (q *Queue) DeleteMessagesBatch(receiptHandles []string) {
    var entries []types.DeleteMessageBatchRequestEntry
    for i, receiptHandle := range receiptHandles {
        entries = append(entries, types.DeleteMessageBatchRequestEntry{
            Id: aws.String(fmt.Sprintf("message_%d", i)),
            ReceiptHandle: aws.String(receiptHandle),
        })
    }
    input := _sqs.DeleteMessageBatchInput{
        Entries:  entries,
        QueueUrl: q.config.QueueUrl,
    }
    result, err := q.client.DeleteMessageBatch(context.TODO(), &input)
    if err != nil {
        fmt.Println("Error deleting batch", err)
    }
    for _, failure := range result.Failed {
        fmt.Printf("Failed to delete message: ID %s. Error code: %s, Error message: %s\n", *failure.Id, *failure.Code, *failure.Message)
    }
}


func (q *Queue) ResetVisibilityBatch(receiptHandles []string) {
    var entries []types.ChangeMessageVisibilityBatchRequestEntry
    for i, receiptHandle := range receiptHandles {
        entries = append(entries, types.ChangeMessageVisibilityBatchRequestEntry{
            Id: aws.String(fmt.Sprintf("message_%d", i)),
            ReceiptHandle: aws.String(receiptHandle),
            VisibilityTimeout: 3,
        })
    }
    input := _sqs.ChangeMessageVisibilityBatchInput{
        Entries: entries,
        QueueUrl: q.config.QueueUrl,
    }
    result, err := q.client.ChangeMessageVisibilityBatch(context.TODO(), &input)
    if err != nil {
        fmt.Println("Error reseting visibility batch", err)
    }
    for _, fail := range result.Failed {
        fmt.Printf("Failed ID: %s, Code: %s, Message: %s\n", *fail.Id, *fail.Code, *fail.Message)
    }
}


func (q *Queue) PutMessagesBatch(messages []dedup.QueueMessage) error {
    var entries []types.SendMessageBatchRequestEntry
    for i, message := range messages {
        entries = append(entries, types.SendMessageBatchRequestEntry{
            Id: aws.String(fmt.Sprintf("message_%d", i)),
            MessageBody: aws.String(message.RawBody()),
        })
    }
    input := _sqs.SendMessageBatchInput{
        Entries: entries,
        QueueUrl: q.config.QueueUrl,
    }
    result, err := q.client.SendMessageBatch(context.TODO(), input)
    if err != nil {
        return fmt.Errorf("error sending message batch: %w", err)
    }
    if len(result.Failed) > 0 {
        for _, fail := range result.Failed {
            fmt.Printf("Failed to send message: ID %s. Error code: %s, Error message: %s\n",
                       *fail.Id, *fail.Code, *fail.Message)
        }
        return fmt.Errorf("some messages failed to send")
    }
    return nil
}
