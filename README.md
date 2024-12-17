# go-sqs-deduplication

### What

The batch deduplicator works by taking as many messages off an AWS SQS queue as possible (up to `maxInflight`) and deleting duplicates based on a unique identifier. This cycle repeats until all messages in the queue have been processed. If the number of unique messages exceeds `maxInflight`, the unique messages are persisted to storage (in the form of another isolated queue) and deleted from the original queue. Finally, the visibility of unique messages is reset or moved back from the storage queue so that they reappear on the original queue.

Pulling, deleting, reseting, and moving messages happens concurrently by `numWorkers` workers.


The deduplicator expects SQS messages in following format:

```json
{
    "data": {
        "uuid": "abc123"
     }
}
```
Where `uuid` is the unique identifier used to deduplicate messages.

The code can be extended to work with other queues or message formats.

### Usage

Run once:
```bash
go run cmd/dedup.go -queueURL=someURL -storageQueueURL=someOtherURL -numWorkers=100  -profileName=someProfile
```

Run forever:
```bash
$ go run cmd/dedup.go -queueURL=someURL -storageQueueURL=someOtherURL -numWorkers=50 -runForever -secondsToSleepBetweenRuns=600
```

Help:
```
  -maxInflight int
    	Maximum number of inflight messages allowed by queue (default 100000)
  -numWorkers int
    	Number of concurrent workers to use (default 20)
  -profileName string
    	AWS profile to use
  -queueURL string
    	SQS URL (required)
  -runForever
    	Runs in a loop with secondsToSleepBetweenRuns
  -secondsToSleepBetweenRuns int
    	Time to sleep between runs if running forever (default 60)
  -storageQueueURL string
    	SQS URL used for storage (required)
  -version
    	Show version
```

Run tests:
```
$ go test ./...
```