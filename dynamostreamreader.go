package dynamostreamsconsumer

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	types2 "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"sync"
	"time"
)

// DynamoStreamReader
type DynamoStreamReader struct {
	storage    ShardPointerStorage
	ddbsClient *dynamodbstreams.Client
	stream     *dynamodbstreams.DescribeStreamOutput
	streamArn  string
}

type MessageProcessor func(ctx context.Context, messageRead map[string]interface{}) error

// NewDynamoStreamReader
func NewDynamoStreamReader(streamArn string, shardPointerWriter ShardPointerStorage) *DynamoStreamReader {
	return &DynamoStreamReader{streamArn: streamArn, storage: shardPointerWriter}
}

type AWSDynamoStream interface {
	GetRecords(ctx context.Context, params *dynamodbstreams.GetRecordsInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error)
}

// Connect
func (r *DynamoStreamReader) Connect(ctx context.Context, awsAuthConfig aws.Config) error {
	var err error
	r.ddbsClient = dynamodbstreams.NewFromConfig(awsAuthConfig)
	r.stream, err = r.ddbsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: &r.streamArn})
	if err != nil {
		return fmt.Errorf("could not describe dynamo stream: %w", err)
	}
	return nil
}

// Read
func (r *DynamoStreamReader) Read(ctx context.Context, processor MessageProcessor) error {
	if r.ddbsClient == nil {
		return fmt.Errorf("dynamo is not connected, use the Connect method prior to reading")
	}

	errChan := make(chan error)
	waitChan := make(chan struct{})
	wg := sync.WaitGroup{}

	shards := r.stream.StreamDescription.Shards
	wg.Add(len(shards))
	go func() {
		for _, shard := range shards {
			go func(s types2.Shard) {
				iterateErr := r.getShardIteratorAndReadShard(ctx, processor, s)
				if iterateErr != nil {
					errChan <- iterateErr
				}
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(waitChan)
	}()

	select {
	case err := <-errChan:
		return err
	case <-waitChan:
		return nil
	}
}

// getShardIteratorAndReadShard
func (r DynamoStreamReader) getShardIteratorAndReadShard(ctx context.Context, processor MessageProcessor, shard types2.Shard) error {
	pointer, err := r.storage.GetShardPointer(context.Background(), *shard.ShardId)
	if err != nil {
		return err
	}
	if pointer.Finished {
		return nil
	}
	iterator, err := r.getShardIterator(ctx, *shard.ShardId, pointer.LastSequenceNumber)
	if err != nil {
		return err
	}

	_, shardPointerResult, readShardErr := r.readShard(ctx, *shard.ShardId, pointer.LastSequenceNumber, *iterator.ShardIterator, processor)
	if readShardErr != nil {
		shardPointerResult.UpdatedAt = time.Now()
		_ = r.storage.SetShardPointer(context.Background(), shardPointerResult)
		return readShardErr
	}
	shardPointerResult.UpdatedAt = time.Now()
	setShardPointerErr := r.storage.SetShardPointer(ctx, shardPointerResult)
	if setShardPointerErr != nil {
		return setShardPointerErr
	}
	return nil
}

// getShardIterator
func (r *DynamoStreamReader) getShardIterator(ctx context.Context, shardId, lastSequenceNumber string) (*dynamodbstreams.GetShardIteratorOutput, error) {
	shardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           &shardId,
		ShardIteratorType: types2.ShardIteratorTypeTrimHorizon,
		StreamArn:         &r.streamArn,
	}

	// if we have the sequence number, we should swap to ShardIteratorTypeAfterSequenceNumber
	if lastSequenceNumber != "" {
		shardIteratorInput.ShardIteratorType = types2.ShardIteratorTypeAfterSequenceNumber
		shardIteratorInput.SequenceNumber = &lastSequenceNumber
	}

	return r.ddbsClient.GetShardIterator(ctx, shardIteratorInput)
}

// readShard
func (r *DynamoStreamReader) readShard(ctx context.Context, shardId, lastSequenceNumber, shardIteratorId string, processor MessageProcessor) (int, ShardPointer, error) {
	counter := 0
	iterationCounter := 0
	for {
		select {
		case <-ctx.Done(): // helper for stopping the loop based on context
			return counter, ShardPointer{ShardId: shardId, LastSequenceNumber: lastSequenceNumber}, ctx.Err()
		default:
			iterationCounter++

			// GetRecords sometimes brings empty data, that's a normal behavior
			records, getRecordsErr := r.ddbsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: &shardIteratorId,
			})
			if getRecordsErr != nil {
				return counter, ShardPointer{ShardId: shardId, LastSequenceNumber: lastSequenceNumber}, getRecordsErr
			}

			// process the data
			if len(records.Records) > 0 {
				for _, d := range records.Records {
					lastSequenceNumber = *d.Dynamodb.SequenceNumber
					counter++
					resultMap, parseErr := r.parseDynamoStreamRecordToMap(d)
					if parseErr != nil {
						return counter, ShardPointer{ShardId: shardId, LastSequenceNumber: lastSequenceNumber}, parseErr
					}
					err := processor(ctx, resultMap)
					if err != nil {
						return 0, ShardPointer{}, err
					}
				}
			}

			if r.shardIsFinished(records, shardIteratorId) {
				return counter, ShardPointer{ShardId: shardId, LastSequenceNumber: lastSequenceNumber, Finished: true}, nil
			}
			shardIteratorId = *records.NextShardIterator
		}
	}
}

// shardIsFinished validates if the shard no longer will receive data following Dynamo's docs
//
// NextShardIterator:
//
//	The next position in the shard from which to start sequentially reading stream records.
//	If set to null, the shard has been closed and the requested iterator will not return any more data.
//
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html#API_streams_GetRecords_RequestSyntax
func (r *DynamoStreamReader) shardIsFinished(records *dynamodbstreams.GetRecordsOutput, shardIteratorId string) bool {
	return records.NextShardIterator == nil || shardIteratorId == *records.NextShardIterator
}

func (r *DynamoStreamReader) parseDynamoStreamRecordToMap(d types2.Record) (map[string]interface{}, error) {
	temp := map[string]interface{}{}
	streamsMap, err := attributevalue.FromDynamoDBStreamsMap(d.Dynamodb.NewImage)
	if err != nil {
		return nil, err
	}
	err = attributevalue.UnmarshalMap(streamsMap, &temp)
	if err != nil {
		return nil, err
	}
	return temp, nil
}
