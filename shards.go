package dynamostreamsconsumer

import (
	"context"
	"time"
)

type ShardPointer struct {
	ShardId            string    `json:"shardId" bson:"_id"`
	LastSequenceNumber string    `json:"lastSequenceNumber"`
	Finished           bool      `json:"finished"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

type ShardPointerStorage interface {
	SetShardPointer(ctx context.Context, pointer ShardPointer) error
	GetShardPointer(ctx context.Context, shardId string) (ShardPointer, error)
}
