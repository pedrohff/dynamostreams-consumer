package dynamostreamsconsumer

type ShardPointer struct {
	ShardId            string `json:"shard_id"`
	LastSequenceNumber string `json:"last_sequence_number"`
	Finished           bool   `json:"finished"`
}

type ShardPointerStorage interface {
	SetShardPointer(pointer ShardPointer) error
	GetShardPointer(string) (ShardPointer, error)
}
