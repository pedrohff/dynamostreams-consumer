# DynamoStreams Consumer

Wrapper to read from a Dynamo Stream from all shards continuously from the beginning and store the last sequence number
from each shard.

The shard's pointers can be stored following the `ShardPointerStorage` interface, and below are the implementations:

| Storage implementation | Source                       | Example                             | Description                                              |
|------------------------|------------------------------|-------------------------------------|----------------------------------------------------------|
| File                   | [source](shardstore/file.go) | [example](shardstore/file/main.go)  | Stores the shard's pointer in a file using json encoding |
| MongoDB                | [source](mongo/file.go)      | [example](shardstore/mongo/main.go) | Stores shard's pointers in a MongoDB Collection          |


