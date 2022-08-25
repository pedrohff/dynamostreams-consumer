package shardstore

import (
	"context"
	dynamostreamsconsumer "github.com/pedrohff/dynamostreams-consumer"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStore struct {
	collection *mongo.Collection
}

func NewMongoStore(col *mongo.Collection) *MongoStore {
	return &MongoStore{collection: col}
}

func (m MongoStore) SetShardPointer(ctx context.Context, pointer dynamostreamsconsumer.ShardPointer) error {
	opts := options.Update().SetUpsert(true)
	filter := bson.D{{"_id", pointer.ShardId}}
	set := bson.M{"$set": pointer}
	_, err := m.collection.UpdateOne(ctx, filter, set, opts)
	return err
}
func (m MongoStore) GetShardPointer(ctx context.Context, shardId string) (dynamostreamsconsumer.ShardPointer, error) {
	result := m.collection.FindOne(ctx, bson.D{{"_id", shardId}})
	s := dynamostreamsconsumer.ShardPointer{}
	err := result.Decode(&s)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return dynamostreamsconsumer.ShardPointer{}, nil
		}
		return dynamostreamsconsumer.ShardPointer{}, err
	}
	return s, nil
}
