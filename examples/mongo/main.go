package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	dynamostreamsconsumer "github.com/pedrohff/dynamostreams-consumer"
	"github.com/pedrohff/dynamostreams-consumer/shardstore"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"os/signal"
	"syscall"
)

var (
	awsRegion       = flag.String("awsRegion", "us-east-1", "aws region")
	awsProfile      = flag.String("awsProfile", "sts", "aws profile")
	streamARN       = flag.String("streamARN", "", "aws dynamodb stream arn")
	mongoURI        = flag.String("mongoURI", "mongodb://root:example@localhost:27017", "mongodb connection uri")
	mongoDatabase   = flag.String("mongoDatabase", "streams", "mongo's database")
	mongoCollection = flag.String("mongoCollection", "store", "mongo's collection for storing shards")
)

func main() {
	flag.Parse()
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*awsRegion),
		config.WithSharedConfigProfile(*awsProfile),
	)
	if err != nil {
		panic(err)
	}

	connect, err := mongo.Connect(ctx, options.Client().ApplyURI(*mongoURI))
	if err != nil {
		panic(err)
		return
	}
	collection := connect.Database(*mongoDatabase).Collection(*mongoCollection)
	storage := shardstore.NewMongoStore(collection)
	if err != nil {
		panic(err)
	}

	reader := dynamostreamsconsumer.NewDynamoStreamReader(*streamARN, storage)
	err = reader.Connect(ctx, cfg)
	if err != nil {
		panic(err)
	}

	err = reader.Read(ctx, func(_ context.Context, m map[string]interface{}) error {
		marshal, err := json.Marshal(m)
		if err != nil {
			return err
		}
		fmt.Println(string(marshal))
		_ = marshal
		return nil
	})

}
