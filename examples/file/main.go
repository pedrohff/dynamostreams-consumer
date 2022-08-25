package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	dynamostreamsconsumer "github.com/pedrohff/dynamostreams-consumer"
	"github.com/pedrohff/dynamostreams-consumer/shardstore"
	"os"
	"os/signal"
	"syscall"
)

var (
	awsRegion  = flag.String("awsRegion", "us-east-1", "aws region")
	awsProfile = flag.String("awsProfile", "sts", "aws profile")
	streamARN  = flag.String("streamARN", "", "aws dynamodb stream arn")
	fileName   = flag.String("fileName", "shardpointer.json", "file to store shard pointers")
)

func main() {
	flag.Parse()
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(*awsProfile),
		config.WithRegion(*awsRegion),
	)
	if err != nil {
		panic(err)
	}

	streamArn := aws.String(*streamARN)
	storage, err := shardstore.NewFileStore(*fileName)
	if err != nil {
		panic(err)
	}

	reader := dynamostreamsconsumer.NewDynamoStreamReader(*streamArn, storage)
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
		return nil
	})
	if err != nil {
		panic(err)
	}

}
