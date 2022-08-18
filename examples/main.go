package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	dynamostreamsconsumer "github.com/pedrohff/dynamostreams-consumer"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile("TODO"),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		panic(err)
	}

	streamArn := aws.String("TODO")
	storage, err := dynamostreamsconsumer.NewLocalShardPointerStorage("shardpointer.json")
	if err != nil {
		panic(err)
	}

	reader := dynamostreamsconsumer.NewDynamoStreamReader(*streamArn, storage)
	err = reader.Connect(ctx, cfg)
	if err != nil {
		panic(err)
	}

	err = reader.Read(ctx, func(m map[string]interface{}) error {
		marshal, err := json.Marshal(m)
		if err != nil {
			return err
		}
		fmt.Println(marshal)
		return nil
	})
	if err != nil {
		panic(err)
	}

}
