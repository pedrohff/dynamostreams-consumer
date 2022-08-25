package shardstore

import (
	"context"
	"encoding/json"
	"github.com/pedrohff/dynamostreams-consumer"
	"io/ioutil"
	"os"
)

type FileStore struct {
	fileName string
	m        map[string]dynamostreamsconsumer.ShardPointer
}

func NewFileStore(fileName string) (FileStore, error) {
	pointers := make(map[string]dynamostreamsconsumer.ShardPointer)

	file, err := ioutil.ReadFile(fileName)
	if err != nil {
		if !os.IsNotExist(err) {
			return FileStore{}, err
		}
	} else if len(file) > 0 {

		err = json.Unmarshal(file, &pointers)
		if err != nil {
			return FileStore{}, err
		}
	}
	return FileStore{fileName: fileName, m: pointers}, nil
}

func (w FileStore) SetShardPointer(_ context.Context, pointer dynamostreamsconsumer.ShardPointer) error {
	w.m[pointer.ShardId] = pointer
	marshal, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(w.fileName, marshal, 777)
}

func (w FileStore) GetShardPointer(_ context.Context, i string) (dynamostreamsconsumer.ShardPointer, error) {
	return w.m[i], nil
}
