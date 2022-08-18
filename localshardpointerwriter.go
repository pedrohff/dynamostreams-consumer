package dynamostreamsconsumer

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type LocalShardPointerStorage struct {
	fileName string
	m        map[string]ShardPointer
}

func NewLocalShardPointerStorage(fileName string) (LocalShardPointerStorage, error) {
	pointers := make(map[string]ShardPointer)

	file, err := ioutil.ReadFile(fileName)
	if err != nil {
		if !os.IsNotExist(err) {
			return LocalShardPointerStorage{}, err
		}
	} else if len(file) > 0 {

		err = json.Unmarshal(file, &pointers)
		if err != nil {
			return LocalShardPointerStorage{}, err
		}
	}
	return LocalShardPointerStorage{fileName: fileName, m: pointers}, nil
}

func (w LocalShardPointerStorage) SetShardPointer(pointer ShardPointer) error {
	w.m[pointer.ShardId] = pointer
	marshal, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(w.fileName, marshal, 777)
}

func (w LocalShardPointerStorage) GetShardPointer(i string) (ShardPointer, error) {
	return w.m[i], nil
}
