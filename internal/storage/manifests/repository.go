package manifests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

var ErrConflict = errors.New("manifest conflict")

type Record struct {
	Key          string    `json:"key"`
	PartitionKey string    `json:"partition_key"`
	Start        time.Time `json:"start"`
	End          time.Time `json:"end"`
	Checksum     string    `json:"checksum"`
	Payload      []byte    `json:"payload"`
	CreatedAt    time.Time `json:"created_at"`
}

type Repository interface {
	Put(context.Context, Record) (Record, error)
	Get(context.Context, string) (Record, error)
	List(context.Context, string) ([]Record, error)
	Delete(context.Context, string) error
}

func Checksum(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
