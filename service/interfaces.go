package service

import (
	"io"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"golang.org/x/net/context"
)

//go:generate moq -out ./mock/vault.go -pkg mock . VaultClient
//go:generate moq -out ./mock/s3.go -pkg mock . S3Client
//go:generate moq -out ./mock/dataset.go -pkg mock . DatasetClient
//go:generate moq -out ./mock/kafka.go -pkg mock . KafkaProducer

// VaultClient is an interface to represent methods called to action upon Vault
type VaultClient interface {
	ReadKey(path, key string) (string, error)
}

// S3Client is an interface to represent methods called to action upon AWS S3
type S3Client interface {
	Get(ctx context.Context, key string) (io.ReadCloser, *int64, error)
	GetWithPSK(ctx context.Context, key string, psk []byte) (io.ReadCloser, *int64, error)
	Checker(ctx context.Context, state *healthcheck.CheckState) error
}

// DatasetClient is an interface to represent methods called to action upon Dataset REST interface
type DatasetClient interface {
	GetInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID, ifMatch string) (m dataset.Instance, eTag string, err error)
	PostInstanceDimensions(ctx context.Context, serviceAuthToken, instanceID string, data dataset.OptionPost, ifMatch string) (eTag string, err error)
	PutInstanceData(ctx context.Context, serviceAuthToken, instanceID string, data dataset.JobInstance, ifMatch string) (eTag string, err error)
}

// KafkaProducer is an interface to represent methods called to action upon Kafka to produce messages
type KafkaProducer interface {
	Channels() *kafka.ProducerChannels
	Close(ctx context.Context) (err error)
}
