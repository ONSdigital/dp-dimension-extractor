package initialise

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	vault "github.com/ONSdigital/dp-vault"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

// ExternalServiceList represents a list of services
type ExternalServiceList struct {
	Consumer                      bool
	DimensionExtractedProducer    bool
	DimensionExtractedErrProducer bool
	Vault                         bool
	AwsSession                    bool
	ErrorReporter                 bool
}

// KafkaProducerName : Type for kafka producer name used by iota constants
type KafkaProducerName int

// Possible names of Kafa Producers
const (
	DimensionExtracted = iota
	DimensionExtractedErr
)

var kafkaProducerNames = []string{"DimensionExtracted", "DimensionExtractedErr"}

// Values of the kafka producers names
func (k KafkaProducerName) String() string {
	return kafkaProducerNames[k]
}

// GetConsumer returns a kafka consumer, which might not be initialised yet.
func (e *ExternalServiceList) GetConsumer(ctx context.Context, kafkaBrokers []string, cfg *config.Config) (kafkaConsumer *kafka.ConsumerGroup, err error) {
	cgChannels := kafka.CreateConsumerGroupChannels(true)
	kafkaConsumer, err = kafka.NewConsumerGroup(
		ctx,
		kafkaBrokers,
		cfg.InputFileAvailableTopic,
		cfg.InputFileAvailableGroup,
		kafka.OffsetNewest,
		true,
		cgChannels,
	)
	if err != nil {
		return
	}

	e.Consumer = true
	return
}

// GetProducer returns a kafka producer, which might not be initialised yet.
func (e *ExternalServiceList) GetProducer(ctx context.Context, kafkaBrokers []string, topic string, name KafkaProducerName, envMax int) (kafkaProducer *kafka.Producer, err error) {
	pChannels := kafka.CreateProducerChannels()
	kafkaProducer, err = kafka.NewProducer(ctx, kafkaBrokers, topic, envMax, pChannels)
	if err != nil {
		return
	}

	switch {
	case name == DimensionExtracted:
		e.DimensionExtractedProducer = true
	case name == DimensionExtractedErr:
		e.DimensionExtractedErrProducer = true
	default:
		err = fmt.Errorf("Kafka producer name not recognised: '%s'. Valid names: %v", name.String(), kafkaProducerNames)
	}

	return
}

// GetVault returns a vault client
func (e *ExternalServiceList) GetVault(cfg *config.Config, retries int) (client *vault.Client, err error) {
	client, err = vault.CreateClient(cfg.VaultToken, cfg.VaultAddr, retries)
	if err != nil {
		return
	}

	e.Vault = true
	return
}

// GetAwsSession returns an AWS client for the AWS region provided in Config
func (e *ExternalServiceList) GetAwsSession(cfg *config.Config) (awsSession *session.Session, err error) {
	awsSession, err = session.NewSession(&aws.Config{Region: &cfg.AWSRegion})
	if err != nil {
		return
	}

	e.AwsSession = true
	return
}

// GetImportErrorReporter returns an ErrorImportReporter to send error reports to the import-reporter (only if DimensionExtractedErrProducer is available)
func (e *ExternalServiceList) GetImportErrorReporter(dimensionExtractedErrProducer reporter.KafkaProducer, serviceName string) (errorReporter reporter.ImportErrorReporter, err error) {
	if !e.DimensionExtractedErrProducer {
		return reporter.ImportErrorReporter{},
			fmt.Errorf("Cannot create ImportErrorReporter because kafka producer '%s' is not available", kafkaProducerNames[DimensionExtractedErr])
	}

	errorReporter, err = reporter.NewImportErrorReporter(dimensionExtractedErrProducer, serviceName)
	if err != nil {
		return
	}

	e.ErrorReporter = true
	return
}
