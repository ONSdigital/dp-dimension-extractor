package initialise

import (
	"fmt"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/vault"
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

var kafkaProducersNames = []string{"DimensionExtracted", "DimensionExtractedErr"}

// Values of the kafka producers names
func (k KafkaProducerName) String() string {
	// return [...]string{"DimensionExtracted", "DimensionExtractedErr"}[k]
	return kafkaProducersNames[k]
}

// GetConsumer returns an initialised kafka consumer
func (e *ExternalServiceList) GetConsumer(kafkaBrokers []string, cfg *config.Config) (kafkaConsumer *kafka.ConsumerGroup, err error) {
	kafkaConsumer, err = kafka.NewSyncConsumer(
		kafkaBrokers,
		cfg.InputFileAvailableTopic,
		cfg.InputFileAvailableGroup,
		kafka.OffsetNewest,
	)

	if err == nil {
		e.Consumer = true
	}

	return
}

// GetProducer returns a kafka producer
func (e *ExternalServiceList) GetProducer(kafkaBrokers []string, topic string, name KafkaProducerName, envMax int) (kafkaProducer kafka.Producer, err error) {
	kafkaProducer, err = kafka.NewProducer(kafkaBrokers, topic, envMax)
	if err != nil {
		return
	}

	switch {
	case name == DimensionExtracted:
		e.DimensionExtractedProducer = true
	case name == DimensionExtractedErr:
		e.DimensionExtractedErrProducer = true
	default:
		err = fmt.Errorf("Kafka producer name not recognised: '%s'. Valid names: %v", name.String(), kafkaProducersNames)
	}

	return
}

// GetVault returns a vault client
func (e *ExternalServiceList) GetVault(cfg *config.Config, retries int) (client *vault.VaultClient, err error) {
	client, err = vault.CreateVaultClient(cfg.VaultToken, cfg.VaultAddr, retries)
	if err == nil {
		e.Vault = true
	}

	return
}

// GetAwsSession returns an AWS client for the AWS region provided in Config
func (e *ExternalServiceList) GetAwsSession(cfg *config.Config) (awsSession *session.Session, err error) {
	awsSession, err = session.NewSession(&aws.Config{Region: &cfg.AWSRegion})
	if err != nil {
		e.AwsSession = true
	}

	return
}

// GetImportErrorReporter returns an ErrorImportReporter to send error reports to the import-reporter (only if DimensionExtractedErrProducer is available)
func (e *ExternalServiceList) GetImportErrorReporter(dimensionExtractedErrProducer reporter.KafkaProducer, serviceName string) (errorReporter reporter.ImportErrorReporter, err error) {
	if !e.DimensionExtractedErrProducer {
		return reporter.ImportErrorReporter{},
			fmt.Errorf("Cannot create ImportErrorReporter because kafka producer '%s' is not available", kafkaProducersNames[DimensionExtractedErr])
	}

	errorReporter, err = reporter.NewImportErrorReporter(dimensionExtractedErrProducer, serviceName)
	if err != nil {
		e.ErrorReporter = true
	}

	return
}
