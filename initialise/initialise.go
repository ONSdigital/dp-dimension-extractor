package initialise

import (
	"errors"

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

const (
	// DimensionExtractedProducer represents a name for
	// the producer that writes to a dimesion extracted topic (DIMENSIONS_EXTRACTED_TOPIC)
	DimensionExtractedProducer = "dimension-extracted-producer"
	// DimensionExtractedErrProducer represents a name for
	// the producer that writes to an error topic (EVENT_REPORTER_TOPIC)
	DimensionExtractedErrProducer = "error-producer"
)

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
func (e *ExternalServiceList) GetProducer(kafkaBrokers []string, topic, name string, envMax int) (kafkaProducer kafka.Producer, err error) {
	kafkaProducer, err = kafka.NewProducer(kafkaBrokers, topic, envMax)
	if err == nil {
		switch {
		case name == DimensionExtractedProducer:
			e.DimensionExtractedProducer = true
		case name == DimensionExtractedErrProducer:
			e.DimensionExtractedErrProducer = true
		}
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
		return reporter.ImportErrorReporter{}, errors.New("Cannot create ImportErrorReporter because kafka producer 'DimensionExtractedErrProducer' is not available")
	}

	errorReporter, err = reporter.NewImportErrorReporter(dimensionExtractedErrProducer, serviceName)
	if err != nil {
		e.ErrorReporter = true
	}

	return
}
