package initialise

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	s3client "github.com/ONSdigital/dp-s3"
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
	S3Clients                     bool
	ErrorReporter                 bool
	HealthCheck                   bool
}

// KafkaProducerName : Type for kafka producer name used by iota constants
type KafkaProducerName int

// Possible names of Kafka Producers
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

// GetS3Clients returns a map of AWS S3 clients corresponding to the list of BucketNames
// and the AWS region provided in the configuration
func (e *ExternalServiceList) GetS3Clients(cfg *config.Config) (awsSession *session.Session, s3Clients map[string]service.S3Client, err error) {
	// establish AWS session
	awsSession, err = session.NewSession(&aws.Config{Region: &cfg.AWSRegion})
	if err != nil {
		return
	}

	// create S3 clients for expected bucket names, so that they can be health-checked
	s3Clients = make(map[string]service.S3Client)
	for _, bucketName := range cfg.BucketNames {
		s3Clients[bucketName] = s3client.NewClientWithSession(bucketName, !cfg.EncryptionDisabled, awsSession)
	}
	e.S3Clients = true

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

// GetHealthCheck creates a healthcheck with versionInfo
func (e *ExternalServiceList) GetHealthCheck(cfg *config.Config, buildTime, gitCommit, version string) (healthcheck.HealthCheck, error) {

	// Create healthcheck object with versionInfo
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return healthcheck.HealthCheck{}, err
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckCriticalTimeout, cfg.HealthCheckInterval)

	e.HealthCheck = true

	return hc, nil
}
