package initialise

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	s3client "github.com/ONSdigital/dp-s3/v3"
	vault "github.com/ONSdigital/dp-vault"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

var kafkaOffset = kafka.OffsetOldest

// Values of the kafka producers names
func (k KafkaProducerName) String() string {
	return kafkaProducerNames[k]
}

// GetConsumer returns a kafka consumer, which might not be initialised yet.
func (e *ExternalServiceList) GetConsumer(ctx context.Context, KafkaConfig *config.KafkaConfig) (kafkaConsumer *kafka.ConsumerGroup, err error) {
	cgChannels := kafka.CreateConsumerGroupChannels(1)
	cgConfig := &kafka.ConsumerGroupConfig{
		Offset:       &kafkaOffset,
		KafkaVersion: &KafkaConfig.Version,
	}
	if KafkaConfig.SecProtocol == config.KafkaTLSProtocolFlag {
		cgConfig.SecurityConfig = kafka.GetSecurityConfig(
			KafkaConfig.SecCACerts,
			KafkaConfig.SecClientCert,
			KafkaConfig.SecClientKey,
			KafkaConfig.SecSkipVerify,
		)
	}
	kafkaConsumer, err = kafka.NewConsumerGroup(
		ctx,
		KafkaConfig.BindAddr,
		KafkaConfig.InputFileAvailableTopic,
		KafkaConfig.InputFileAvailableGroup,
		cgChannels,
		cgConfig,
	)
	if err != nil {
		return
	}

	e.Consumer = true
	return
}

// GetProducer returns a kafka producer, which might not be initialised yet.
func (e *ExternalServiceList) GetProducer(ctx context.Context, kafkaConfig *config.KafkaConfig, topic string, name KafkaProducerName, envMax int) (kafkaProducer *kafka.Producer, err error) {
	pChannels := kafka.CreateProducerChannels()
	pConfig := &kafka.ProducerConfig{
		KafkaVersion:    &kafkaConfig.Version,
		MaxMessageBytes: &envMax,
	}
	if kafkaConfig.SecProtocol == config.KafkaTLSProtocolFlag {
		pConfig.SecurityConfig = kafka.GetSecurityConfig(
			kafkaConfig.SecCACerts,
			kafkaConfig.SecClientCert,
			kafkaConfig.SecClientKey,
			kafkaConfig.SecSkipVerify,
		)
	}
	kafkaProducer, err = kafka.NewProducer(ctx, kafkaConfig.BindAddr, topic, pChannels, pConfig)
	if err != nil {
		return
	}

	switch {
	case name == DimensionExtracted:
		e.DimensionExtractedProducer = true
	case name == DimensionExtractedErr:
		e.DimensionExtractedErrProducer = true
	default:
		err = fmt.Errorf("kafka producer name not recognised: '%s' Valid names: %v", name.String(), kafkaProducerNames)
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
func (e *ExternalServiceList) GetS3Clients(ctx context.Context, cfg *config.Config) (*aws.Config, map[string]service.S3Client, error) {
	if cfg.LocalstackHost != "" {
		awsConfig, err := awsConfig.LoadDefaultConfig(ctx,
			awsConfig.WithRegion(cfg.AWSRegion),
			awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		)
		if err != nil {
			return nil, nil, err
		}

		s3Clients := make(map[string]service.S3Client)
		for _, bucketName := range cfg.BucketNames {
			s3Clients[bucketName] = s3client.NewClientWithConfig(bucketName, awsConfig, func(o *s3.Options) {
				o.BaseEndpoint = aws.String(cfg.LocalstackHost)
				o.UsePathStyle = true
			})
		}
		e.S3Clients = true

		return &awsConfig, s3Clients, nil
	}

	// establish AWS config
	awsConfig, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		return nil, nil, err
	}

	// create S3 clients for expected bucket names, so that they can be health-checked
	s3Clients := make(map[string]service.S3Client)
	for _, bucketName := range cfg.BucketNames {
		s3Clients[bucketName] = s3client.NewClientWithConfig(bucketName, awsConfig)
	}
	e.S3Clients = true

	return &awsConfig, s3Clients, nil
}

// GetImportErrorReporter returns an ErrorImportReporter to send error reports to the import-reporter (only if DimensionExtractedErrProducer is available)
func (e *ExternalServiceList) GetImportErrorReporter(dimensionExtractedErrProducer reporter.KafkaProducer, serviceName string) (errorReporter reporter.ImportErrorReporter, err error) {
	if !e.DimensionExtractedErrProducer {
		return reporter.ImportErrorReporter{},
			fmt.Errorf("cannot create ImportErrorReporter because kafka producer '%s' is not available", kafkaProducerNames[DimensionExtractedErr])
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
