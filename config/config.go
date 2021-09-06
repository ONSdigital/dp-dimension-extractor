package config

import (
	"encoding/json"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config is the filing resource handler config
type Config struct {
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	DatasetAPIURL              string        `envconfig:"DATASET_API_URL"`
	DimensionsExtractedTopic   string        `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	DimensionExtractorURL      string        `envconfig:"DIMENSION_EXTRACTOR_URL"`
	EncryptionDisabled         bool          `envconfig:"ENCRYPTION_DISABLED"`
	EventReporterTopic         string        `envconfig:"EVENT_REPORTER_TOPIC"`
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	InputFileAvailableGroup    string        `envconfig:"INPUT_FILE_AVAILABLE_GROUP"`
	InputFileAvailableTopic    string        `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	KafkaAddr                  []string      `envconfig:"KAFKA_ADDR"                     json:"-"`
	KafkaMaxBytes              string        `envconfig:"KAFKA_MAX_BYTES"`
	KafkaVersion               string        `envconfig:"KAFKA_VERSION"`
	KafkaSecProtocol           string        `envconfig:"KAFKA_SEC_PROTO"`
	KafkaSecCACerts            string        `envconfig:"KAFKA_SEC_CA_CERTS"`
	KafkaSecClientCert         string        `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	KafkaSecClientKey          string        `envconfig:"KAFKA_SEC_CLIENT_KEY"           json:"-"`
	KafkaSecSkipVerify         bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	MaxRetries                 int           `envconfig:"REQUEST_MAX_RETRIES"`
	VaultAddr                  string        `envconfig:"VAULT_ADDR"`
	VaultToken                 string        `envconfig:"VAULT_TOKEN"                    json:"-"`
	VaultPath                  string        `envconfig:"VAULT_PATH"`
	ServiceAuthToken           string        `envconfig:"SERVICE_AUTH_TOKEN"             json:"-"`
	ZebedeeURL                 string        `envconfig:"ZEBEDEE_URL"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	AWSRegion                  string        `envconfig:"AWS_REGION"`
	BucketNames                []string      `envconfig:"BUCKET_NAMES"                   json:"-"`
}

var cfg *Config

// Get configures the application and returns the configuration
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		BindAddr:                   ":21400",
		DimensionsExtractedTopic:   "dimensions-extracted",
		DimensionExtractorURL:      "http://localhost:21400",
		DatasetAPIURL:              "http://localhost:22000",
		EncryptionDisabled:         false,
		EventReporterTopic:         "report-events",
		GracefulShutdownTimeout:    5 * time.Second,
		InputFileAvailableTopic:    "input-file-available",
		InputFileAvailableGroup:    "input-file-available",
		KafkaAddr:                  []string{"localhost:9092"},
		KafkaMaxBytes:              "2000000",
		KafkaVersion:               "1.0.2",
		MaxRetries:                 3,
		VaultAddr:                  "http://localhost:8200",
		VaultToken:                 "",
		VaultPath:                  "secret/shared/psk",
		ServiceAuthToken:           "E45F9BFC-3854-46AE-8187-11326A4E00F4",
		ZebedeeURL:                 "http://localhost:8082",
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		AWSRegion:                  "eu-west-1",
		BucketNames:                []string{"dp-frontend-florence-file-uploads"},
	}

	if err := envconfig.Process("", cfg); err != nil {
		return cfg, err
	}

	cfg.ServiceAuthToken = "Bearer " + cfg.ServiceAuthToken

	return cfg, nil
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Config) String() string {
	json, _ := json.Marshal(config)
	return string(json)
}
