package config

import (
	"encoding/json"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config is the filing resource handler config
type Config struct {
	AWSPrivateKey            string        `envconfig:"RSA_PRIVATE_KEY"                json:"-"`
	AWSRegion                string        `envconfig:"AWS_REGION"`
	BindAddr                 string        `envconfig:"BIND_ADDR"`
	Brokers                  []string      `envconfig:"KAFKA_ADDR"                     json:"-"`
	DatasetAPIURL            string        `envconfig:"DATASET_API_URL"`
	DatasetAPIAuthToken      string        `envconfig:"DATASET_API_AUTH_TOKEN"         json:"-"`
	DimensionsExtractedTopic string        `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	DimensionExtractorURL    string        `envconfig:"DIMENSION_EXTRACTOR_URL"`
	EncryptionDisabled       bool          `envconfig:"ENCRYPTION_DISABLED"`
	EventReporterTopic       string        `envconfig:"EVENT_REPORTER_TOPIC"`
	GracefulShutdownTimeout  time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	InputFileAvailableGroup  string        `envconfig:"INPUT_FILE_AVAILABLE_GROUP"`
	InputFileAvailableTopic  string        `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	KafkaMaxBytes            string        `envconfig:"KAFKA_MAX_BYTES"`
	MaxRetries               int           `envconfig:"REQUEST_MAX_RETRIES"`
	ServiceAuthToken         string        `envconfig:"SERVICE_AUTH_TOKEN"             json:"-"`
}

var cfg *Config

// Get configures the application and returns the configuration
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		AWSRegion:                "eu-west-1",
		BindAddr:                 ":21400",
		Brokers:                  []string{"localhost:9092"},
		DimensionsExtractedTopic: "dimensions-extracted",
		DimensionExtractorURL:    "http://localhost:21400",
		DatasetAPIURL:            "http://localhost:22000",
		DatasetAPIAuthToken:      "FD0108EA-825D-411C-9B1D-41EF7727F465",
		EncryptionDisabled:       true,
		EventReporterTopic:       "report-events",
		GracefulShutdownTimeout:  5 * time.Second,
		InputFileAvailableTopic:  "input-file-available",
		InputFileAvailableGroup:  "input-file-available",
		KafkaMaxBytes:            "2000000",
		MaxRetries:               3,
		ServiceAuthToken:         "E45F9BFC-3854-46AE-8187-11326A4E00F4",
	}

	return cfg, envconfig.Process("", cfg)
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Config) String() string {
	json, _ := json.Marshal(config)
	return string(json)
}
