package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config is the filing resource handler config
type Config struct {
	AWSRegion                string        `envconfig:"AWS_REGION"`
	BindAddr                 string        `envconfig:"BIND_ADDR"`
	Brokers                  []string      `envconfig:"KAFKA_ADDR"`
	DatasetAPIURL            string        `envconfig:"DATASET_API_URL"`
	DatasetAPIAuthToken      string        `envconfig:"DATASET_API_AUTH_TOKEN"`
	DatasetAPITimeout        time.Duration `envconfig:"DATASET_API_TIMEOUT"`
	DimensionsExtractedTopic string        `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	DimensionExtractorURL    string        `envconfig:"DIMENSION_EXTRACTOR_URL"`
	InputFileAvailableGroup  string        `envconfig:"INPUT_FILE_AVAILABLE_GROUP"`
	InputFileAvailableTopic  string        `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	KafkaMaxBytes            string        `envconfig:"KAFKA_MAX_BYTES"`
	MaxRetries               int           `envconfig:"REQUEST_MAX_RETRIES"`
	GracefulShutdownTimeout  time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	DimensionBatchMaxSize    int           `envconfig:"DIMENSION_BATCH_MAX_SIZE"`
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
		DatasetAPITimeout:        55 * time.Second,
		InputFileAvailableTopic:  "input-file-available",
		InputFileAvailableGroup:  "dp-dimension-extractor",
		KafkaMaxBytes:            "2000000",
		MaxRetries:               3,
		GracefulShutdownTimeout:  5 * time.Second,
		DimensionBatchMaxSize:    1000,
	}

	return cfg, envconfig.Process("", cfg)
}
