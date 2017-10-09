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
	DimensionsExtractedTopic string        `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	DimensionExtractorURL    string        `envconfig:"DIMENSION_EXTRACTOR_URL"`
	InputFileAvailableGroup  string        `envconfig:"INPUT_FILE_AVAILABLE_GROUP"`
	InputFileAvailableTopic  string        `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	KafkaMaxBytes            string        `envconfig:"KAFKA_MAX_BYTES"`
	MaxRetries               int           `envconfig:"REQUEST_MAX_RETRIES"`
	ShutdownTimeout          time.Duration `envconfig:"SHUTDOWN_TIMEOUT"`
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
		InputFileAvailableTopic:  "input-file-available",
		InputFileAvailableGroup:  "input-file-available",
		KafkaMaxBytes:            "2000000",
		MaxRetries:               3,
		ShutdownTimeout:          5 * time.Second,
	}

	return cfg, envconfig.Process("", cfg)
}
