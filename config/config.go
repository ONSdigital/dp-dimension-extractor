package config

import "github.com/ian-kent/gofigure"

// Config is the filing resource handler config
type Config struct {
	AWSRegion                string   `env:"AWS_REGION" flag:"aws-region" flagDesc:"The AWS region to use"`
	BindAddr                 string   `env:"BIND_ADDR" flag:"bind-addr" flagDesc:"The port to bind to"`
	Brokers                  []string `env:"KAFKA_ADDR" flag:"kafka-addr" flagDesc:"The Kafka broker addresses"`
	DimensionsExtractedTopic string   `env:"DIMENSIONS_EXTRACTED_TOPIC" flag:"dimensions-extracted-topic" flagDesc:"The Kafka topic to write dimension messages to"`
	ImportAPIURL             string   `env:"IMPORT_API_URL" flag:"import-api-url" flagDesc:"The import api url"`
	ImportAPIAuthToken       string   `env:"IMPORT_AUTH_TOKEN" flag:"import-auth-token" flagDesc:"Authentication token for access to import API"`
	InputFileAvailableGroup  string   `env:"INPUT_FILE_AVAILABLE_GROUP" flag:"input-file-available-group" flagDesc:"The Kafka consumer group to consume file messages from"`
	InputFileAvailableTopic  string   `env:"INPUT_FILE_AVAILABLE_TOPIC" flag:"input-file-available-topic" flagDesc:"The Kafka topic to consume file messages from"`
	KafkaMaxBytes            string   `env:"KAFKA_MAX_BYTES" flag:"kafka-max-bytes" flagDesc:"The maximum permitted size of a message. Should be set equal to or smaller than the broker's 'message.max.bytes'"`
	MaxRetries               int      `env:"REQUEST_MAX_RETRIES" flag:"request-max-retries" flagDesc:"The maximum number of attempts for a single http request due to external service failure"`
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
		ImportAPIURL:             "http://localhost:21800",
		InputFileAvailableTopic:  "input-file-available",
		InputFileAvailableGroup:  "input-file-available",
		KafkaMaxBytes:            "2000000",
		MaxRetries:               3,
		ImportAPIAuthToken:       "FD0108EA-825D-411C-9B1D-41EF7727F465",
	}

	if err := gofigure.Gofigure(cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
