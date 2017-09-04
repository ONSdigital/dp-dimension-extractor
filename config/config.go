package config

import "github.com/kelseyhightower/envconfig"

// Config is the filing resource handler config
type Config struct {
	AWSRegion                string   `envconfig:"AWS_REGION"`
	BindAddr                 string   `envconfig:"BIND_ADDR"`
	Brokers                  []string `envconfig:"KAFKA_ADDR"`
	DimensionsExtractedTopic string   `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	ImportAPIURL             string   `envconfig:"IMPORT_API_URL"`
	ImportAPIAuthToken       string   `envconfig:"IMPORT_AUTH_TOKEN"`
	InputFileAvailableGroup  string   `envconfig:"INPUT_FILE_AVAILABLE_GROUP"`
	InputFileAvailableTopic  string   `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	KafkaMaxBytes            string   `envconfig:"KAFKA_MAX_BYTES"`
	MaxRetries               int      `envconfig:"REQUEST_MAX_RETRIES"`
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

	return cfg, envconfig.Process("", cfg)
}
