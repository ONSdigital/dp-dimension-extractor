package config

import (
	"os"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ian-kent/gofigure"
)

// Config is the filing resource handler config
type Config struct {
	AWSRegion                string   `env:"AWS_REGION" flag:"aws-region" flagDesc:"The AWS region to use"`
	BindAddr                 string   `env:"BIND_ADDR" flag:"bind-addr" flagDesc:"The port to bind to"`
	Brokers                  []string `env:"KAFKA_ADDR" flag:"kafka-addr" flagDesc:"The kafka broker addresses"`
	DimensionsExtractedTopic string   `env:"DIMENSIONS_EXTRACTED_TOPIC" flag:"dimensions-extracted-topic" flagDesc:"The Kafka topic to write dimension messages to"`
	ImportAPIURL             string   `env:"IMPORT_API_URL" flag:"import-api-url" flagDesc:"The import api url"`
	InputFileAvailableOffset int64    `env:"INPUT_FILE_AVAILABLE_OFFSET" flag:"input-file-available-offset" flagDesc:"The offset you wish to consume from (-1 to continue from last committed message)"`
	InputFileAvailableTopic  string   `env:"INPUT_FILE_AVAILABLE_TOPIC" flag:"input-file-available-topic" flagDesc:"The Kafka topic to consume file messages from"`
	KafkaMaxBytes            string   `env:"KAFKA_MAX_BYTES" flag:"kafka-max-bytes" flagDesc:"The maximum permitted size of a message. Should be set equal to or smaller than the broker's 'message.max.bytes'"`
}

var cfg *Config

// Get configures the application and returns the configuration
func Get() *Config {
	if cfg != nil {
		return cfg
	}

	var brokers []string

	brokers = append(brokers, "localhost:9092")

	cfg = &Config{
		AWSRegion:                "eu-west-1",
		BindAddr:                 ":8080",
		Brokers:                  brokers,
		DimensionsExtractedTopic: "dimensions-extracted",
		ImportAPIURL:             "http://localhost:21800",
		InputFileAvailableOffset: int64(-1),
		InputFileAvailableTopic:  "input-file-available",
		KafkaMaxBytes:            "2000000",
	}

	if err := gofigure.Gofigure(cfg); err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	return cfg
}
