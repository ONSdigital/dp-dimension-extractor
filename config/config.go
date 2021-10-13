package config

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// KafkaTLSProtocolFlag informs service to use TLS protocol for kafka
const KafkaTLSProtocolFlag = "TLS"

// Config is the filing resource handler config
type Config struct {
	AWSRegion                  string        `envconfig:"AWS_REGION"`
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	DatasetAPIURL              string        `envconfig:"DATASET_API_URL"`
	EncryptionDisabled         bool          `envconfig:"ENCRYPTION_DISABLED"`
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	KafkaConfig                KafkaConfig
	MaxRetries                 int           `envconfig:"REQUEST_MAX_RETRIES"`
	VaultAddr                  string        `envconfig:"VAULT_ADDR"`
	VaultToken                 string        `envconfig:"VAULT_TOKEN"                    json:"-"`
	VaultPath                  string        `envconfig:"VAULT_PATH"`
	ServiceAuthToken           string        `envconfig:"SERVICE_AUTH_TOKEN"             json:"-"`
	ZebedeeURL                 string        `envconfig:"ZEBEDEE_URL"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	BucketNames                []string      `envconfig:"BUCKET_NAMES"                   json:"-"`
}

// KafkaConfig contains the config required to connect to Kafka
type KafkaConfig struct {
	BindAddr                 []string `envconfig:"KAFKA_ADDR"                            json:"-"`
	MaxBytes                 string   `envconfig:"KAFKA_MAX_BYTES"`
	Version                  string   `envconfig:"KAFKA_VERSION"`
	SecProtocol              string   `envconfig:"KAFKA_SEC_PROTO"`
	SecCACerts               string   `envconfig:"KAFKA_SEC_CA_CERTS"`
	SecClientCert            string   `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	SecClientKey             string   `envconfig:"KAFKA_SEC_CLIENT_KEY"                  json:"-"`
	SecSkipVerify            bool     `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	DimensionsExtractedTopic string   `envconfig:"DIMENSIONS_EXTRACTED_TOPIC"`
	EventReporterTopic       string   `envconfig:"EVENT_REPORTER_TOPIC"`
	InputFileAvailableGroup  string   `envconfig:"INPUT_FILE_AVAILABLE_GROUP"`
	InputFileAvailableTopic  string   `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
}

var cfg *Config

func getDefaultConfig() *Config {
	return &Config{
		AWSRegion:               "eu-west-1",
		BindAddr:                ":21400",
		DatasetAPIURL:           "http://localhost:22000",
		EncryptionDisabled:      false,
		GracefulShutdownTimeout: 5 * time.Second,
		KafkaConfig: KafkaConfig{
			BindAddr:                 []string{"localhost:9092"},
			MaxBytes:                 "2000000",
			Version:                  "1.0.2",
			SecProtocol:              "",
			SecCACerts:               "",
			SecClientCert:            "",
			SecClientKey:             "",
			SecSkipVerify:            false,
			DimensionsExtractedTopic: "dimensions-extracted",
			EventReporterTopic:       "report-events",
			InputFileAvailableTopic:  "input-file-available",
			InputFileAvailableGroup:  "input-file-available",
		},
		MaxRetries:                 3,
		VaultAddr:                  "http://localhost:8200",
		VaultToken:                 "",
		VaultPath:                  "secret/shared/psk",
		ServiceAuthToken:           "E45F9BFC-3854-46AE-8187-11326A4E00F4",
		ZebedeeURL:                 "http://localhost:8082",
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		BucketNames:                []string{"dp-frontend-florence-file-uploads"},
	}
}

// Get configures the application and returns the configuration
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = getDefaultConfig()

	if err := envconfig.Process("", cfg); err != nil {
		return cfg, err
	}

	cfg.ServiceAuthToken = "Bearer " + cfg.ServiceAuthToken

	if errs := cfg.KafkaConfig.validateKafkaValues(); len(errs) != 0 {
		return nil, fmt.Errorf("kafka config validation errors: %v", strings.Join(errs, ", "))
	}

	return cfg, nil
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Config) String() string {
	jsonCfg, _ := json.Marshal(config)
	return string(jsonCfg)
}