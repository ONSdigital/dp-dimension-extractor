package config

func (kafkaConfig KafkaConfig) validateKafkaValues() []string {
	errs := []string{}

	if len(kafkaConfig.BindAddr) == 0 {
		errs = append(errs, "no KAFKA_ADDR given")
	}

	if kafkaConfig.MaxBytes == "" {
		errs = append(errs, "no KAFKA_MAX_BYTES given")
	}

	if kafkaConfig.Version == "" {
		errs = append(errs, "no KAFKA_VERSION given")
	}

	if kafkaConfig.SecProtocol != "" && kafkaConfig.SecProtocol != KafkaTLSProtocolFlag {
		errs = append(errs, "KAFKA_SEC_PROTO has invalid value")
	}

	isKafkaClientCertSet := kafkaConfig.SecClientCert != ""
	isKafkaClientKeySet := kafkaConfig.SecClientKey != ""
	if isKafkaClientKeySet && !isKafkaClientCertSet {
		errs = append(errs, "no KAFKA_SEC_CLIENT_CERT given but got KAFKA_SEC_CLIENT_KEY")
	}
	if isKafkaClientCertSet && !isKafkaClientKeySet {
		errs = append(errs, "no KAFKA_SEC_CLIENT_KEY given but got KAFKA_SEC_CLIENT_CERT")
	}

	return errs
}
