package config

func (kafkaConfig KafkaConfig) validateKafkaValues() []string {
	errs := []string{}

	if len(kafkaConfig.BindAddr) == 0 {
		errs = append(errs, "no KAFKA_ADDR given")
	}

	if len(kafkaConfig.MaxBytes) == 0 {
		errs = append(errs, "no KAFKA_MAX_BYTES given")
	}

	if len(kafkaConfig.Version) == 0 {
		errs = append(errs, "no KAFKA_VERSION given")
	}

	if kafkaConfig.SecProtocol != "" && kafkaConfig.SecProtocol != KafkaTLSProtocolFlag {
		errs = append(errs, "KAFKA_SEC_PROTO has invalid value")
	}

	// isKafkaClientCertSet xor isKafkaClientKeySet
	isKafkaClientCertSet := len(kafkaConfig.SecClientCert) != 0
	isKafkaClientKeySet := len(kafkaConfig.SecClientKey) != 0
	if (isKafkaClientCertSet || isKafkaClientKeySet) && !(isKafkaClientCertSet && isKafkaClientKeySet) {
		errs = append(errs, "only one of KAFKA_SEC_CLIENT_CERT or KAFKA_SEC_CLIENT_KEY has been set - requires both")
	}

	return errs
}
