package config

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestValidateKafkaValues(t *testing.T) {
	Convey("Given valid kafka configurations", t, func() {
		cfg = getDefaultConfig()

		Convey("When validateKafkaValues is called", func() {
			errs := cfg.KafkaConfig.validateKafkaValues()

			Convey("Then no error messages should be returned", func() {
				So(errs, ShouldBeEmpty)
			})
		})
	})

	Convey("Given an empty KAFKA_ADDR", t, func() {
		cfg = getDefaultConfig()
		cfg.KafkaConfig.BindAddr = []string{}

		Convey("When validateKafkaValues is called", func() {
			errs := cfg.KafkaConfig.validateKafkaValues()

			Convey("Then an error message should be returned", func() {
				So(errs, ShouldNotBeEmpty)
				So(errs, ShouldResemble, []string{"no KAFKA_ADDR given"})
			})
		})
	})

	Convey("Given an empty KAFKA_MAX_BYTES", t, func() {
		cfg = getDefaultConfig()
		cfg.KafkaConfig.MaxBytes = ""

		Convey("When validateKafkaValues is called", func() {
			errs := cfg.KafkaConfig.validateKafkaValues()

			Convey("Then an error message should be returned", func() {
				So(errs, ShouldNotBeEmpty)
				So(errs, ShouldResemble, []string{"no KAFKA_MAX_BYTES given"})
			})
		})
	})

	Convey("Given an empty KAFKA_VERSION", t, func() {
		cfg = getDefaultConfig()
		cfg.KafkaConfig.Version = ""

		Convey("When validateKafkaValues is called", func() {
			errs := cfg.KafkaConfig.validateKafkaValues()

			Convey("Then an error message should be returned", func() {
				So(errs, ShouldNotBeEmpty)
				So(errs, ShouldResemble, []string{"no KAFKA_VERSION given"})
			})
		})
	})

	Convey("Given an invalid KAFKA_SEC_PROTO", t, func() {
		cfg = getDefaultConfig()
		cfg.KafkaConfig.SecProtocol = "invalid"

		Convey("When validateKafkaValues is called", func() {
			errs := cfg.KafkaConfig.validateKafkaValues()

			Convey("Then an error message should be returned", func() {
				So(errs, ShouldNotBeEmpty)
				So(errs, ShouldResemble, []string{"KAFKA_SEC_PROTO has invalid value"})
			})
		})
	})

	Convey("Given one of KAFKA_SEC_CLIENT_CERT or KAFKA_SEC_CLIENT_KEY has been set", t, func() {
		cfg = getDefaultConfig()
		cfg.KafkaConfig.SecClientCert = "test"

		Convey("When validateKafkaValues is called", func() {
			errs := cfg.KafkaConfig.validateKafkaValues()

			Convey("Then an error message should be returned", func() {
				So(errs, ShouldNotBeEmpty)
				So(errs, ShouldResemble, []string{"only one of KAFKA_SEC_CLIENT_CERT or KAFKA_SEC_CLIENT_KEY has been set - requires both"})
			})
		})
	})

	Convey("Given more than one invalid kafka configuration", t, func() {
		cfg = getDefaultConfig()
		cfg.KafkaConfig.Version = ""
		cfg.KafkaConfig.SecProtocol = "invalid"

		Convey("When validateKafkaValues is called", func() {
			errs := cfg.KafkaConfig.validateKafkaValues()

			Convey("Then an error message should be returned", func() {
				So(errs, ShouldNotBeEmpty)
				So(errs, ShouldResemble, []string{"no KAFKA_VERSION given", "KAFKA_SEC_PROTO has invalid value"})
			})
		})
	})
}
