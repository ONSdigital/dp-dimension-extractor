package config

import (
	"errors"
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGet(t *testing.T) {
	Convey("Given a clean environment", t, func() {
		os.Clearenv()
		cfg = nil

		Convey("When the config values are retrieved", func() {
			cfg, err := Get()

			Convey("Then there should be no error returned", func() {
				So(err, ShouldBeNil)

				Convey("And values should be set to the expected defaults", func() {
					So(cfg.AWSRegion, ShouldEqual, "eu-west-1")
					So(cfg.BindAddr, ShouldEqual, ":21400")
					So(cfg.DatasetAPIURL, ShouldEqual, "http://localhost:22000")
					So(cfg.EncryptionDisabled, ShouldEqual, false)
					So(cfg.GracefulShutdownTimeout, ShouldEqual, 5*time.Second)
					So(cfg.KafkaConfig.BindAddr, ShouldResemble, []string{"localhost:9092", "localhost:9093", "localhost:9094"})
					So(cfg.KafkaConfig.MaxBytes, ShouldEqual, "2000000")
					So(cfg.KafkaConfig.Version, ShouldEqual, "1.0.2")
					So(cfg.KafkaConfig.SecProtocol, ShouldEqual, "")
					So(cfg.KafkaConfig.SecCACerts, ShouldEqual, "")
					So(cfg.KafkaConfig.SecClientCert, ShouldEqual, "")
					So(cfg.KafkaConfig.SecClientKey, ShouldEqual, "")
					So(cfg.KafkaConfig.SecSkipVerify, ShouldEqual, false)
					So(cfg.KafkaConfig.DimensionsExtractedTopic, ShouldEqual, "dimensions-extracted")
					So(cfg.KafkaConfig.EventReporterTopic, ShouldEqual, "report-events")
					So(cfg.KafkaConfig.InputFileAvailableGroup, ShouldEqual, "input-file-available")
					So(cfg.KafkaConfig.InputFileAvailableTopic, ShouldEqual, "input-file-available")
					So(cfg.MaxRetries, ShouldEqual, 3)
					So(cfg.VaultAddr, ShouldEqual, "http://localhost:8200")
					So(cfg.VaultPath, ShouldEqual, "secret/shared/psk")
					So(cfg.VaultToken, ShouldEqual, "")
					So(cfg.ServiceAuthToken, ShouldEqual, "Bearer E45F9BFC-3854-46AE-8187-11326A4E00F4")
					So(cfg.ZebedeeURL, ShouldEqual, "http://localhost:8082")
					So(cfg.HealthCheckInterval, ShouldEqual, 30*time.Second)
					So(cfg.HealthCheckCriticalTimeout, ShouldEqual, 90*time.Second)
					So(cfg.BucketNames, ShouldResemble, []string{"dp-frontend-florence-file-uploads"})
				})
			})
		})

		Convey("When configuration is called with an invalid security setting", func() {
			defer os.Clearenv()
			os.Setenv("KAFKA_SEC_PROTO", "ssl")
			cfg, err := Get()

			Convey("Then an error should be returned", func() {
				So(cfg, ShouldBeNil)
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errors.New("kafka config validation errors: KAFKA_SEC_PROTO has invalid value"))
			})
		})
	})
}

func TestString(t *testing.T) {
	Convey("Given the config values", t, func() {
		cfg, err := Get()
		So(err, ShouldBeNil)

		Convey("When String is called", func() {
			cfgStr := cfg.String()

			Convey("Then the string format of config should not contain any sensitive configurations", func() {
				So(cfgStr, ShouldNotContainSubstring, "BindAddr:[localhost:9092]") // KafkaConfig.BindAddr
				So(cfgStr, ShouldNotContainSubstring, "SecClientKey")
				So(cfgStr, ShouldNotContainSubstring, "VaultToken")
				So(cfgStr, ShouldNotContainSubstring, "ServiceAuthToken")
				So(cfgStr, ShouldNotContainSubstring, "BucketNames")

				Convey("And should contain all non-sensitive configurations", func() {
					So(cfgStr, ShouldContainSubstring, "AWSRegion")
					So(cfgStr, ShouldContainSubstring, "BindAddr")
					So(cfgStr, ShouldContainSubstring, "DatasetAPIURL")
					So(cfgStr, ShouldContainSubstring, "DimensionsExtractedTopic")
					So(cfgStr, ShouldContainSubstring, "EncryptionDisabled")
					So(cfgStr, ShouldContainSubstring, "GracefulShutdownTimeout")
					So(cfgStr, ShouldContainSubstring, "InputFileAvailableGroup")
					So(cfgStr, ShouldContainSubstring, "InputFileAvailableTopic")

					So(cfgStr, ShouldContainSubstring, "KafkaConfig")
					So(cfgStr, ShouldContainSubstring, "MaxBytes")
					So(cfgStr, ShouldContainSubstring, "Version")
					So(cfgStr, ShouldContainSubstring, "SecProtocol")
					So(cfgStr, ShouldContainSubstring, "SecCACerts")
					So(cfgStr, ShouldContainSubstring, "SecClientCert")
					So(cfgStr, ShouldContainSubstring, "SecSkipVerify")

					So(cfgStr, ShouldContainSubstring, "MaxRetries")
					So(cfgStr, ShouldContainSubstring, "VaultAddr")
					So(cfgStr, ShouldContainSubstring, "VaultPath")
					So(cfgStr, ShouldContainSubstring, "ZebedeeURL")
					So(cfgStr, ShouldContainSubstring, "HealthCheckInterval")
					So(cfgStr, ShouldContainSubstring, "HealthCheckCriticalTimeout")
				})
			})
		})
	})
}
