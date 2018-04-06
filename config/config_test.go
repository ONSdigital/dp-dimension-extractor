package config_test

import (
	"testing"
	"time"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {
	Convey("Given an environment with no environment variables set", t, func() {
		cfg, err := config.Get()

		Convey("When the config values are retrieved", func() {

			Convey("There should be no error returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("The values should be set to the expected defaults", func() {
				So(cfg.AWSPrivateKey, ShouldEqual, "")
				So(cfg.AWSRegion, ShouldEqual, "eu-west-1")
				So(cfg.BindAddr, ShouldEqual, ":21400")
				So(cfg.Brokers[0], ShouldEqual, "localhost:9092")
				So(cfg.DatasetAPIURL, ShouldEqual, "http://localhost:22000")
				So(cfg.DatasetAPIAuthToken, ShouldEqual, "FD0108EA-825D-411C-9B1D-41EF7727F465")
				So(cfg.DimensionsExtractedTopic, ShouldEqual, "dimensions-extracted")
				So(cfg.DimensionExtractorURL, ShouldEqual, "http://localhost:21400")
				So(cfg.EncryptionDisabled, ShouldEqual, true)
				So(cfg.EventReporterTopic, ShouldEqual, "report-events")
				So(cfg.GracefulShutdownTimeout, ShouldEqual, 5*time.Second)
				So(cfg.InputFileAvailableTopic, ShouldEqual, "input-file-available")
				So(cfg.KafkaMaxBytes, ShouldEqual, "2000000")
				So(cfg.MaxRetries, ShouldEqual, 3)
				So(cfg.ServiceAuthToken, ShouldEqual, "Bearer E45F9BFC-3854-46AE-8187-11326A4E00F4")
				So(cfg.ZebedeeURL, ShouldEqual, "http://localhost:8082")
			})
		})
	})
}
