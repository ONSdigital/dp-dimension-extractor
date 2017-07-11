package config_test

import (
	"testing"

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
				So(cfg.AWSRegion, ShouldEqual, "eu-west-1")
				So(cfg.BindAddr, ShouldEqual, ":21400")
				So(cfg.Brokers[0], ShouldEqual, "localhost:9092")
				So(cfg.DimensionsExtractedTopic, ShouldEqual, "dimensions-extracted")
				So(cfg.ImportAPIURL, ShouldEqual, "http://localhost:21800")
				So(cfg.InputFileAvailableOffset, ShouldEqual, -1)
				So(cfg.InputFileAvailableTopic, ShouldEqual, "input-file-available")
				So(cfg.KafkaMaxBytes, ShouldEqual, "2000000")
			})
		})
	})
}
