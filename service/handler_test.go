package service

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestSpec(t *testing.T) {

	Convey("Given an inputFileAvailable event with a HTTP S3 URL", t, func() {

		inputFileAvailable := &inputFileAvailable{
			FileURL:"https://s3-eu-west-1.amazonaws.com/csv-exported/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv",
			InstanceID:"123",
		}

		Convey("When the s3URL function is called", func() {

			s3URL, err := inputFileAvailable.s3URL()

			Convey("The expected URL is returned with the S3 scheme", func() {
				So(err, ShouldBeNil)
				So(s3URL, ShouldEqual, "s3://csv-exported/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv")
			})
		})
	})
}
