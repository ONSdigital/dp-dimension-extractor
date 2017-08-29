package service

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {

	Convey("Given inputFileAvailable events with HTTP and S3 URLs", t, func() {

		inputFileAvailableHttp := &inputFileAvailable{
			FileURL:    "https://s3-eu-west-1.amazonaws.com/csv-exported/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv",
			InstanceID: "123",
		}
		inputFileAvailableS3 := &inputFileAvailable{
			FileURL:    "s3://csv-exported/2137bad0-737c-4221-b75a-042e1.csv",
			InstanceID: "1234",
		}

		Convey("When the s3URL function is called", func() {

			s3URL, err := inputFileAvailableHttp.s3URL()

			Convey("The expected URL is returned with the S3 scheme", func() {
				So(err, ShouldBeNil)
				So(s3URL, ShouldEqual, "s3://csv-exported/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv")
			})

			s3URL, err = inputFileAvailableS3.s3URL()
			Convey("The expected URL is returned with the S3 scheme preserved", func() {
				So(err, ShouldBeNil)
				So(s3URL, ShouldEqual, "s3://csv-exported/2137bad0-737c-4221-b75a-042e1.csv")
			})

		})
	})
}
