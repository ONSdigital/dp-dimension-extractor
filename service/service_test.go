package service

import (
	"testing"

	s3client "github.com/ONSdigital/dp-s3"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {
	Convey("Given inputFileAvailable events with HTTP and S3 URLs", t, func() {

		inputFileAvailableHTTP := &inputFileAvailable{
			FileURL:    "https://s3-eu-west-1.amazonaws.com/csv-exported/dir1/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv",
			InstanceID: "123",
		}
		inputFileAvailableS3 := &inputFileAvailable{
			FileURL:    "s3://csv-exported/dir2/2137bad0-737c-4221-b75a-042e1.csv",
			InstanceID: "1234",
		}

		Convey("When the s3URL function is called", func() {

			s3URL, err := inputFileAvailableHTTP.s3URL()

			Convey("The expected URL is returned with the S3 scheme", func() {
				So(err, ShouldBeNil)
				So(s3URL, ShouldResemble, &s3client.S3Url{
					Scheme:     "s3",
					BucketName: "csv-exported",
					Key:        "dir1/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv",
					Region:     "",
				})
				S3URLStr, err := s3URL.String(s3client.StyleAliasVirtualHosted)
				So(err, ShouldBeNil)
				So(S3URLStr, ShouldEqual, "s3://csv-exported/dir1/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv")
			})

			s3URL, err = inputFileAvailableS3.s3URL()
			Convey("The expected URL is returned with the S3 scheme preserved", func() {
				So(err, ShouldBeNil)
				So(s3URL, ShouldResemble, &s3client.S3Url{
					Scheme:     "s3",
					BucketName: "csv-exported",
					Key:        "dir2/2137bad0-737c-4221-b75a-042e1.csv",
					Region:     "",
				})
				S3URLStr, err := s3URL.String(s3client.StyleAliasVirtualHosted)
				So(err, ShouldBeNil)
				So(S3URLStr, ShouldEqual, "s3://csv-exported/dir2/2137bad0-737c-4221-b75a-042e1.csv")
			})
		})
	})
}
