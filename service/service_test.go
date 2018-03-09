package service

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {
	Convey("Given inputFileAvailable events with HTTP and S3 URLs", t, func() {

		inputFileAvailableHTTP := &inputFileAvailable{
			FileURL:    "https://s3-eu-west-1.amazonaws.com/csv-exported/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv",
			InstanceID: "123",
		}
		inputFileAvailableS3 := &inputFileAvailable{
			FileURL:    "s3://csv-exported/2137bad0-737c-4221-b75a-042e1.csv",
			InstanceID: "1234",
		}

		Convey("When the s3URL function is called", func() {

			s3URL, err := inputFileAvailableHTTP.s3URL()

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

func TestGetBucketAndFilename(t *testing.T) {
	t.Parallel()
	Convey("Given a valid S3 URL, succesfully retrieve bucket and filename", t, func() {
		s3URL := "s3.amazonaws.com/privateBucket/myFile"
		bucket, filename, err := getBucketAndFilename(s3URL)

		So(err, ShouldBeNil)
		So(bucket, ShouldEqual, "privateBucket")
		So(filename, ShouldEqual, "myFile")
	})

	Convey("Given an empty s3 url, return error 'could not find bucket or filename in file url'", t, func() {
		bucket, filename, err := getBucketAndFilename("")

		So(bucket, ShouldEqual, "")
		So(filename, ShouldEqual, "")
		So(err, ShouldNotBeNil)
		So(err, ShouldResemble, errors.New("could not find bucket or filename in file url"))
	})

	Convey("Given an s3 url that is missing the bucket name, return error 'missing bucket name in file url'", t, func() {
		bucket, filename, err := getBucketAndFilename("s3://some-file")

		So(bucket, ShouldEqual, "")
		So(filename, ShouldEqual, "")
		So(err, ShouldNotBeNil)
		So(err, ShouldResemble, errors.New("missing bucket name in file url"))
	})

	Convey("Given an s3 url that is missing the filename, return error 'missing filename in file url'", t, func() {
		bucket, filename, err := getBucketAndFilename("s3://")

		So(bucket, ShouldEqual, "")
		So(filename, ShouldEqual, "")
		So(err, ShouldNotBeNil)
		So(err, ShouldResemble, errors.New("missing filename in file url"))
	})
}
