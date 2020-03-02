package service_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/dp-dimension-extractor/service/mock"
	"github.com/ONSdigital/dp-kafka/kafkatest"
	s3client "github.com/ONSdigital/dp-s3"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {
	Convey("Given inputFileAvailable events with HTTP and S3 URLs", t, func() {

		inputFileAvailableHTTP := &service.InputFileAvailable{
			FileURL:    "https://s3-eu-west-1.amazonaws.com/csv-exported/dir1/2137bad0-737c-4221-b75a-ce7ffd3042e1.csv",
			InstanceID: "123",
		}
		inputFileAvailableS3 := &service.InputFileAvailable{
			FileURL:    "s3://csv-exported/dir2/2137bad0-737c-4221-b75a-042e1.csv",
			InstanceID: "1234",
		}

		Convey("When the s3URL function is called", func() {

			s3URL, err := inputFileAvailableHTTP.S3URL()

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

			s3URL, err = inputFileAvailableS3.S3URL()
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

func TestHandleMessage(t *testing.T) {
	// TODO test HandleMessage, possibly splitting the method, and will need to create new mocks.
	mockKafkaProducer := &mock.KafkaProducerMock{}
	// mockKafkaProducer := kafkatest.NewMessageProducer()
	mockDatasetClient := &mock.DatasetClientMock{}
	s3Clients := map[string]service.S3Client{
		"myBucket": &mock.S3ClientMock{},
	}
	// var mockAwsSession *session.Session
	mockVaultClient := &mock.VaultClientMock{}

	svc := &service.Service{
		AuthToken:                  "myAuthToken",
		DimensionExtractedProducer: mockKafkaProducer,
		DimensionExtractorURL:      "dimensionExtractorURL",
		EncryptionDisabled:         true,
		EnvMax:                     1,
		DatasetClient:              mockDatasetClient,
		AwsSession:                 nil,
		S3Clients:                  s3Clients,
		VaultClient:                mockVaultClient,
		VaultPath:                  "myVaultPath",
	}
	svc.HandleMessage(context.Background(), &kafkatest.Message{})
}
