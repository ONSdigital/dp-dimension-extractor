package service_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/dataset"
	"github.com/ONSdigital/dp-dimension-extractor/schema"
	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/dp-dimension-extractor/service/mock"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/kafkatest"
	s3client "github.com/ONSdigital/dp-s3"
	. "github.com/smartystreets/goconvey/convey"
)

// testing constants
const (
	validBucket     = "csv-exported"
	validS3ObjKey   = "dir1/ff8b452f-7ad4-40ed-a1ad-578ee5c948e0.csv"
	validFileURL    = "https://s3-eu-west-1.amazonaws.com/csv-exported/dir1/ff8b452f-7ad4-40ed-a1ad-578ee5c948e0.csv"
	validS3URL      = "s3://csv-exported/dir1/ff8b452f-7ad4-40ed-a1ad-578ee5c948e0.csv"
	validInstanceID = "123"
	validAuthToken  = "myAuthToken"
	validVaultPath  = "myVaultPath"
)

// testing variables
var (
	validPsk     = []byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	testInstance = dataset.Instance{
		Version: dataset.Version{
			ID:         "versionId",
			InstanceID: validInstanceID,
			Dimensions: []dataset.Dimension{
				dataset.Dimension{ID: "Time", Name: "time"},
				dataset.Dimension{ID: "Geography", Name: "geography"},
				dataset.Dimension{ID: "Aggregate", Name: "aggregate"},
			},
		},
	}
	validCsvContent = `
V4_0,Time,Time,UK-only,Geography,Cpih1dim1aggid,Aggregate
93.7,Month,Mar-12,K02000001,          ,cpih1dim1T80000,08 Communication
`
	testChannels = kafka.CreateProducerChannels()
)

var ctx = context.Background()

// mock functions for testing
var (
	mockChannelsFunc    = func() *kafka.ProducerChannels { return testChannels }
	mockGetInstanceFunc = func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string) (dataset.Instance, error) {
		if instanceID == validInstanceID {
			return testInstance, nil
		}
		return dataset.Instance{}, errors.New("Unexpected instance ID")
	}
	mockPostInstanceFunc = func(ctx context.Context, serviceAuthToken string, instanceID string, data dataset.OptionPost) error {
		return nil
	}
	mockPutInstanceFunc = func(ctx context.Context, serviceAuthToken string, instanceID string, data dataset.JobInstance) error {
		return nil
	}

	mockGetFunc = func(key string) (io.ReadCloser, error) {
		if key != validS3ObjKey {
			return nil, errors.New("wrong S3 Key")
		}
		return ioutil.NopCloser(bytes.NewReader([]byte(validCsvContent))), nil
	}
	mockGetWithPskFunc = func(key string, psk []byte) (io.ReadCloser, error) {
		if key != validS3ObjKey {
			return nil, errors.New("wrong S3 Key")
		}
		if hex.EncodeToString(psk) != hex.EncodeToString(validPsk) {
			return nil, errors.New("wrong PSK")
		}
		return ioutil.NopCloser(bytes.NewReader([]byte(validCsvContent))), nil

	}
	mockReadKeyFunc = func(path string, key string) (string, error) {
		if path == (validVaultPath + "/" + validS3ObjKey) {
			return hex.EncodeToString(validPsk), nil
		}
		return "", errors.New("wrong vault path")
	}
)

func TestS3URL(t *testing.T) {
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

	Convey("Given the intention to Handle a message ", t, func() {
		mockVaultClient := &mock.VaultClientMock{ReadKeyFunc: mockReadKeyFunc}
		mockDatasetClient := &mock.DatasetClientMock{
			GetInstanceFunc:            mockGetInstanceFunc,
			PostInstanceDimensionsFunc: mockPostInstanceFunc,
			PutInstanceDataFunc:        mockPutInstanceFunc,
		}
		mockS3Client := &mock.S3ClientMock{GetFunc: mockGetFunc, GetWithPSKFunc: mockGetWithPskFunc}

		Convey("With a service which does not expect any external call", func() {
			svc := &service.Service{
				AuthToken:                  validAuthToken,
				DimensionExtractedProducer: &mock.KafkaProducerMock{},
				EncryptionDisabled:         true,
				DatasetClient:              &mock.DatasetClientMock{},
				AwsSession:                 nil,
				S3Clients:                  map[string]service.S3Client{},
				VaultClient:                &mock.VaultClientMock{},
				VaultPath:                  validVaultPath,
			}

			Convey("When a message with unexpected avro format is is received, HandleMessage returns an error", func() {
				msg := kafkatest.NewMessage([]byte("wrongMessageFormat"), 1)
				_, err := svc.HandleMessage(ctx, msg)
				So(err, ShouldResemble, errors.New("Invalid string length"))
			})

			Convey("When a valid message with unexpected fileURL format is received, HandleMessage returns an error", func() {
				msgIn := &service.InputFileAvailable{
					FileURL:    "wrongS3PathFormat",
					InstanceID: validInstanceID,
				}
				msgPayload, err := schema.InputFileAvailableSchema.Marshal(msgIn)
				So(err, ShouldBeNil)
				msg := kafkatest.NewMessage(msgPayload, 1)
				_, err = svc.HandleMessage(ctx, msg)
				So(err, ShouldResemble, errors.New("could not find bucket or filename in file path-style url wrongS3PathFormat"))
			})

		})

		Convey("With a service with encryption disabled", func() {

			svc := &service.Service{
				AuthToken:                  validAuthToken,
				DimensionExtractedProducer: &mock.KafkaProducerMock{ChannelsFunc: mockChannelsFunc},
				EncryptionDisabled:         true,
				DatasetClient:              mockDatasetClient,
				AwsSession:                 nil,
				S3Clients:                  map[string]service.S3Client{validBucket: mockS3Client},
				VaultClient:                mockVaultClient,
				VaultPath:                  validVaultPath,
			}

			Convey("When a valid message with unexpected instance ID is received, HandleMessage returns error trying to get the instance", func() {
				msgIn := &service.InputFileAvailable{
					FileURL:    validS3URL,
					InstanceID: "wrongInstance",
				}
				msgPayload, err := schema.InputFileAvailableSchema.Marshal(msgIn)
				So(err, ShouldBeNil)
				msg := kafkatest.NewMessage(msgPayload, 1)
				_, err = svc.HandleMessage(ctx, msg)
				So(err, ShouldResemble, errors.New("Unexpected instance ID"))
				validateS3Get(mockS3Client, validS3ObjKey)
				So(len(mockVaultClient.ReadKeyCalls()), ShouldEqual, 0)
				So(len(mockDatasetClient.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockDatasetClient.PostInstanceDimensionsCalls()), ShouldEqual, 0)
				So(len(mockDatasetClient.PutInstanceDataCalls()), ShouldEqual, 0)
			})

			Convey("When a valid message pointing to an inexistent S3 Key is received, HandleMessage returns error while trying to get the object from S3", func() {
				msgIn := &service.InputFileAvailable{
					FileURL:    "s3://csv-exported/dir1/inexistent.csv",
					InstanceID: validInstanceID,
				}
				msgPayload, err := schema.InputFileAvailableSchema.Marshal(msgIn)
				So(err, ShouldBeNil)
				msg := kafkatest.NewMessage(msgPayload, 1)
				_, err = svc.HandleMessage(ctx, msg)
				So(err, ShouldResemble, errors.New("wrong S3 Key"))
				validateS3Get(mockS3Client, "dir1/inexistent.csv")
				So(len(mockVaultClient.ReadKeyCalls()), ShouldEqual, 0)
				So(len(mockDatasetClient.GetInstanceCalls()), ShouldEqual, 0)
				So(len(mockDatasetClient.PostInstanceDimensionsCalls()), ShouldEqual, 0)
				So(len(mockDatasetClient.PutInstanceDataCalls()), ShouldEqual, 0)
			})

			Convey("When a valid message is received, HandleMessage extracts the dimensions, perform a POST for each one, and a PUT to update the instance", func(c C) {
				go func() {
					expectedProducerMessage, err := schema.DimensionsExtractedSchema.Marshal(&service.DimensionExtracted{
						FileURL:    validS3URL,
						InstanceID: validInstanceID,
					})
					c.So(err, ShouldBeNil)
					producedMessage := <-testChannels.Output
					c.So(producedMessage, ShouldResemble, expectedProducerMessage)
				}()

				_, err := svc.HandleMessage(ctx, createValidMessage())
				So(err, ShouldBeNil)

				validateS3Get(mockS3Client, validS3ObjKey)
				validateGetInstance(mockDatasetClient)
				validatePostedDimensions(mockDatasetClient,
					map[string]dataset.OptionPost{
						"time":      dataset.OptionPost{Code: "Month", CodeList: "Time", Label: "Mar-12", Name: "time", Option: "Mar-12"},
						"geography": dataset.OptionPost{Code: "K02000001", CodeList: "Geography", Label: "          ", Name: "geography", Option: "K02000001"},
						"aggregate": dataset.OptionPost{Code: "cpih1dim1T80000", CodeList: "Aggregate", Label: "08 Communication", Name: "aggregate", Option: "cpih1dim1T80000"},
					},
				)
				validatePutInstance(mockDatasetClient, &dataset.JobInstance{
					HeaderNames:          []string{"V4_0", "Time", "Time", "UK-only", "Geography", "Cpih1dim1aggid", "Aggregate"},
					NumberOfObservations: 1})
			})

		})

		Convey("Given a service with encryption enabled", func() {

			svc := &service.Service{
				AuthToken:                  validAuthToken,
				DimensionExtractedProducer: &mock.KafkaProducerMock{ChannelsFunc: mockChannelsFunc},
				EncryptionDisabled:         false,
				DatasetClient:              mockDatasetClient,
				AwsSession:                 nil,
				S3Clients:                  map[string]service.S3Client{validBucket: mockS3Client},
				VaultClient:                mockVaultClient,
				VaultPath:                  validVaultPath,
			}

			Convey("When a valid message pointing to an inexistent S3 Key is received, HandleMessage returns error while trying to read the PSK from vault", func() {
				msgIn := &service.InputFileAvailable{
					FileURL:    "s3://csv-exported/dir1/inexistent.csv",
					InstanceID: validInstanceID,
				}
				msgPayload, err := schema.InputFileAvailableSchema.Marshal(msgIn)
				So(err, ShouldBeNil)
				msg := kafkatest.NewMessage(msgPayload, 1)
				_, err = svc.HandleMessage(ctx, msg)
				So(err, ShouldResemble, errors.New("wrong vault path"))
				So(len(mockVaultClient.ReadKeyCalls()), ShouldEqual, 1)
				So(len(mockS3Client.GetWithPSKCalls()), ShouldEqual, 0)
				So(len(mockDatasetClient.GetInstanceCalls()), ShouldEqual, 0)
				So(len(mockDatasetClient.PostInstanceDimensionsCalls()), ShouldEqual, 0)
				So(len(mockDatasetClient.PutInstanceDataCalls()), ShouldEqual, 0)
			})

			Convey("When a valid message is received ", func(c C) {
				go func() {
					expectedProducerMessage, err := schema.DimensionsExtractedSchema.Marshal(&service.DimensionExtracted{
						FileURL:    validS3URL,
						InstanceID: validInstanceID,
					})
					c.So(err, ShouldBeNil)
					producedMessage := <-testChannels.Output
					c.So(producedMessage, ShouldResemble, expectedProducerMessage)
				}()

				_, err := svc.HandleMessage(ctx, createValidMessage())
				So(err, ShouldBeNil)

				validateS3GetWithPSK(mockS3Client, validS3ObjKey, validPsk)
				validateVault(mockVaultClient)
				validateGetInstance(mockDatasetClient)
				validatePostedDimensions(mockDatasetClient,
					map[string]dataset.OptionPost{
						"time":      dataset.OptionPost{Code: "Month", CodeList: "Time", Label: "Mar-12", Name: "time", Option: "Mar-12"},
						"geography": dataset.OptionPost{Code: "K02000001", CodeList: "Geography", Label: "          ", Name: "geography", Option: "K02000001"},
						"aggregate": dataset.OptionPost{Code: "cpih1dim1T80000", CodeList: "Aggregate", Label: "08 Communication", Name: "aggregate", Option: "cpih1dim1T80000"},
					},
				)
				validatePutInstance(mockDatasetClient, &dataset.JobInstance{
					HeaderNames:          []string{"V4_0", "Time", "Time", "UK-only", "Geography", "Cpih1dim1aggid", "Aggregate"},
					NumberOfObservations: 1})
			})
		})
	})
}

func createValidMessage() kafka.Message {
	msgIn := &service.InputFileAvailable{
		FileURL:    validFileURL,
		InstanceID: validInstanceID,
	}
	msgPayload, err := schema.InputFileAvailableSchema.Marshal(msgIn)
	So(err, ShouldBeNil)
	return kafkatest.NewMessage(msgPayload, 1)
}

// checks that S3 Get was called exactly once, and GetWithPsk was not called
func validateS3Get(mockS3Client *mock.S3ClientMock, expectedObjKey string) {
	So(len(mockS3Client.GetCalls()), ShouldEqual, 1)
	So(mockS3Client.GetCalls()[0].Key, ShouldEqual, expectedObjKey)
	So(len(mockS3Client.GetWithPSKCalls()), ShouldEqual, 0)
}

// checks that S3 GetWithPSK was called exactly once, and Get was not called
func validateS3GetWithPSK(mockS3Client *mock.S3ClientMock, expectedObjKey string, expectedPsk []byte) {
	So(len(mockS3Client.GetWithPSKCalls()), ShouldEqual, 1)
	So(mockS3Client.GetWithPSKCalls()[0].Key, ShouldEqual, expectedObjKey)
	So(mockS3Client.GetWithPSKCalls()[0].Psk, ShouldResemble, expectedPsk)
	So(len(mockS3Client.GetCalls()), ShouldEqual, 0)
}

// checks that ReadKey was called exactly once with the expected vault key
func validateVault(mockVaultClient *mock.VaultClientMock) {
	So(len(mockVaultClient.ReadKeyCalls()), ShouldEqual, 1)
	So(mockVaultClient.ReadKeyCalls()[0].Path, ShouldEqual, validVaultPath+"/"+validS3ObjKey)
}

// checks that GetInstance was called exactly once with expected paramters
func validateGetInstance(mockDatasetClient *mock.DatasetClientMock) {
	So(len(mockDatasetClient.GetInstanceCalls()), ShouldEqual, 1)
	So(mockDatasetClient.GetInstanceCalls()[0].InstanceID, ShouldEqual, validInstanceID)
	So(mockDatasetClient.GetInstanceCalls()[0].ServiceAuthToken, ShouldEqual, validAuthToken)
	So(mockDatasetClient.GetInstanceCalls()[0].CollectionID, ShouldEqual, "")
	So(mockDatasetClient.GetInstanceCalls()[0].UserAuthToken, ShouldEqual, "")
}

// checks that postDimension was called exactly once for each item in the expected map, with the expected optionPosts depending on the name
func validatePostedDimensions(mockDatasetClient *mock.DatasetClientMock, expected map[string]dataset.OptionPost) {
	So(len(mockDatasetClient.PostInstanceDimensionsCalls()), ShouldEqual, len(expected))
	So(mockDatasetClient.PostInstanceDimensionsCalls()[0].InstanceID, ShouldEqual, validInstanceID)
	So(mockDatasetClient.PostInstanceDimensionsCalls()[0].ServiceAuthToken, ShouldEqual, validAuthToken)
	for _, call := range mockDatasetClient.PostInstanceDimensionsCalls() {
		So(call.Data, ShouldResemble, expected[call.Data.Name])
	}
}

// checks that PutInstance was called exactly once with expected paramters
func validatePutInstance(mockDatasetClient *mock.DatasetClientMock, expectedData *dataset.JobInstance) {
	So(len(mockDatasetClient.PutInstanceDataCalls()), ShouldEqual, 1)
	So(mockDatasetClient.PutInstanceDataCalls()[0].InstanceID, ShouldEqual, validInstanceID)
	So(mockDatasetClient.PutInstanceDataCalls()[0].ServiceAuthToken, ShouldEqual, validAuthToken)
	So(mockDatasetClient.PutInstanceDataCalls()[0].Data, ShouldResemble, *expectedData)
}
