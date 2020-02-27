package service

import (
	"encoding/csv"
	"encoding/hex"
	"io"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-dimension-extractor/codelists"
	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	"github.com/ONSdigital/dp-dimension-extractor/instance"
	"github.com/ONSdigital/dp-dimension-extractor/schema"
	kafka "github.com/ONSdigital/dp-kafka"
	rchttp "github.com/ONSdigital/dp-rchttp"
	s3client "github.com/ONSdigital/dp-s3"
	"github.com/ONSdigital/log.go/log"
	"github.com/aws/aws-sdk-go/aws/session"
	"golang.org/x/net/context"
)

type dimensionExtracted struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type inputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

// VaultClient is an interface to represent methods called to action upon vault
type VaultClient interface {
	ReadKey(path, key string) (string, error)
}

// s3URL parses the fileURL into an S3Url struct. s3:// prefix is interpreted as
// DNS-Alias-virtual-hosted style. Otherwise, path-style is assumed.
func (inputFileAvailable *inputFileAvailable) s3URL() (*s3client.S3Url, error) {
	if strings.HasPrefix(inputFileAvailable.FileURL, "s3:") {
		// Assume DNS Alias Virtual Hosted style URL (e.g. s3://bucket/key)
		return s3client.ParseAliasVirtualHostedURL(inputFileAvailable.FileURL)
	}
	// Assume Path-style / Global-path-style URL (e.g. https://https://s3-eu-west-1.amazonaws.com/bucket/key)
	s3URL, err := s3client.ParseGlobalPathStyleURL(inputFileAvailable.FileURL)
	if err != nil {
		return nil, err
	}
	s3URL.Scheme = "s3"
	return s3URL, nil
}

// Service handles incoming messages.
type Service struct {
	AuthToken                  string
	DatasetAPIURL              string
	DatasetAPIAuthToken        string
	DimensionExtractedProducer *kafka.Producer
	DimensionExtractorURL      string
	EncryptionDisabled         bool
	EnvMax                     int64
	HTTPClient                 *rchttp.Client
	MaxRetries                 int
	AwsSession                 *session.Session
	S3Clients                  map[string]*s3client.S3
	VaultClient                VaultClient
	VaultPath                  string
}

// HandleMessage handles a message by sending requests to the dataset API
// before producing a new message to confirm successful completion
func (svc *Service) HandleMessage(ctx context.Context, message kafka.Message) (string, error) {
	producerMessage, instanceID, file, err := svc.retrieveData(message)
	if err != nil {
		return instanceID, err
	}
	defer file.Close()

	codelistMap, err := codelists.GetFromInstance(ctx, svc.DatasetAPIURL, svc.DatasetAPIAuthToken, svc.AuthToken, instanceID, svc.HTTPClient)
	if err != nil {
		log.Event(ctx, "encountered error immediately when requesting data from the dataset api", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	csvReader := csv.NewReader(file)

	// Scan for header row, this information will need to be sent to the
	// dataset API with the number of observations in a PUT request
	headerRow, err := csvReader.Read()
	if err != nil {
		log.Event(ctx, "encountered error immediately when processing header row", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	timeColumn := checkHeaderForTime(headerRow)

	metaData := strings.Split(headerRow[0], "_")
	dimensionColumnOffset, err := strconv.Atoi(metaData[1])
	if err != nil {
		log.Event(ctx, "encountered error distinguishing dimension column offset", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	// Meta data for dimension column offset does not consider the observation column, so add 1 to value
	dimensionColumnOffset = dimensionColumnOffset + 1

	log.Event(ctx, "a list of headers", log.INFO, log.Data{"instance_id": instanceID, "header_row": headerRow})

	dimensions := make(map[string]string)
	numberOfObservations := 0

	// Iterate over csv file pulling out unique dimensions
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Event(ctx, "encountered error reading csv", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID, "csv_line": line})
			return instanceID, err
		}

		dim := dimension.Extract{
			AuthToken:             svc.AuthToken,
			Dimensions:            dimensions,
			DimensionColumnOffset: dimensionColumnOffset,
			HeaderRow:             headerRow,
			DatasetAPIURL:         svc.DatasetAPIURL,
			DatasetAPIAuthToken:   svc.DatasetAPIAuthToken,
			InstanceID:            instanceID,
			Line:                  line,
			MaxRetries:            svc.MaxRetries,
			TimeColumn:            timeColumn,
			CodelistMap:           codelistMap,
		}

		lineDimensions, err := dim.Extract()
		if err != nil {
			log.Event(ctx, "encountered error retrieving dimensions", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID, "csv_line": line})
			return instanceID, err
		}

		for _, request := range lineDimensions {
			if err := request.Post(ctx, svc.HTTPClient); err != nil {
				log.Event(ctx, "encountered error sending request to datset api", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID, "csv_line": line})
				return instanceID, err
			}
		}

		numberOfObservations++
	}

	log.Event(ctx, "a count of the number of observations", log.INFO, log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})

	jobInstance := instance.NewJobInstance(svc.AuthToken, svc.DatasetAPIURL, svc.DatasetAPIAuthToken, instanceID, numberOfObservations, headerRow, svc.MaxRetries)

	// PUT request to dataset API to pass the header row and the
	// number of observations that exist against this job instance
	if err := jobInstance.PutData(ctx, svc.HTTPClient); err != nil {
		log.Event(ctx, "encountered error sending request to the dataset api", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})
		return instanceID, err
	}

	log.Event(ctx, "a list of headers", log.INFO, log.Data{"instance_id": instanceID, "header_row": headerRow})
	// Once csv file has been iterated over and there were no errors,
	// send a completed messsage to the dimensions-extracted topic

	svc.DimensionExtractedProducer.Channels().Output <- producerMessage

	return instanceID, nil
}

func (svc *Service) retrieveData(message kafka.Message) ([]byte, string, io.ReadCloser, error) {

	event, err := readMessage(message.GetData())
	if err != nil {
		log.Event(nil, "error reading message", log.ERROR, log.Error(err), log.Data{"schema": "failed to unmarshal event"})
		return nil, "", nil, err
	}

	logData := log.Data{"instance_id": event.InstanceID, "event": event}

	s3URL, err := event.s3URL()
	if err != nil {
		log.Event(nil, "encountered error parsing file URL", log.ERROR, log.Error(err), logData)
		return nil, event.InstanceID, nil, err
	}
	s3URLStr, err := s3URL.String(s3client.StyleAliasVirtualHosted)
	if err != nil {
		log.Event(nil, "unable to represent S3URL from parsed file URL", log.ERROR, log.Error(err), logData)
		return nil, event.InstanceID, nil, err
	}

	logData["file_url"] = event.FileURL
	logData["s3_url"] = s3URLStr
	logData["bucket"] = s3URL.BucketName
	logData["filename"] = s3URL.Key

	log.Event(nil, "event received", log.INFO, logData)

	// Get S3 Client corresponding to the Bucket extracted from URL, or create one if not available
	s3, ok := svc.S3Clients[s3URL.BucketName]
	if !ok {
		log.Event(nil, "Retreiving data from unexpected S3 bucket", log.WARN, log.Data{"RequestedBucket": s3URL.BucketName})
		s3 = s3client.NewClientWithSession(s3URL.BucketName, !svc.EncryptionDisabled, svc.AwsSession)
	}

	var output io.ReadCloser

	if !svc.EncryptionDisabled {
		path := svc.VaultPath + "/" + s3URL.Key
		vaultKey := "key"

		pskStr, err := svc.VaultClient.ReadKey(path, vaultKey)
		if err != nil {
			return nil, event.InstanceID, nil, err
		}
		psk, err := hex.DecodeString(pskStr)
		if err != nil {
			return nil, event.InstanceID, nil, err
		}

		output, err = s3.GetWithPSK(s3URL.Key, psk)
		if err != nil {
			log.Event(nil, "encountered error retrieving and decrypting csv file", log.ERROR, log.Error(err), logData)
			return nil, event.InstanceID, nil, err
		}
	} else {
		output, err = s3.Get(s3URL.Key)
		if err != nil {
			log.Event(nil, "encountered error retrieving csv file", log.ERROR, log.Error(err), logData)
			return nil, event.InstanceID, nil, err
		}
	}

	log.Event(nil, "file successfully read from aws", log.INFO, logData)

	producerMessage, err := schema.DimensionsExtractedSchema.Marshal(&dimensionExtracted{
		FileURL:    s3URLStr,
		InstanceID: event.InstanceID,
	})
	if err != nil {
		output.Close()
		return nil, event.InstanceID, nil, err
	}

	return producerMessage, event.InstanceID, output, nil
}

func readMessage(eventValue []byte) (*inputFileAvailable, error) {
	var i inputFileAvailable

	if err := schema.InputFileAvailableSchema.Unmarshal(eventValue, &i); err != nil {
		return nil, err
	}

	return &i, nil
}

func checkHeaderForTime(headerNames []string) int {
	for index, name := range headerNames {
		if strings.ToLower(name) == "time" {
			return index
		}
	}

	return 0
}
