package service

import (
	"encoding/csv"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	"github.com/ONSdigital/dp-dimension-extractor/schema"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	s3client "github.com/ONSdigital/dp-s3/v3"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/net/context"
)

// DimensionExtracted represents a kafka avro model for a dimension extracted file for an instance
type DimensionExtracted struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

// InputFileAvailable represents a kafka avro model for an available input file fo an instance
type InputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

// S3URL parses the fileURL into an S3Url struct. s3:// prefix is interpreted as
// DNS-Alias-virtual-hosted style. Otherwise, path-style is assumed.
func (inputFileAvailable *InputFileAvailable) S3URL() (*s3client.S3Url, error) {
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
	DimensionExtractedProducer KafkaProducer
	EncryptionDisabled         bool
	DatasetClient              DatasetClient
	AwsConfig                  *aws.Config
	S3Clients                  map[string]S3Client
	VaultClient                VaultClient
	VaultPath                  string
}

// HandleMessage handles a message by sending requests to the dataset API
// before producing a new message to confirm successful completion
func (svc *Service) HandleMessage(ctx context.Context, message kafka.Message) (string, error) {

	producerMessage, instanceID, file, err := svc.retrieveData(ctx, message)
	if err != nil {
		return instanceID, err
	}
	defer file.Close()

	codeLists, _, err := svc.DatasetClient.GetInstance(ctx, "", svc.AuthToken, "", instanceID, headers.IfMatchAnyETag)
	if err != nil {
		log.Error(ctx, "encountered error immediately when requesting data from the dataset api", err, log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	codelistMap := make(map[string]string)
	for _, cl := range codeLists.Dimensions {
		codelistMap[cl.Name] = cl.ID
	}

	csvReader := csv.NewReader(file)

	// Scan for header row, this information will need to be sent to the
	// dataset API with the number of observations in a PUT request
	headerRow, err := csvReader.Read()
	if err != nil {
		log.Error(ctx, "encountered error immediately when processing header row", err, log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	metaData := strings.Split(headerRow[0], "_")
	if len(metaData) < 2 {
		err = errors.New("no underscore in header row")
		log.Error(ctx, "encountered badly-formatted header row", err, log.Data{"instance_id": instanceID})
		return instanceID, err
	}
	dimensionColumnOffset, err := strconv.Atoi(metaData[1])
	if err != nil {
		log.Error(ctx, "encountered error distinguishing dimension column offset", err, log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	// Meta data for dimension column offset does not consider the observation column, so add 1 to value
	dimensionColumnOffset++

	log.Info(ctx, "a list of headers", log.Data{"instance_id": instanceID, "header_row": headerRow})

	dimensionOptions := make(map[string]dataset.OptionPost)
	numberOfObservations := 0
	numberOfOptions := 0

	// Iterate over csv file pulling out unique dimensions
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error(ctx, "encountered error reading csv", err, log.Data{"instance_id": instanceID, "csv_line": line})
			return instanceID, err
		}

		dim := dimension.Extract{
			DimensionColumnOffset: dimensionColumnOffset,
			HeaderRow:             headerRow,
			InstanceID:            instanceID,
			Line:                  line,
			CodelistMap:           codelistMap,
		}

		lineDimensions, err := dim.Extract()
		if err != nil {
			log.Error(ctx, "encountered error retrieving dimensions", err, log.Data{"instance_id": instanceID, "csv_line": line})
			return instanceID, err
		}

		// loop through each dimension option identified in the CSV row
		for optionKey, optionToPost := range lineDimensions {

			// if the option is already in the full map of options then do not add it
			if _, ok := dimensionOptions[optionKey]; ok {
				continue
			}

			dimensionOptions[optionKey] = optionToPost
			numberOfOptions++
		}

		numberOfObservations++
	}

	log.Info(ctx, "dimensions extracted from the import file", log.Data{
		"instance_id":                 instanceID,
		"number_of_observations":      numberOfObservations,
		"number_of_dimension_options": numberOfOptions,
	})

	for optionKey, optionToPost := range dimensionOptions {
		if _, err := svc.DatasetClient.PostInstanceDimensions(ctx, svc.AuthToken, instanceID, optionToPost, headers.IfMatchAnyETag); err != nil {
			log.Error(ctx, "encountered error sending request to dataset api", err, log.Data{"instance_id": instanceID, "dimension_option": optionKey})
			return instanceID, err
		}
	}

	// PUT request to dataset API to pass the header row and the number of observations that exist against this job instance
	_, err = svc.DatasetClient.PutInstanceData(
		ctx,
		svc.AuthToken,
		instanceID,
		dataset.JobInstance{
			HeaderNames:          headerRow,
			NumberOfObservations: numberOfObservations,
		}, headers.IfMatchAnyETag)
	if err != nil {
		log.Error(ctx, "encountered error sending request to the dataset api", err, log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})
		return instanceID, err
	}
	log.Info(ctx, "successfully sent request to dataset API", log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})

	// Once csv file has been iterated over and there were no errors,
	// send a completed messsage to the dimensions-extracted topic

	svc.DimensionExtractedProducer.Channels().Output <- producerMessage

	return instanceID, nil
}

func (svc *Service) retrieveData(ctx context.Context, message kafka.Message) ([]byte, string, io.ReadCloser, error) {

	event, err := readMessage(message.GetData())
	if err != nil {
		log.Error(ctx, "error reading message", err, log.Data{"schema": "failed to unmarshal event"})
		return nil, "", nil, err
	}

	logData := log.Data{"instance_id": event.InstanceID, "event": event}

	s3URL, err := event.S3URL()
	if err != nil {
		log.Error(ctx, "encountered error parsing file url", err, logData)
		return nil, event.InstanceID, nil, err
	}
	s3URLStr, err := s3URL.String(s3client.AliasVirtualHostedStyle)
	if err != nil {
		log.Error(ctx, "unable to represent s3 url from parsed file url", err, logData)
		return nil, event.InstanceID, nil, err
	}

	logData["file_url"] = event.FileURL
	logData["s3_url"] = s3URLStr
	logData["bucket"] = s3URL.BucketName
	logData["filename"] = s3URL.Key

	log.Info(ctx, "event received", logData)

	// Get S3 Client corresponding to the Bucket extracted from URL, or create one if not available
	s3, ok := svc.S3Clients[s3URL.BucketName]
	if !ok {
		log.Warn(ctx, "retreiving data from unexpected s3 bucket", log.Data{"RequestedBucket": s3URL.BucketName})
		s3 = s3client.NewClientWithConfig(s3URL.BucketName, *svc.AwsConfig)
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

		output, _, err = s3.GetWithPSK(ctx, s3URL.Key, psk)
		if err != nil {
			log.Error(ctx, "encountered error retrieving and decrypting csv file", err, logData)
			return nil, event.InstanceID, nil, err
		}
	} else {
		output, _, err = s3.Get(ctx, s3URL.Key)
		if err != nil {
			log.Error(ctx, "encountered error retrieving csv file", err, logData)
			return nil, event.InstanceID, nil, err
		}
	}

	log.Info(ctx, "file successfully read from aws", logData)

	producerMessage, err := schema.DimensionsExtractedSchema.Marshal(&DimensionExtracted{
		FileURL:    s3URLStr,
		InstanceID: event.InstanceID,
	})
	if err != nil {
		output.Close()
		return nil, event.InstanceID, nil, err
	}

	return producerMessage, event.InstanceID, output, nil
}

func readMessage(eventValue []byte) (*InputFileAvailable, error) {
	var i InputFileAvailable

	if err := schema.InputFileAvailableSchema.Unmarshal(eventValue, &i); err != nil {
		return nil, err
	}

	return &i, nil
}
