package service

import (
	"encoding/csv"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"

	"net/url"

	"github.com/ONSdigital/dp-dimension-extractor/codelists"
	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	"github.com/ONSdigital/dp-dimension-extractor/instance"
	"github.com/ONSdigital/dp-dimension-extractor/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
	"github.com/ONSdigital/s3crypto"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/net/context"
)

const chunkSize = 5 * 1024 * 1024

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

func (inputFileAvailable *inputFileAvailable) s3URL() (string, error) {
	if strings.HasPrefix(inputFileAvailable.FileURL, "s3:") {
		return inputFileAvailable.FileURL, nil
	}
	url, err := url.Parse(inputFileAvailable.FileURL)
	if err != nil {
		return "", err
	}

	return "s3:/" + url.Path, nil
}

// Service handles incoming messages.
type Service struct {
	AuthToken                  string
	DatasetAPIURL              string
	DatasetAPIAuthToken        string
	DimensionExtractedProducer kafka.Producer
	DimensionExtractorURL      string
	EncryptionDisabled         bool
	EnvMax                     int64
	HTTPClient                 *rchttp.Client
	MaxRetries                 int
	S3                         *session.Session
	VaultClient                VaultClient
	VaultPath                  string
}

// HandleMessage handles a message by sending requests to the dataset API
// before producing a new message to confirm successful completion
func (svc *Service) HandleMessage(ctx context.Context, message kafka.Message) (string, error) {
	producerMessage, instanceID, output, err := retrieveData(message, svc.S3, svc.EncryptionDisabled, svc.VaultClient, svc.VaultPath)
	if err != nil {
		return instanceID, err
	}
	file := output.Body
	defer output.Body.Close()

	codelistMap, err := codelists.GetFromInstance(ctx, svc.DatasetAPIURL, svc.DatasetAPIAuthToken, svc.AuthToken, instanceID, svc.HTTPClient)
	if err != nil {
		log.ErrorC("encountered error immediately when requesting data from the dataset api", err, log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	csvReader := csv.NewReader(file)

	// Scan for header row, this information will need to be sent to the
	// dataset API with the number of observations in a PUT request
	headerRow, err := csvReader.Read()
	if err != nil {
		log.ErrorC("encountered error immediately when processing header row", err, log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	timeColumn := checkHeaderForTime(headerRow)

	metaData := strings.Split(headerRow[0], "_")
	dimensionColumnOffset, err := strconv.Atoi(metaData[1])
	if err != nil {
		log.ErrorC("encountered error distinguishing dimension column offset", err, log.Data{"instance_id": instanceID})
		return instanceID, err
	}

	// Meta data for dimension column offset does not consider the observation column, so add 1 to value
	dimensionColumnOffset = dimensionColumnOffset + 1

	log.Trace("a list of headers", log.Data{"instance_id": instanceID, "header_row": headerRow})

	dimensions := make(map[string]string)
	numberOfObservations := 0

	// Iterate over csv file pulling out unique dimensions
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.ErrorC("encountered error reading csv", err, log.Data{"instance_id": instanceID, "csv_line": line})
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
			log.ErrorC("encountered error retrieving dimensions", err, log.Data{"instance_id": instanceID, "csv_line": line})
			return instanceID, err
		}

		for _, request := range lineDimensions {
			if err := request.Post(ctx, svc.HTTPClient); err != nil {
				log.ErrorC("encountered error sending request to datset api", err, log.Data{"instance_id": instanceID, "csv_line": line})
				return instanceID, err
			}
		}

		numberOfObservations++
	}

	log.Trace("a count of the number of observations", log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})

	jobInstance := instance.NewJobInstance(svc.AuthToken, svc.DatasetAPIURL, svc.DatasetAPIAuthToken, instanceID, numberOfObservations, headerRow, svc.MaxRetries)

	// PUT request to dataset API to pass the header row and the
	// number of observations that exist against this job instance
	if err := jobInstance.PutData(ctx, svc.HTTPClient); err != nil {
		log.ErrorC("encountered error sending request to the dataset api", err, log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})
		return instanceID, err
	}

	log.Trace("a list of headers", log.Data{"instance_id": instanceID, "header_row": headerRow})
	// Once csv file has been iterated over and there were no errors,
	// send a completed messsage to the dimensions-extracted topic

	svc.DimensionExtractedProducer.Output() <- producerMessage

	return instanceID, nil
}

func retrieveData(message kafka.Message, sess *session.Session, encryptionDisabled bool, vc VaultClient, vaultPath string) ([]byte, string, *s3.GetObjectOutput, error) {
	event, err := readMessage(message.GetData())
	if err != nil {
		log.Error(err, log.Data{"schema": "failed to unmarshal event"})
		return nil, "", nil, err
	}

	logData := log.Data{"instance_id": event.InstanceID, "event": event}

	s3URL, err := event.s3URL()
	if err != nil {
		log.ErrorC("encountered error parsing file URL", err, logData)
		return nil, event.InstanceID, nil, err
	}

	bucket, filename, err := getBucketAndFilename(s3URL)
	if err != nil {
		log.ErrorC("unable to find bucket and filename in event file url", err, logData)
		return nil, event.InstanceID, nil, err
	}
	logData["file_url"] = event.FileURL
	logData["s3_url"] = s3URL
	logData["bucket"] = bucket
	logData["filename"] = filename

	log.Debug("event received", logData)

	var output *s3.GetObjectOutput

	// Get csv from S3 bucket
	getInput := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &filename,
	}

	if !encryptionDisabled {
		client := s3crypto.New(sess, &s3crypto.Config{HasUserDefinedPSK: true, MultipartChunkSize: chunkSize})

		path := vaultPath + "/" + filename
		vaultKey := "key"

		pskStr, err := vc.ReadKey(path, vaultKey)
		if err != nil {
			return nil, event.InstanceID, nil, err
		}
		psk, err := hex.DecodeString(pskStr)
		if err != nil {
			return nil, event.InstanceID, nil, err
		}

		output, err = client.GetObjectWithPSK(getInput, psk)
		if err != nil {
			log.ErrorC("encountered error retrieving and decrypting csv file", err, logData)
			return nil, event.InstanceID, nil, err
		}
	} else {
		client := s3.New(sess)

		output, err = client.GetObject(getInput)
		if err != nil {
			log.ErrorC("encountered error retrieving csv file", err, logData)
			return nil, event.InstanceID, nil, err
		}
	}

	log.Debug("file successfully read from aws", logData)

	producerMessage, err := schema.DimensionsExtractedSchema.Marshal(&dimensionExtracted{
		FileURL:    s3URL,
		InstanceID: event.InstanceID,
	})
	if err != nil {
		output.Body.Close()
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

// FIXME function will fail to retrieve correct file location if folder
// structure is to be introduced in s3 bucket
func getBucketAndFilename(s3URL string) (string, string, error) {
	urlSplitz := strings.Split(s3URL, "/")
	n := len(urlSplitz)
	if n < 3 {
		return "", "", errors.New("could not find bucket or filename in file url")
	}
	bucket := urlSplitz[n-2]
	filename := urlSplitz[n-1]
	if filename == "" {
		return "", "", errors.New("missing filename in file url")
	}
	if bucket == "" {
		return "", "", errors.New("missing bucket name in file url")
	}

	return bucket, filename, nil
}
