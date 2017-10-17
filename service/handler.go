package service

import (
	"encoding/csv"
	"io"
	"net/http"
	"strconv"
	"strings"

	"net/url"

	"fmt"
	"github.com/ONSdigital/dp-dimension-extractor/codelists"
	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	"github.com/ONSdigital/dp-dimension-extractor/instance"
	"github.com/ONSdigital/dp-dimension-extractor/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/s3"
	"github.com/pkg/errors"
)

type dimensionExtracted struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type inputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
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

// handleMessage handles a message by sending requests to the dataset API
// before producing a new message to confirm successful completion
func (svc *Service) handleMessage(message kafka.Message) (string, error) {
	producerMessage, instanceID, file, err := retrieveData(message, svc.S3)
	if err != nil {
		return instanceID, err
	}

	codelistMap, err := codelists.GetFromInstance(svc.DatasetAPIURL, instanceID, http.DefaultClient)
	if err != nil {
		return instanceID, errors.Wrap(err, fmt.Sprintf("encountered error immediately when requesting data from the dataset api: instance_id=%s", instanceID))
	}

	csvReader := csv.NewReader(file)

	// Scan for header row, this information will need to be sent to the
	// dataset API with the number of observations in a PUT request
	headerRow, err := csvReader.Read()
	if err != nil {
		return instanceID, errors.Wrap(err, fmt.Sprintf("encountered error immediately when processing header row: instance_id=%s", instanceID))
	}

	timeColumn := checkHeaderForTime(headerRow)

	metaData := strings.Split(headerRow[0], "_")
	dimensionColumnOffset, err := strconv.Atoi(metaData[1])
	if err != nil {
		return instanceID, errors.Wrap(err, fmt.Sprintf("encountered error distinguishing dimension column offset: instance_id=%s", instanceID))
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
			return instanceID, errors.Wrap(err, fmt.Sprintf("encountered error reading csv: instance_id=%s, csv_line=%s", instanceID, line))
		}

		dimension := dimension.Extract{
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

		lineDimensions, err := dimension.Extract()
		if err != nil {
			return instanceID, errors.Wrap(err, fmt.Sprintf("encountered error retrieving dimensions: instance_id=%s, csv_line=%s", instanceID, line))
		}

		for _, request := range lineDimensions {
			if err := request.Post(http.DefaultClient); err != nil {
				return instanceID, errors.Wrap(err, fmt.Sprintf("encountered error sending request to datset api: instance_id=%s, csv_line=%s", instanceID, line))
			}
		}

		numberOfObservations++
	}

	log.Trace("a count of the number of observations", log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})

	instance := instance.NewJobInstance(svc.DatasetAPIURL, svc.DatasetAPIAuthToken, instanceID, numberOfObservations, headerRow, svc.MaxRetries)

	// PUT request to dataset API to pass the header row and the
	// number of observations that exist against this job instance
	if err := instance.PutData(http.DefaultClient); err != nil {
		return instanceID, errors.Wrap(err, fmt.Sprintf("encountered error sending request to the dataset api: insatnce_id=%s, number_of_observations=%d", instanceID, numberOfObservations))
	}

	log.Trace("a list of headers", log.Data{"instance_id": instanceID, "header_row": headerRow})
	// Once csv file has been iterated over and there were no errors,
	// send a completed messsage to the dimensions-extracted topic

	svc.Producer.Output() <- producerMessage

	return instanceID, nil
}

func retrieveData(message kafka.Message, s3 *s3.S3) ([]byte, string, io.Reader, error) {
	event, err := readMessage(message.GetData())
	if err != nil {
		return nil, "", nil, err
	}

	s3URL, err := event.s3URL()
	if err != nil {
		return nil, event.InstanceID, nil, errors.Wrap(err, fmt.Sprintf("encountered error parsing file URL: instance_id:%s", event.InstanceID))
	}

	log.Debug("event received", log.Data{"file_url": event.FileURL, "s3_url": s3URL, "instance_id": event.InstanceID})

	// Get csv from S3 bucket
	file, err := s3.Get(s3URL)
	if err != nil {
		details := fmt.Sprintf("encountered error retrieving csv file: instance_id=%s, S3URL=%s", event.InstanceID, s3URL)
		return nil, event.InstanceID, nil, errors.Wrap(err, details)
	}

	log.Debug("file successfully read from aws", log.Data{"instance_id": event.InstanceID})

	dimensionExtractedEvent := &dimensionExtracted{
		FileURL:    s3URL,
		InstanceID: event.InstanceID,
	}
	producerMessage, err := schema.DimensionsExtractedSchema.Marshal(dimensionExtractedEvent)
	if err != nil {
		details := fmt.Sprintf("error while attempting to marshal service.dimensionExtractedEvent: event=%v", *dimensionExtractedEvent)
		return nil, event.InstanceID, nil, errors.Wrap(err, details)
	}

	return producerMessage, event.InstanceID, file, nil
}

func readMessage(eventValue []byte) (*inputFileAvailable, error) {
	var i inputFileAvailable

	if err := schema.InputFileAvailableSchema.Unmarshal(eventValue, &i); err != nil {
		return nil, errors.Wrap(err, "error while attempting to unmarshal kakfa message to service.inputFileAvailable")
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
