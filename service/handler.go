package service

import (
	"encoding/csv"
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
	"github.com/ONSdigital/go-ns/s3"
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
func (svc *Service) handleMessage(ctx context.Context, message kafka.Message) (string, error) {
	producerMessage, instanceID, file, err := retrieveData(message, svc.S3)
	if err != nil {
		return instanceID, err
	}

	codelistMap, err := codelists.GetFromInstance(ctx, svc.DatasetAPIURL, svc.DatasetAPIAuthToken, instanceID, svc.HTTPClient)

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

	// create a new batch map (per DimensionID)
	// requestMap := make(map[string]dimension.RequestBatch)
	requestBatch := dimension.RequestBatch{
		DatasetAPIURL:       svc.DatasetAPIURL,
		DatasetAPIAuthToken: svc.DatasetAPIAuthToken,
		Batch:               make([]dimension.Request, 0, svc.DimensionBatchMaxSize),
	}
	requestsNextFree := 0

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

		dimension := dimension.Extract{
			InstanceID:            instanceID,
			Dimensions:            dimensions,
			DimensionColumnOffset: dimensionColumnOffset,
			HeaderRow:             headerRow,
			Line:                  line,
			TimeColumn:            timeColumn,
			CodelistMap:           codelistMap,
		}
		numberOfObservations++

		lineDimensions, err := dimension.Extract()
		if err != nil {
			log.ErrorC("encountered error retrieving dimensions", err, log.Data{"instance_id": instanceID, "csv_line": line})
			return instanceID, err
		}

		for _, request := range lineDimensions {
			requestBatch.Batch = append(requestBatch.Batch, request)
			requestsNextFree++
			if requestsNextFree == svc.DimensionBatchMaxSize {
				if err := svc.postBatch(ctx, instanceID, &requestBatch, numberOfObservations); err != nil {
					return instanceID, err
				}
				requestsNextFree = 0
			}
		}

	}
	if requestsNextFree > 0 {
		// post any remaining dimensons in requestBatch
		if err := svc.postBatch(ctx, instanceID, &requestBatch, numberOfObservations); err != nil {
			return instanceID, err
		}
	}

	log.Trace("a count of the number of observations", log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})

	instance := instance.NewJobInstance(svc.DatasetAPIURL, svc.DatasetAPIAuthToken, instanceID, numberOfObservations, headerRow, svc.MaxRetries)

	// PUT request to dataset API to pass the header row and the
	// number of observations that exist against this job instance
	if err := instance.PutData(ctx, svc.HTTPClient); err != nil {
		log.ErrorC("encountered error sending request to the dataset api", err, log.Data{"instance_id": instanceID, "number_of_observations": numberOfObservations})
		return instanceID, err
	}

	log.Trace("a list of headers", log.Data{"instance_id": instanceID, "header_row": headerRow})
	// Once csv file has been iterated over and there were no errors,
	// send a completed messsage to the dimensions-extracted topic

	svc.Producer.Output() <- producerMessage

	return instanceID, nil
}

// postBatch posts a batch of dimensions to the DatasetAPI
func (svc *Service) postBatch(ctx context.Context, instanceID string, requestBatch *dimension.RequestBatch, numberOfObservations int) error {
	log.Trace("sending request", log.Data{"instance_id": instanceID, "batch_size": len(requestBatch.Batch), "batch_cap": cap(requestBatch.Batch)})
	if err := requestBatch.Post(ctx, svc.HTTPClient, instanceID); err != nil {
		log.ErrorC("encountered error sending request to datset api", err, log.Data{"instance_id": instanceID, "observation_count": numberOfObservations})
		return err
	}
	requestBatch.Batch = requestBatch.Batch[0:0]
	return nil
}

func retrieveData(message kafka.Message, s3 *s3.S3) ([]byte, string, io.Reader, error) {
	event, err := readMessage(message.GetData())
	if err != nil {
		log.Error(err, log.Data{"schema": "failed to unmarshal event"})
		return nil, "", nil, err
	}

	s3URL, err := event.s3URL()
	if err != nil {
		log.ErrorC("encountered error parsing file URL", err, log.Data{"instance_id": event.InstanceID})
		return nil, event.InstanceID, nil, err
	}

	log.Debug("event received", log.Data{"file_url": event.FileURL, "s3_url": s3URL, "instance_id": event.InstanceID})

	// Get csv from S3 bucket
	file, err := s3.Get(s3URL)
	if err != nil {
		log.ErrorC("encountered error retrieving csv file", err, log.Data{"instance_id": event.InstanceID})
		return nil, event.InstanceID, nil, err
	}

	log.Debug("file successfully read from aws", log.Data{"instance_id": event.InstanceID})

	producerMessage, err := schema.DimensionsExtractedSchema.Marshal(&dimensionExtracted{
		FileURL:    s3URL,
		InstanceID: event.InstanceID,
	})

	return producerMessage, event.InstanceID, file, nil
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
