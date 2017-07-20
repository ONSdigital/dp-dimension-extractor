package handler

import (
	"encoding/csv"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	"github.com/ONSdigital/dp-dimension-extractor/instance"
	"github.com/ONSdigital/dp-dimension-extractor/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/s3"
)

type dimensionExtracted struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type inputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

// HandleMessage handles a message by sending requests to the import API
// before producing a new message to confirm successful completion
func HandleMessage(producer kafka.MessageProducer, s3 *s3.S3, importAPIURL string, message kafka.Message, maxRetries int) (string, error) {
	event, err := readMessage(message.GetData())
	if err != nil {
		log.Error(err, log.Data{"schema": "failed to unmarshal event"})
		return "", err
	}

	log.Debug("event received", log.Data{"file_url": event.FileURL, "instance_id": event.InstanceID})

	// Get csv from S3 bucket using m.S3URL
	file, err := s3.Get(event.FileURL)
	if err != nil {
		log.ErrorC("encountered error retrieving csv file", err, log.Data{"instance_id": event.InstanceID})
		return event.InstanceID, err
	}

	log.Debug("file successfully read from aws", log.Data{"instance_id": event.InstanceID})

	csvReader := csv.NewReader(file)

	// Scan for header row, this information will need to be sent to the
	// import API with the number of observations in a PUT request
	headerRow, err := csvReader.Read()
	if err != nil {
		log.ErrorC("encountered error immediately when processing header row", err, log.Data{"instance_id": event.InstanceID})
		return event.InstanceID, err
	}

	metaData := strings.Split(headerRow[0], "_")
	dimensionColumnOffset, err := strconv.Atoi(metaData[1])
	if err != nil {
		log.ErrorC("encountered error distinguishing dimension column offset", err, log.Data{"instance_id": event.InstanceID})
		return event.InstanceID, err
	}

	log.Trace("a list of headers", log.Data{"instance_id": event.InstanceID, "header_row": headerRow})

	var hasErrored bool
	dimensions := make(map[string]string)
	numberOfObservations := 0

	// Iterate over csv file pulling out unique dimensions
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.ErrorC("encountered error reading csv", err, log.Data{"instance_id": event.InstanceID, "csv_line": line})
			hasErrored = true
			break
		}

		dimension := dimension.New(dimensions, dimensionColumnOffset, importAPIURL, event.InstanceID, line, maxRetries)

		lineDimensions, err := dimension.Extract()
		if err != nil {
			log.ErrorC("encountered error retrieving dimensions", err, log.Data{"instance_id": event.InstanceID, "csv_line": line})
			hasErrored = true
			break
		}

		for _, request := range lineDimensions {
			if err := request.Put(http.DefaultClient); err != nil {
				log.ErrorC("encountered error sending request to import API", err, log.Data{"instance_id": event.InstanceID, "csv_line": line})
				hasErrored = true
				break
			}
		}

		if hasErrored {
			break
		}

		numberOfObservations++
	}

	// Check for any errors in processing of event before continuing
	// with process
	if hasErrored {
		return event.InstanceID, err
	}

	log.Trace("a count of the number of observations", log.Data{"instance_id": event.InstanceID, "number_of_observations": numberOfObservations})

	instance := instance.NewJobInstance(importAPIURL, event.InstanceID, numberOfObservations, headerRow, maxRetries)

	// PUT request to import API to pass the header row and the
	// number of observations that exist against this job instance
	if err := instance.PutData(http.DefaultClient); err != nil {
		log.ErrorC("encountered error sending request to import API", err, log.Data{"instance_id": event.InstanceID, "number_of_observations": numberOfObservations})
		return event.InstanceID, err
	}

	log.Trace("a list of headers", log.Data{"instance_id": event.InstanceID, "header_row": headerRow})
	// Once csv file has been iterated over and there were no errors,
	// send a completed messsage to the dimensions-extracted topic
	producerMessage, err := schema.DimensionsExtractedSchema.Marshal(&dimensionExtracted{
		FileURL:    event.FileURL,
		InstanceID: event.InstanceID,
	})

	producer.Output() <- producerMessage

	return event.InstanceID, nil
}

// ----------------------------------------------------------------------------

func readMessage(eventValue []byte) (inputFileAvailable, error) {
	var i inputFileAvailable

	if err := schema.InputFileAvailableSchema.Unmarshal(eventValue, &i); err != nil {
		return i, err
	}

	return i, nil
}
