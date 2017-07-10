package service

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"

	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	"github.com/ONSdigital/dp-dimension-extractor/schema"
	"github.com/ONSdigital/go-ns/avro"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/s3"
)

// Service represents the necessary config for dp-dimension-extractor
type Service struct {
	EnvMax        int64
	ConsumerGroup *kafka.ConsumerGroup
	ImportAPIURL  string
	Producer      kafka.Producer
	S3            *s3.S3
}

type inputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type dimensionExtracted struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

// Start handles consumption of events ad produces new events
func (svc *Service) Start() {
	log.Info("application started", log.Data{"consumer-group": svc.ConsumerGroup, "producer": svc.Producer, "import-api-url": svc.ImportAPIURL})
	healthChannel := make(chan bool)
	exitChannel := make(chan bool)

	go func() {
		for {
			select {
			case message := <-svc.ConsumerGroup.Incoming:
				m, err := readMessage(message.GetData())
				if err != nil {
					log.ErrorC("could not unmarshal message", err, nil)
					continue
				}
				log.Info("received message", log.Data{"file-url": m.FileURL, "instance-id": m.InstanceID})

				// Get csv from S3 bucket using m.S3URL
				//s3Value := svc.s3Value
				file, err := (svc.S3).Get(m.FileURL)
				if err != nil {
					log.ErrorC("encountered error retrieving csv file", err, nil)
					continue
				}

				log.Info("just about to iterate", nil)

				csvReader := csv.NewReader(file)

				// Scan and discard header row (for now) - the data rows contain sufficient information about the structure
				if _, err := csvReader.Read(); err != nil {
					log.ErrorC("encountered error immediately when processing header row", err, nil)
				}

				dimensionExtractedSchema := &avro.Schema{
					Definition: schema.DimensionsExtracted,
				}

				producerMessage, err := dimensionExtractedSchema.Marshal(&dimensionExtracted{
					FileURL:    m.FileURL,
					InstanceID: m.InstanceID,
				})

				dimensions := make(map[string]string)
				count := 0

				// Iterate over csv file pulling out unique dimensions
				for {
					line, err := csvReader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						log.ErrorC("encountered error reading csv", err, log.Data{"csv_line": line})
						break
					}

					dimensions, count, err = svc.sendRequests(m.InstanceID, count, dimensions, line)
					if err != nil {
						log.ErrorC("encountered error sending request to import API", err, log.Data{"csv_line": line})
						break
					}
				}
				log.Trace("count is:", log.Data{"count": count})

				// Once csv file has been iterated over and there were no errors,
				// send a completed messsage to the dimensions-extracted topic
				svc.Producer.Output <- producerMessage

				message.Commit()
			case errorMessage := <-svc.ConsumerGroup.Errors:
				log.Error(fmt.Errorf("aborting"), log.Data{"messageReceived": errorMessage})
				exitChannel <- true
				return
			case <-healthChannel:
			}
		}
	}()
	<-exitChannel
	log.Info("service publish scheduler stopped", nil)
}

func readMessage(eventValue []byte) (inputFileAvailable, error) {
	inputFileAvailableSchema := &avro.Schema{
		Definition: schema.InputFileAvailable,
	}

	var i inputFileAvailable

	if err := inputFileAvailableSchema.Unmarshal(eventValue, &i); err != nil {
		return i, err
	}

	log.Info("file successfully read from aws", nil)

	return i, nil
}

func (svc *Service) sendRequests(instanceID string, count int, dimensions map[string]string, line []string) (map[string]string, int, error) {
	for i := 3; i < len(line); i += 3 {
		var d dimension.Node
		d.Dimension = instanceID + "_" + line[i] + "_" + line[i+1] + "_" + line[i+2]
		d.DimensionValue = line[i+2]
		dimensionAlreadyExists := false

		// Check if dimension already exists
		if _, ok := dimensions[d.Dimension]; ok {
			dimensionAlreadyExists = true
		}

		if !dimensionAlreadyExists {
			dimensions[d.Dimension] = d.Dimension

			// Send request to import api `<url>/imports/<instance_id>/dimensions if dimension is unique
			url := svc.ImportAPIURL + "/import/" + instanceID + "/dimensions"
			if err := d.Put(http.DefaultClient, url); err != nil {
				log.ErrorC("encountered error calling import API", err, log.Data{"csv_line": line})
				return dimensions, count, err
			}
			count++
		}
	}

	return dimensions, count, nil
}
