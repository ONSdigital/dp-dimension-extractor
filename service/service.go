package service

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"

	"github.com/ONSdigital/dp-dimension-extractor/dimension"
	"github.com/ONSdigital/dp-dimension-extractor/schema"
	"github.com/ONSdigital/go-ns/avro"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/s3"
)

// ----------------------------------------------------------------------------

// Service represents the necessary config for dp-dimension-extractor
type Service struct {
	EnvMax       int64
	Consumer     *kafka.ConsumerGroup
	ImportAPIURL string
	Producer     kafka.Producer
	S3           *s3.S3
}

type inputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type dimensionExtracted struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

// ----------------------------------------------------------------------------

// Start handles consumption of events ad produces new events
func (svc *Service) Start() {
	log.Info("application started", log.Data{"import-api-url": svc.ImportAPIURL})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	healthChannel := make(chan bool)
	exitChannel := make(chan bool)

	go func() {
		for {
			select {
			case <-c:
				//Falls into this block when the service is shutdown to safely close the consumer
				log.Info("safely closing consumer and producer on service shutdown", nil)
				svc.Consumer.Closer <- true
				svc.Producer.Closer <- true
				exitChannel <- true
				return
			case message := <-svc.Consumer.Incoming:
				m, err := readMessage(message.GetData())
				if err != nil {
					log.ErrorC("could not unmarshal message", err, log.Data{"instanceID": m.InstanceID})
					continue
				}
				log.Info("received message", log.Data{"file-url": m.FileURL, "instance-id": m.InstanceID})

				// Get csv from S3 bucket using m.S3URL
				//s3Value := svc.s3Value
				file, err := svc.S3.Get(m.FileURL)
				if err != nil {
					log.ErrorC("encountered error retrieving csv file", err, log.Data{"instanceID": m.InstanceID})
					continue
				}

				log.Info("file successfully read from aws", log.Data{"instanceID": m.InstanceID})

				csvReader := csv.NewReader(file)

				// Scan and discard header row (for now) - the data rows contain sufficient information about the structure
				if _, err := csvReader.Read(); err != nil {
					log.ErrorC("encountered error immediately when processing header row", err, log.Data{"instanceID": m.InstanceID})
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
						log.ErrorC("encountered error reading csv", err, log.Data{"instanceID": m.InstanceID, "csv_line": line})
						break
					}

					dimensions, count, err = svc.sendRequests(m.InstanceID, dimensions, line, count)
					if err != nil {
						log.ErrorC("encountered error sending request to import API", err, log.Data{"instanceID": m.InstanceID, "csv_line": line})
						break
					}
				}
				log.Trace("count is:", log.Data{"instanceID": m.InstanceID, "count": count})

				// Once csv file has been iterated over and there were no errors,
				// send a completed messsage to the dimensions-extracted topic
				svc.Producer.Output <- producerMessage

				message.Commit()
			case errorMessage := <-svc.Consumer.Errors:
				log.Error(fmt.Errorf("aborting"), log.Data{"messageReceived": errorMessage})
				svc.Consumer.Closer <- true
				svc.Producer.Closer <- true
				exitChannel <- true
				return
			case <-healthChannel:
			}
		}
	}()
	<-exitChannel
	log.Info("service dimension extractor stopped", nil)
}

// ----------------------------------------------------------------------------

func readMessage(eventValue []byte) (inputFileAvailable, error) {
	inputFileAvailableSchema := &avro.Schema{
		Definition: schema.InputFileAvailable,
	}

	var i inputFileAvailable

	if err := inputFileAvailableSchema.Unmarshal(eventValue, &i); err != nil {
		return i, err
	}

	return i, nil
}

// ----------------------------------------------------------------------------

func (svc *Service) sendRequests(instanceID string, dimensions map[string]string, line []string, count int) (map[string]string, int, error) {
	for i := 3; i < len(line); i += 3 {
		var d dimension.Node
		d.Dimension = instanceID + "_" + line[i] + "_" + line[i+1] + "_" + line[i+2]
		d.DimensionValue = line[i+2]
		dimensionAlreadyExists := false

		// If dimension already exists add dimension to map
		if _, ok := dimensions[d.Dimension]; ok {
			dimensionAlreadyExists = true
		}

		// If dimension does not already exists in map (is unique), send request
		if !dimensionAlreadyExists {
			dimensions[d.Dimension] = d.Dimension

			url := svc.ImportAPIURL + "/import/" + instanceID + "/dimensions"
			if err := d.Put(http.DefaultClient, url); err != nil {
				log.ErrorC("encountered error calling import API", err, log.Data{"instanceID": instanceID, "csv_line": line})
				return dimensions, count, err
			}
			count++
		}
	}

	return dimensions, count, nil
}
