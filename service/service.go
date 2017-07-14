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

// Start handles consumption of events and produces new events
func (svc *Service) Start() {
	log.Info("application started", log.Data{"import-api-url": svc.ImportAPIURL})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)

	healthCh := make(chan bool)
	exitCh := make(chan bool)

	go func() {
		for {
			select {
			case <-signals:
				//Falls into this block when the service is shutdown to safely close the consumer

				svc.Consumer.Closer <- true
				svc.Producer.Closer <- true
				exitCh <- true

				log.Info("graceful shutdown was successful", nil)
				return
			case message := <-svc.Consumer.Incoming:
				event, err := readMessage(message.GetData())
				if err != nil {
					log.Error(err, log.Data{"schema": "failed to unmarshal event"})
					continue
				}

				log.Debug("event received", log.Data{"file-url": event.FileURL, "instance-id": event.InstanceID})

				// Get csv from S3 bucket using m.S3URL
				file, err := svc.S3.Get(event.FileURL)
				if err != nil {
					log.ErrorC("encountered error retrieving csv file", err, log.Data{"instanceID": event.InstanceID})
					continue
				}

				log.Debug("file successfully read from aws", log.Data{"instanceID": event.InstanceID})

				csvReader := csv.NewReader(file)

				// Scan and discard header row (for now) - the data rows contain sufficient information about the structure
				if _, err := csvReader.Read(); err != nil {
					log.ErrorC("encountered error immediately when processing header row", err, log.Data{"instanceID": event.InstanceID})
				}

				dimensions := make(map[string]string)

				// Iterate over csv file pulling out unique dimensions
				for {
					line, err := csvReader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						log.ErrorC("encountered error reading csv", err, log.Data{"instanceID": event.InstanceID, "csv_line": line})
						break
					}

					dimension := dimension.New(dimensions, line, svc.ImportAPIURL, event.InstanceID)

					//log.Trace("did we get here", nil)
					lineDimensions, err := dimension.Extract()
					if err != nil {
						log.ErrorC("encountered error retrieving dimensions", err, log.Data{"instanceID": event.InstanceID, "csv_line": line})
						break
					}
					//log.Trace("did we get here 2", nil)

					for _, request := range lineDimensions {
						if err := request.Put(http.DefaultClient); err != nil {
							log.ErrorC("encountered error sending request to import API", err, log.Data{"instanceID": event.InstanceID, "csv_line": line})
							continue
						}
					}
				}

				// Once csv file has been iterated over and there were no errors,
				// send a completed messsage to the dimensions-extracted topic
				producerMessage, err := schema.DimensionsExtractedSchema.Marshal(&dimensionExtracted{
					FileURL:    event.FileURL,
					InstanceID: event.InstanceID,
				})

				svc.Producer.Output <- producerMessage

				log.Debug("event processed - committing message", log.Data{"instanceID": event.InstanceID})
				message.Commit()
				log.Debug("message committed", log.Data{"instanceID": event.InstanceID})
			case errorMessage := <-svc.Consumer.Errors:
				log.Error(fmt.Errorf("aborting"), log.Data{"messageReceived": errorMessage})
				svc.Consumer.Closer <- true
				svc.Producer.Closer <- true
				exitCh <- true
				return
			case <-healthCh:
			}
		}
	}()
	<-exitCh

	log.Info("service dimension extractor stopped", nil)
}

// ----------------------------------------------------------------------------

func readMessage(eventValue []byte) (inputFileAvailable, error) {
	var i inputFileAvailable

	if err := schema.InputFileAvailableSchema.Unmarshal(eventValue, &i); err != nil {
		return i, err
	}

	return i, nil
}
