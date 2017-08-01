package service

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/s3"
)

// Service represents the necessary config for dp-dimension-extractor
type Service struct {
	EnvMax       int64
	Consumer     kafka.MessageConsumer
	ImportAPIURL string
	MaxRetries   int
	Producer     kafka.MessageProducer
	S3           *s3.S3
}

// Start handles consumption of events
func (svc *Service) Start() {
	log.Info("application started", log.Data{"import_api_url": svc.ImportAPIURL})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)

	healthCh := make(chan bool)
	exitCh := make(chan bool)

	go func() {
		for {
			select {
			case <-signals:
				//Falls into this block when the service is shutdown to safely close the consumer

				svc.Consumer.Closer() <- true
				svc.Producer.Closer() <- true
				exitCh <- true

				log.Info("graceful shutdown was successful", nil)
				return
			case message := <-svc.Consumer.Incoming():

				instanceID, err := svc.handleMessage(message)
				if err != nil {
					log.ErrorC("event failed to process", err, log.Data{"instance_id": instanceID})
				} else {
					log.Debug("event successfully processed", log.Data{"instance_id": instanceID})
				}

				message.Commit()
				log.Debug("message committed", log.Data{"instance_id": instanceID})
			case errorMessage := <-svc.Consumer.Errors():
				log.Error(fmt.Errorf("aborting"), log.Data{"message_received": errorMessage})
				svc.Consumer.Closer() <- true
				svc.Producer.Closer() <- true
				exitCh <- true
				return
			case <-healthCh:
			}
		}
	}()
	<-exitCh

	log.Info("service dimension extractor stopped", nil)
}
