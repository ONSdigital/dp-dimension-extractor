package service

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/ONSdigital/dp-dimension-extractor/api"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
	"github.com/ONSdigital/go-ns/s3"
)

// Service represents the necessary config for dp-dimension-extractor
type Service struct {
	EnvMax                int64
	BindAddr              string
	Consumer              *kafka.ConsumerGroup
	DatasetAPIURL         string
	DatasetAPIAuthToken   string
	DimensionExtractorURL string
	HTTPClient            *rchttp.Client
	MaxRetries            int
	Producer              kafka.Producer
	S3                    *s3.S3
	Shutdown              time.Duration
}

// Start handles consumption of events
func (svc *Service) Start() {
	log.Info("application started", log.Data{"dataset_api_url": svc.DatasetAPIURL})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	serviceLoopDone := make(chan bool)
	listeningToConsumerGroupStopped := make(chan bool)
	readyToCloseOutboundConnections := make(chan bool)
	apiErrors := make(chan error, 1)

	svc.HTTPClient = rchttp.DefaultClient

	ctx, cancel := context.WithCancel(context.Background())

	api.CreateDimensionExtractorAPI(svc.DimensionExtractorURL, svc.BindAddr, apiErrors)

	go func() {
		for {
			select {
			case <-listeningToConsumerGroupStopped:
				// This case allows any in flight process to finish before
				// closing any outbound connections
				readyToCloseOutboundConnections <- true
				return
			case message := <-svc.Consumer.Incoming():

				instanceID, err := svc.handleMessage(ctx, message)
				if err != nil {
					log.ErrorC("event failed to process", err, log.Data{"instance_id": instanceID})
				} else {
					log.Debug("event successfully processed", log.Data{"instance_id": instanceID})
				}

				message.Commit()
				log.Debug("message committed", log.Data{"instance_id": instanceID})
			case consumerError := <-svc.Consumer.Errors():
				log.Error(fmt.Errorf("aborting consumer"), log.Data{"message_received": consumerError})
				close(serviceLoopDone)
			case producerError := <-svc.Producer.Errors():
				log.Error(fmt.Errorf("aborting producer"), log.Data{"message_received": producerError})
				close(serviceLoopDone)
			case <-apiErrors:
				log.Error(fmt.Errorf("server error forcing shutdown"), nil)
				close(serviceLoopDone)
			}
		}
	}()

	select {
	case <-serviceLoopDone:
		log.Debug("Quitting after done was closed", nil)
	case signal := <-signals:
		log.Debug("Quitting after os signal received", log.Data{"signal": signal})
	}

	childContext, childCancel := context.WithTimeout(ctx, svc.Shutdown)

	waitGroup := int32(0)

	atomic.AddInt32(&waitGroup, 1)
	go func() {
		log.Debug("Attempting to stop listening to kafka consumer group", nil)
		svc.Consumer.StopListeningToConsumer(childContext)
		log.Debug("Successfully stopped listening to kafka consumer group", nil)
		listeningToConsumerGroupStopped <- true
		<-readyToCloseOutboundConnections
		log.Debug("Attempting to close http server", nil)
		if err := api.Close(childContext); err != nil {
			log.ErrorC("Failed to gracefully close http server", err, nil)
		} else {
			log.Debug("Successfully closed http server", nil)
		}
		log.Debug("Attempting to close kafka producer", nil)
		svc.Producer.Close(childContext)
		log.Debug("Successfully closed kafka producer", nil)
		log.Debug("Attempting to close kafka consumer group", nil)
		svc.Consumer.Close(childContext)
		log.Debug("Successfully closed kafka consumer group", nil)
		atomic.AddInt32(&waitGroup, -1)

		log.Debug("Closed kafka successfully", nil)
	}()

	// setup a timer to zero waitGroup after timeout
	go func() {
		<-childContext.Done()
		log.Error(errors.New("timeout while shutting down"), nil)
		atomic.AddInt32(&waitGroup, -atomic.LoadInt32(&waitGroup))
	}()

	for atomic.LoadInt32(&waitGroup) > 0 {
	}

	log.Info("Shutdown complete", nil)

	childCancel()
	cancel()
	os.Exit(1)
}
