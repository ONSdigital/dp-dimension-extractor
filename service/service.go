package service

import (
	"fmt"
	"os"
	"os/signal"
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

	eventLoopDone := make(chan bool)
	apiErrors := make(chan error, 1)

	svc.HTTPClient = rchttp.DefaultClient

	eventLoopContext, eventLoopCancel := context.WithCancel(context.Background())

	api.CreateDimensionExtractorAPI(svc.DimensionExtractorURL, svc.BindAddr, apiErrors)

	// eventLoop
	go func() {
		defer close(eventLoopDone)
		for {
			select {
			case <-eventLoopContext.Done():
				log.Trace("Event loop context done", log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return
			case message := <-svc.Consumer.Incoming():

				instanceID, err := svc.handleMessage(eventLoopContext, message)
				if err != nil {
					log.ErrorC("event failed to process", err, log.Data{"instance_id": instanceID})
				} else {
					log.Debug("event successfully processed", log.Data{"instance_id": instanceID})
				}

				svc.Consumer.CommitAndRelease(message)
				log.Debug("message committed", log.Data{"instance_id": instanceID})
			}
		}
	}()

	// block until a fatal error, signal or eventLoopDone - then proceed to shutdown
	select {
	case <-eventLoopDone:
		log.Debug("Quitting after done was closed", nil)
	case signal := <-signals:
		log.Debug("Quitting after os signal received", log.Data{"signal": signal})
	case consumerError := <-svc.Consumer.Errors():
		log.Error(fmt.Errorf("aborting consumer"), log.Data{"message_received": consumerError})
	case producerError := <-svc.Producer.Errors():
		log.Error(fmt.Errorf("aborting producer"), log.Data{"message_received": producerError})
	case <-apiErrors:
		log.Error(fmt.Errorf("server error forcing shutdown"), nil)
	}

	// give the app `Timeout` seconds to close gracefully before killing it.
	ctx, cancel := context.WithTimeout(context.Background(), svc.Shutdown)

	go func() {
		log.Debug("Stopping kafka consumer listener", nil)
		svc.Consumer.StopListeningToConsumer(ctx)
		log.Debug("Stopped kafka consumer listener", nil)
		eventLoopCancel()
		<-eventLoopDone
		log.Debug("Closing http server", nil)
		if err := api.Close(ctx); err != nil {
			log.ErrorC("Failed to gracefully close http server", err, nil)
		} else {
			log.Debug("Gracefully closed http server", nil)
		}
		log.Debug("Closing kafka producer", nil)
		svc.Producer.Close(ctx)
		log.Debug("Closed kafka producer", nil)
		log.Debug("Closing kafka consumer", nil)
		svc.Consumer.Close(ctx)
		log.Debug("Closed kafka consumer", nil)

		log.Info("Done shutdown - cancelling timeout context", nil)

		cancel() // stop timer
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		log.Error(ctx.Err(), nil)
	} else {
		log.Info("Done shutdown gracefully", log.Data{"context": ctx.Err()})
	}
	os.Exit(1)
}
