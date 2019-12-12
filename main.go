package main

import (
	"net/http"
	"net/url"
	"os"
	"strconv"

	"fmt"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dimension-extractor/api"
	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/dp-dimension-extractor/event"
	"github.com/ONSdigital/dp-dimension-extractor/initialise"
	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
	"golang.org/x/net/context"
)

const authorizationHeader = "Authorization"

func main() {
	log.Namespace = "dp-dimension-extractor"
	log.Info("Starting dimension extractor", nil)

	// Signals channel to notify only of SIGING and SIGTERM
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Attempt to get config. Exit on failure.
	cfg, err := config.Get()
	exitIfError("", err, nil)

	// Sensitive fields are omitted from config.String().
	log.Info("config on startup", log.Data{"config": cfg})

	// Attempt to parse envMax from config. Exit on failure.
	envMax, err := strconv.ParseInt(cfg.KafkaMaxBytes, 10, 32)
	exitIfError("encountered error parsing kafka max bytes", err, nil)

	// External services and their initialization state
	var serviceList initialise.ExternalServiceList

	// Get syncConsumerGroup Kafka Consumer
	syncConsumerGroup, err := serviceList.GetConsumer(cfg.Brokers, cfg)
	logIfError("could not obtain consumer", err, nil)

	// Get AWS Session to access S3
	s3, err := serviceList.GetAwsSession(cfg)
	logIfError("", err, nil)

	// Get dimensionExtracted Kafka Producer
	dimensionExtractedProducer, err := serviceList.GetProducer(
		cfg.Brokers,
		cfg.DimensionsExtractedTopic,
		initialise.DimensionExtracted,
		int(envMax),
	)
	logIfError("", err, nil)

	// Get dimensionExtracted Error Kafka Producer
	dimensionExtractedErrProducer, err := serviceList.GetProducer(
		cfg.Brokers,
		cfg.EventReporterTopic,
		initialise.DimensionExtractedErr,
		int(envMax),
	)
	logIfError("", err, nil)

	// create Channels
	eventLoopDone := make(chan bool)
	apiErrors := make(chan error, 1)

	// Create API
	api.CreateDimensionExtractorAPI(cfg.DimensionExtractorURL, cfg.BindAddr, apiErrors)

	// If encryption is enabled, get Vault Client
	var vc service.VaultClient
	if !cfg.EncryptionDisabled {
		vc, err = serviceList.GetVault(cfg, 3)
		logIfError("", err, nil)
	}

	service := &service.Service{
		AuthToken:                  cfg.ServiceAuthToken,
		DatasetAPIURL:              cfg.DatasetAPIURL,
		DatasetAPIAuthToken:        cfg.DatasetAPIAuthToken,
		DimensionExtractedProducer: dimensionExtractedProducer,
		DimensionExtractorURL:      cfg.DimensionExtractorURL,
		EncryptionDisabled:         cfg.EncryptionDisabled,
		EnvMax:                     envMax,
		HTTPClient:                 rchttp.DefaultClient,
		MaxRetries:                 cfg.MaxRetries,
		S3:                         s3,
		VaultClient:                vc,
		VaultPath:                  cfg.VaultPath,
	}

	// Get Error reporter
	errorReporter, err := serviceList.GetImportErrorReporter(dimensionExtractedErrProducer, log.Namespace)
	logIfError("error while attempting to create error reporter client", err, nil)

	// Initialize event Consumer struct with initialized kafka consumers/producers and services
	eventConsumer := event.Consumer{
		KafkaConsumer: syncConsumerGroup,
		EventService:  service,
		ErrorReporter: errorReporter,
	}

	eventLoopContext, eventLoopCancel := context.WithCancel(context.Background())

	// Validate this service against Zebedee
	serviceIdentityValidated := make(chan bool)
	go func() {
		if err = checkServiceIdentity(eventLoopContext, cfg.ZebedeeURL, cfg.ServiceAuthToken); err != nil {
			log.ErrorC("could not obtain valid service account", err, nil)
		} else {
			serviceIdentityValidated <- true
		}
	}()

	// Start Event Consumer (only if it is available)
	if serviceList.Consumer {
		eventConsumer.Start(eventLoopContext, eventLoopDone, serviceIdentityValidated)
	}

	// Log non-fatal errors, without exiting
	go func() {
		var consumerErrors, producerErrors chan (error)

		if serviceList.Consumer {
			consumerErrors = syncConsumerGroup.Errors()
		} else {
			consumerErrors = make(chan error, 1)
		}

		if serviceList.DimensionExtractedProducer {
			producerErrors = service.DimensionExtractedProducer.Errors()
		} else {
			producerErrors = make(chan error, 1)
		}

		select {
		case consumerError := <-consumerErrors:
			log.ErrorC("kafka consumer", consumerError, nil)
		case producerError := <-producerErrors:
			log.ErrorC("kafka producer", producerError, nil)
		case apiError := <-apiErrors:
			log.ErrorC("server error", apiError, nil)
		case <-eventLoopDone:
			log.ErrorC("event loop done", nil, nil)
		}
	}()

	// Block until a fatal error occurs
	select {
	case signal := <-signals:
		log.Debug("quitting after os signal received", log.Data{"signal": signal})
	}

	// give the app `Timeout` seconds to close gracefully before killing it.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

	go func() {

		// If kafka consumer exists, stop listening to it. (Will close later)
		if serviceList.Consumer {
			log.Debug("stopping kafka consumer listener", nil)
			syncConsumerGroup.StopListeningToConsumer(ctx)
			log.Debug("stopped kafka consumer listener", nil)
		}

		eventLoopCancel()
		<-eventLoopDone

		// Close API
		log.Debug("closing http server", nil)
		if err := api.Close(ctx); err != nil {
			log.ErrorC("failed to gracefully close http server", err, nil)
		} else {
			log.Debug("gracefully closed http server", nil)
		}

		// If DimensionExtracted kafka producer exists, close it
		if serviceList.DimensionExtractedProducer {
			log.Debug("closing kafka producer", log.Data{"producer": "DimensionExtracted"})
			service.DimensionExtractedProducer.Close(ctx)
			log.Debug("closed kafka producer", log.Data{"producer": "DimensionExtracted"})
		}

		// If DimentionExtractedError kafka producer exists, close it.
		if serviceList.DimensionExtractedErrProducer {
			log.Debug("closing kafka producer", log.Data{"producer": "DimensionExtractedErr"})
			dimensionExtractedErrProducer.Close(ctx)
			log.Debug("closed kafka producer", log.Data{"producer": "DimensionExtractedErr"})
		}

		// If kafka consumer exists, close it.
		if serviceList.Consumer {
			log.Debug("closing kafka consumer", log.Data{"consumer": "SyncConsumerGroup"})
			syncConsumerGroup.Close(ctx)
			log.Debug("closed kafka consumer", log.Data{"consumer": "SyncConsumerGroup"})
		}

		log.Info("done shutdown - cancelling timeout context", nil)
		cancel() // stop timer
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		log.Error(ctx.Err(), nil)
	} else {
		log.Info("done shutdown gracefully", log.Data{"context": ctx.Err()})
	}
	os.Exit(1)
}

func checkServiceIdentity(ctx context.Context, zebedeeURL, serviceAuthToken string) error {
	// TODO switch out below to use gedges go-ns package
	client := rchttp.DefaultClient

	path := fmt.Sprintf("%s/identity", zebedeeURL)

	var URL *url.URL
	URL, err := url.Parse(path)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("GET", URL.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set(authorizationHeader, serviceAuthToken)

	res, err := client.Do(ctx, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, zebedeeURL)
	}

	log.Info("dimension extractor has a valid service account", nil)
	return nil
}

// if error is not nil, log it and exit
func exitIfError(correlationKey string, err error, data log.Data) {
	if err != nil {
		log.ErrorC(correlationKey, err, data)
		os.Exit(1)
	}
}

// if error is not nil, log it only
func logIfError(correlationKey string, err error, data log.Data) {
	if err != nil {
		log.ErrorC(correlationKey, err, data)
	}
}
