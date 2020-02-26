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
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"
	rchttp "github.com/ONSdigital/dp-rchttp"
	s3client "github.com/ONSdigital/dp-s3"
	vault "github.com/ONSdigital/dp-vault"
	"github.com/ONSdigital/log.go/log"
	"golang.org/x/net/context"
)

const authorizationHeader = "Authorization"

var (
	// BuildTime represents the time in which the service was built
	BuildTime string
	// GitCommit represents the commit (SHA-1) hash of the service that is running
	GitCommit string
	// Version represents the version of the service that is running
	Version string
)

func main() {
	log.Namespace = "dp-dimension-extractor"
	ctx := context.Background()
	log.Event(ctx, "Starting dimension extractor", log.INFO)

	// Signals channel to notify only of SIGING and SIGTERM
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Attempt to get config. Exit on failure.
	cfg, err := config.Get()
	exitIfError(ctx, "", err, nil)

	// Sensitive fields are omitted from config.String().
	log.Event(ctx, "config on startup", log.INFO, log.Data{"config": cfg})

	// Attempt to parse envMax from config. Exit on failure.
	envMax, err := strconv.ParseInt(cfg.KafkaMaxBytes, 10, 32)
	exitIfError(ctx, "encountered error parsing kafka max bytes", err, nil)

	// External services and their initialization state
	var serviceList initialise.ExternalServiceList

	// Get syncConsumerGroup Kafka Consumer
	syncConsumerGroup, err := serviceList.GetConsumer(ctx, cfg.Brokers, cfg)
	exitIfError(ctx, "could not obtain consumer", err, nil)

	// Get AWS Session to access S3
	awsSession, s3Clients, err := serviceList.GetS3Clients(cfg)
	logIfError(ctx, "", err, nil)

	// Get dimensionExtracted Kafka Producer
	dimensionExtractedProducer, err := serviceList.GetProducer(
		ctx,
		cfg.Brokers,
		cfg.DimensionsExtractedTopic,
		initialise.DimensionExtracted,
		int(envMax),
	)
	exitIfError(ctx, "", err, nil)

	// Get dimensionExtracted Error Kafka Producer
	dimensionExtractedErrProducer, err := serviceList.GetProducer(
		ctx,
		cfg.Brokers,
		cfg.EventReporterTopic,
		initialise.DimensionExtractedErr,
		int(envMax),
	)
	exitIfError(ctx, "", err, nil)

	// Get HealthCheck
	hc, err := serviceList.GetHealthCheck(cfg, BuildTime, GitCommit, Version)
	exitIfError(ctx, "", err, nil)

	// If encryption is enabled, get Vault Client
	var vc *vault.Client
	if !cfg.EncryptionDisabled {
		vc, err = serviceList.GetVault(cfg, 3)
		logIfError(ctx, "", err, nil)
	}

	if err := registerCheckers(&hc, !cfg.EncryptionDisabled, syncConsumerGroup, dimensionExtractedProducer, dimensionExtractedErrProducer, s3Clients, vc); err != nil {
		os.Exit(1)
	}

	// create Channels
	eventLoopDone := make(chan bool)
	apiErrors := make(chan error, 1)

	// Create API with healthchecker
	api.CreateDimensionExtractorAPI(ctx, cfg.DimensionExtractorURL, cfg.BindAddr, apiErrors, &hc)

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
		AwsSession:                 awsSession,
		S3Clients:                  s3Clients,
		VaultClient:                vc,
		VaultPath:                  cfg.VaultPath,
	}

	// Get Error reporter
	errorReporter, err := serviceList.GetImportErrorReporter(dimensionExtractedErrProducer, log.Namespace)
	logIfError(ctx, "error while attempting to create error reporter client", err, nil)

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
			log.Event(eventLoopContext, "could not obtain valid service account", log.ERROR, log.Error(err))
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
			consumerErrors = syncConsumerGroup.Channels().Errors
		} else {
			consumerErrors = make(chan error, 1)
		}

		if serviceList.DimensionExtractedProducer {
			producerErrors = service.DimensionExtractedProducer.Channels().Errors
		} else {
			producerErrors = make(chan error, 1)
		}

		select {
		case consumerError := <-consumerErrors:
			log.Event(ctx, "kafka consumer", log.ERROR, log.Error(consumerError))
		case producerError := <-producerErrors:
			log.Event(ctx, "kafka producer", log.ERROR, log.Error(producerError))
		case apiError := <-apiErrors:
			log.Event(ctx, "server error", log.ERROR, log.Error(apiError))
		case <-eventLoopDone:
			log.Event(ctx, "event loop done", log.ERROR)
		}
	}()

	// Block until a fatal error occurs
	select {
	case signal := <-signals:
		log.Event(ctx, "quitting after os signal received", log.INFO, log.Data{"signal": signal})
	}

	// give the app `Timeout` seconds to close gracefully before killing it.
	shutdownContext, cancel := context.WithTimeout(ctx, cfg.GracefulShutdownTimeout)

	go func() {

		// If kafka consumer exists, stop listening to it. (Will close later)
		if serviceList.Consumer {
			log.Event(shutdownContext, "stopping kafka consumer listener", log.INFO)
			syncConsumerGroup.StopListeningToConsumer(shutdownContext)
			log.Event(shutdownContext, "stopped kafka consumer listener", log.INFO)
		}

		eventLoopCancel()
		<-eventLoopDone

		// Close API
		log.Event(shutdownContext, "closing http server", log.INFO)
		if err := api.Close(shutdownContext, &hc); err != nil {
			log.Event(shutdownContext, "failed to gracefully close http server", log.ERROR, log.Error(err))
		} else {
			log.Event(shutdownContext, "gracefully closed http server", log.INFO)
		}

		// If DimensionExtracted kafka producer exists, close it
		if serviceList.DimensionExtractedProducer {
			log.Event(shutdownContext, "closing kafka producer", log.INFO, log.Data{"producer": "DimensionExtracted"})
			service.DimensionExtractedProducer.Close(shutdownContext)
			log.Event(shutdownContext, "closed kafka producer", log.INFO, log.Data{"producer": "DimensionExtracted"})
		}

		// If DimensionExtractedError kafka producer exists, close it.
		if serviceList.DimensionExtractedErrProducer {
			log.Event(shutdownContext, "closing kafka producer", log.INFO, log.Data{"producer": "DimensionExtractedErr"})
			dimensionExtractedErrProducer.Close(shutdownContext)
			log.Event(shutdownContext, "closed kafka producer", log.INFO, log.Data{"producer": "DimensionExtractedErr"})
		}

		// If kafka consumer exists, close it.
		if serviceList.Consumer {
			log.Event(shutdownContext, "closing kafka consumer", log.INFO, log.Data{"consumer": "SyncConsumerGroup"})
			syncConsumerGroup.Close(shutdownContext)
			log.Event(shutdownContext, "closed kafka consumer", log.INFO, log.Data{"consumer": "SyncConsumerGroup"})
		}

		log.Event(shutdownContext, "done shutdown - cancelling timeout context", log.INFO)
		cancel() // stop timer
	}()

	// wait for timeout or success (via cancel)
	<-shutdownContext.Done()
	if shutdownContext.Err() == context.DeadlineExceeded {
		log.Event(shutdownContext, "shutdown timeout", log.ERROR, log.Error(shutdownContext.Err()))
	} else {
		log.Event(shutdownContext, "done shutdown gracefully", log.ERROR, log.Data{"context": shutdownContext.Err()})
	}
	os.Exit(1)
}

func checkServiceIdentity(ctx context.Context, zebedeeURL, serviceAuthToken string) error {
	// TODO switch out below to use Identity client
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

	log.Event(ctx, "dimension extractor has a valid service account", log.INFO)
	return nil
}

// registerCheckers adds the checkers for the provided clients to the healthcheck object.
// VaultClient health client will only be registered if encryption is enabled.
func registerCheckers(hc *healthcheck.HealthCheck, isEncryptionEnabled bool,
	kafkaConsumer *kafka.ConsumerGroup,
	dimensionExtractedProducer *kafka.Producer,
	dimensionExtractedErrProducer *kafka.Producer,
	s3Clients map[string]*s3client.S3,
	vc *vault.Client) (err error) {

	if err = hc.AddCheck("Kafka Consumer", kafkaConsumer.Checker); err != nil {
		log.Event(nil, "Error Adding Check for Kafka Consumer", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka Producer", dimensionExtractedProducer.Checker); err != nil {
		log.Event(nil, "Error Adding Check for Kafka Producer", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka Error Producer", dimensionExtractedErrProducer.Checker); err != nil {
		log.Event(nil, "Error Adding Check for Kafka Error Producer", log.ERROR, log.Error(err))
	}

	if isEncryptionEnabled {
		if err = hc.AddCheck("Vault", vc.Checker); err != nil {
			log.Event(nil, "Error Adding Check for Vault", log.ERROR, log.Error(err))
		}
	}

	for bucketName, s3 := range s3Clients {
		if err = hc.AddCheck(fmt.Sprintf("S3 bucket %s", bucketName), s3.Checker); err != nil {
			log.Event(nil, "Error Adding Check for S3 Client", log.ERROR, log.Error(err))
		}
	}

	return
}

// if error is not nil, log it and exit
func exitIfError(ctx context.Context, msg string, err error, data log.Data) {
	if err != nil {
		log.Event(ctx, fmt.Sprintf("fatal error %s", msg), log.ERROR, log.Error(err), data)
		os.Exit(1)
	}
}

// if error is not nil, log it only
func logIfError(ctx context.Context, msg string, err error, data log.Data) {
	if err != nil {
		log.Event(ctx, fmt.Sprintf("error %s", msg), log.ERROR, log.Error(err), data)
	}
}
